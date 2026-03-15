package worker

import (
	"context"
	"errors"
	"math/big"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/Gavine-Gao/blockchain-sync-sol/common/clock"
	"github.com/Gavine-Gao/blockchain-sync-sol/database"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient/chain-account/account"
)

const TxHandleTaskBatchSize uint64 = 500

type Config struct {
	LoopIntervalMsec uint
	HeaderBufferSize uint
	StartHeight      *big.Int
	Confirmations    uint64
}

type BaseSynchronizer struct {
	loopInterval     time.Duration
	headerBufferSize uint64
	fromBlock        uint64
	confirmations    uint64

	rpcClient *rpcclient.WalletChainAccountClient
	database  *database.DB

	worker          *clock.LoopFn
	isFallBack      atomic.Bool
	batchWg         *sync.WaitGroup
	retryWg         *sync.WaitGroup
	bank            *ChannelBank
	retryCh         chan uint64
	addrCache       *AddressCache // 布隆过滤器 + map 地址缓存
	onBatchComplete func(txs []TransactionChannel) error
}

func (syncer *BaseSynchronizer) Start() error {
	if syncer.worker != nil {
		return errors.New("already started")
	}
	syncer.worker = clock.NewLoopFn(clock.SystemClock, syncer.tick, func() error {
		log.Info("shutting down batch producer")
		return nil
	}, syncer.loopInterval)
	return nil
}

func (syncer *BaseSynchronizer) Close() error {
	if syncer.worker == nil {
		return nil
	}
	return syncer.worker.Close()
}

// tick 定时触发：获取最新高度 → 分批 → 多协程并发扫块 → 流式推入 ChannelBank → 消费 → 更新 fromBlock
func (syncer *BaseSynchronizer) tick(ctx context.Context) {
	if syncer.isFallBack.Load() {
		log.Info("in fallback mode, skip tick")
		return
	}

	// 计时：整批次耗时
	batchStart := time.Now()
	defer func() {
		SyncBatchDuration.Observe(time.Since(batchStart).Seconds())
	}()

	// 1. 获取链上最新高度
	chainHeader, err := syncer.rpcClient.GetBlockHeader(nil)
	if err != nil {
		log.Error("get latest block header fail", "err", err)
		return
	}
	latestHeight, err := strconv.ParseUint(chainHeader.BlockHeader.Number, 10, 64)
	if err != nil {
		log.Error("parse latest block number fail", "err", err)
		return
	}

	// 2. 计算安全高度（减去确认数）
	safeHeight := latestHeight
	if latestHeight > syncer.confirmations {
		safeHeight = latestHeight - syncer.confirmations
	}

	// 3. 无新块则跳过
	if syncer.fromBlock >= safeHeight {
		log.Info("no new blocks to sync", "fromBlock", syncer.fromBlock, "safeHeight", safeHeight)
		SyncBlockLatency.Set(0)
		return
	}

	// 记录同步延迟
	SyncBlockLatency.Set(float64(latestHeight - syncer.fromBlock))

	// 4. 限制单次扫描范围
	endBlock := syncer.fromBlock + syncer.headerBufferSize
	if endBlock > safeHeight {
		endBlock = safeHeight
	}

	log.Info("start scanning blocks", "from", syncer.fromBlock, "to", endBlock)

	// 5. 创建流式 ChannelBank 和重试 Channel
	syncer.bank = NewChannelBank(1024, syncer.fromBlock)
	syncer.batchWg = &sync.WaitGroup{}

	// 6. 线程安全的区块响应收集器（扫块时同时收集区块头，避免重复拉取）
	var blocksMu sync.Mutex
	var blockResponses []*account.BlockResponse

	syncer.startRetryWorker(&blocksMu, &blockResponses)

	// 7. 按 batchSize 分批，并发扫块
	batchSize := TxHandleTaskBatchSize
	for batchStart := syncer.fromBlock; batchStart < endBlock; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > endBlock {
			batchEnd = endBlock
		}

		batch := make([]uint64, 0, batchEnd-batchStart)
		for h := batchStart; h < batchEnd; h++ {
			batch = append(batch, h)
		}

		syncer.batchWg.Add(1)
		go func(batch []uint64) {
			defer syncer.batchWg.Done()

			for _, h := range batch {
				// 限制并发 RPC 调用
				block, err := syncer.rpcClient.GetBlockInfo(h)
				if err != nil {
					log.Error("get block info fail, sending to retry", "height", h, "err", err)
					SyncRPCCalls.WithLabelValues("GetBlockInfo", "error").Inc()
					syncer.retryCh <- h
					continue
				}
				SyncRPCCalls.WithLabelValues("GetBlockInfo", "ok").Inc()
				SyncBlocksScanned.Inc()
				syncer.processBlockTransactions(block)

				// 同时收集区块响应，供后续存区块头使用
				blocksMu.Lock()
				blockResponses = append(blockResponses, block)
				blocksMu.Unlock()
			}
		}(batch)
	}

	// 8. 等待所有扫块 goroutine 完成 → 关闭 retryCh → 等待重试完成 → 关闭 bank
	go func() {
		syncer.batchWg.Wait()
		close(syncer.retryCh)
		syncer.retryWg.Wait()
		log.Info("all batches and retries processed, closing channel bank")
		syncer.bank.Close()
	}()

	// 9. 流式消费 ChannelBank：按区块号分组，每完成一个区块立刻写 DB
	var blockBatch []TransactionChannel
	var currentBlock uint64
	var firstTx = true

	for tx := range syncer.bank.Channel() {
		blockNum := tx.BlockNumber.Uint64()

		if !firstTx && blockNum != currentBlock && len(blockBatch) > 0 {
			if syncer.onBatchComplete != nil {
				if err := syncer.onBatchComplete(blockBatch); err != nil {
					log.Error("onBatchComplete fail", "block", currentBlock, "err", err)
				}
			}
			blockBatch = nil
		}
		currentBlock = blockNum
		firstTx = false
		blockBatch = append(blockBatch, tx)
	}
	if syncer.onBatchComplete != nil && len(blockBatch) > 0 {
		if err := syncer.onBatchComplete(blockBatch); err != nil {
			log.Error("onBatchComplete fail", "block", currentBlock, "err", err)
		}
	}

	// 10. 用已有的 blockResponses 存区块头（不再重复拉取）
	blockHeaders := syncer.processHeader(blockResponses)
	if len(blockHeaders) > 0 {
		if storeErr := syncer.database.Blocks.StoreBlockss(blockHeaders); storeErr != nil {
			log.Error("store blocks fail", "err", storeErr)
		}
	}

	// 11. 更新 fromBlock
	syncer.fromBlock = endBlock
	log.Info("scan completed", "newFromBlock", syncer.fromBlock)
}

// startRetryWorker 初始化重试通道并启动 retry goroutine
// 失败的区块号从 retryCh 读取，指数退避重试 3 次，成功后推回 bank，仍失败标记 fallback
func (syncer *BaseSynchronizer) startRetryWorker(blocksMu *sync.Mutex, blockResponses *[]*account.BlockResponse) {
	syncer.retryWg = &sync.WaitGroup{}
	syncer.retryCh = make(chan uint64, 256)

	syncer.retryWg.Add(1)
	go func() {
		defer syncer.retryWg.Done()
		for blockNum := range syncer.retryCh {
			var success bool
			for attempt := 1; attempt <= 3; attempt++ {
				log.Info("retrying block", "height", blockNum, "attempt", attempt)
				SyncRetriedBlocks.Inc()
				time.Sleep(time.Duration(attempt) * time.Second)

				block, err := syncer.rpcClient.GetBlockInfo(blockNum)
				if err != nil {
					log.Error("retry get block fail", "height", blockNum, "attempt", attempt, "err", err)
					continue
				}
				syncer.processBlockTransactions(block)

				// 重试成功也收集区块头
				blocksMu.Lock()
				*blockResponses = append(*blockResponses, block)
				blocksMu.Unlock()

				success = true
				break
			}
			if !success {
				log.Error("block permanently failed after retries, marking fallback", "height", blockNum)
				syncer.bank.Push(TransactionChannel{
					BlockNumber: big.NewInt(int64(blockNum)),
					TxType:      database.TxTypeUnKnow,
				})
				syncer.isFallBack.Store(true)
			}
		}
	}()
}

// processBlockTransactions 使用布隆过滤器快速过滤地址，不查 DB
func (syncer *BaseSynchronizer) processBlockTransactions(block *account.BlockResponse) {
	if block == nil {
		return
	}

	blockNum := big.NewInt(block.Height)
	pushed := false // 标记是否 Push 过数据

	for _, tx := range block.Transactions {
		// 布隆过滤器快速判断：一定不存在则跳过
		toMayExist := syncer.addrCache.MayExist(tx.To)
		fromMayExist := syncer.addrCache.MayExist(tx.From)

		if !toMayExist && !fromMayExist {
			continue
		}

		// 布隆过滤器说"可能存在"，用 map 确认并拿地址类型
		toInfos, existTo := syncer.addrCache.Lookup(tx.To)
		fromInfos, existFrom := syncer.addrCache.Lookup(tx.From)

		if !existTo && !existFrom {
			continue // 布隆过滤器误报
		}

		// 遍历所有匹配的 business
		processed := make(map[string]bool)
		var allInfos []AddressInfo
		allInfos = append(allInfos, toInfos...)
		allInfos = append(allInfos, fromInfos...)

		for _, info := range allInfos {
			if processed[info.BusinessId] {
				continue
			}
			processed[info.BusinessId] = true

			var toType database.AddressType
			var fromType database.AddressType
			var toMatch, fromMatch bool

			for _, ti := range toInfos {
				if ti.BusinessId == info.BusinessId {
					toType = ti.AddressType
					toMatch = true
					break
				}
			}
			for _, fi := range fromInfos {
				if fi.BusinessId == info.BusinessId {
					fromType = fi.AddressType
					fromMatch = true
					break
				}
			}

			if !toMatch && !fromMatch {
				continue
			}

			txItem := TransactionChannel{
				BusinessId:   info.BusinessId,
				BlockNumber:  blockNum,
				BlockHash:    block.Hash,
				TxHash:       tx.Hash,
				FromAddress:  tx.From,
				ToAddress:    tx.To,
				Amount:       tx.Amount,
				TxFee:        block.BaseFee,
				TxStatus:     1,
				TokenAddress: tx.TokenAddress,
				TxType:       database.TxTypeUnKnow,
			}

			if !fromMatch && (toMatch && toType == database.AddressTypeUser) {
				txItem.TxType = database.TxTypeDeposit
			}
			if (fromMatch && fromType == database.AddressTypeHot) && !toMatch {
				txItem.TxType = database.TxTypeWithdraw
			}
			if (fromMatch && fromType == database.AddressTypeUser) && (toMatch && toType == database.AddressTypeHot) {
				txItem.TxType = database.TxTypeCollection
			}
			if (fromMatch && fromType == database.AddressTypeHot) && (toMatch && toType == database.AddressTypeCold) {
				txItem.TxType = database.TxTypeHot2Cold
			}
			if (fromMatch && fromType == database.AddressTypeCold) && (toMatch && toType == database.AddressTypeHot) {
				txItem.TxType = database.TxTypeCold2Hot
			}

			syncer.bank.Push(txItem)
			pushed = true
		}
	}

	// 如果这个区块没有任何匹配的交易，推一个空标记让 nextExpected 推进
	if !pushed {
		syncer.bank.Push(TransactionChannel{
			BlockNumber: blockNum,
			TxType:      database.TxTypeUnKnow,
		})
	}
}

func (syncer *BaseSynchronizer) processHeader(blocks []*account.BlockResponse) []database.Blocks {
	if len(blocks) == 0 {
		return nil
	}
	var blockList []database.Blocks
	for _, block := range blocks {
		blockList = append(blockList, database.Blocks{
			Hash:   block.Hash,
			Number: big.NewInt(block.Height),
		})
	}
	return blockList
}
