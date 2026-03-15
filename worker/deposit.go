package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/uuid"

	"github.com/Gavine-Gao/blockchain-sync-sol/common/retry"
	"github.com/Gavine-Gao/blockchain-sync-sol/common/tasks"
	"github.com/Gavine-Gao/blockchain-sync-sol/config"
	"github.com/Gavine-Gao/blockchain-sync-sol/database"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient"
)

type Deposit struct {
	*BaseSynchronizer
	confirms       uint8
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
}

func NewDeposit(cfg *config.Config, db *database.DB, rpcClient *rpcclient.WalletChainAccountClient, shutdown context.CancelCauseFunc) (*Deposit, error) {
	var fromBlock uint64
	dbLatestBlockHeader, err := db.Blocks.LatestBlocks()
	if err != nil {
		log.Error("get latest block from database fail")
		return nil, err
	}
	if dbLatestBlockHeader != nil {
		log.Info("sync block", "number", dbLatestBlockHeader.Number, "hash", dbLatestBlockHeader.Hash)
		fromBlock = dbLatestBlockHeader.Number.Uint64()
	} else if cfg.ChainNode.StartingHeight > 0 {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(big.NewInt(int64(cfg.ChainNode.StartingHeight)))
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromBlock, _ = strconv.ParseUint(chainLatestBlockHeader.BlockHeader.Number, 10, 64)
	} else {
		chainLatestBlockHeader, err := rpcClient.GetBlockHeader(nil)
		if err != nil {
			log.Error("get block from chain account fail", "err", err)
			return nil, err
		}
		fromBlock, _ = strconv.ParseUint(chainLatestBlockHeader.BlockHeader.Number, 10, 64)
	}

	// 初始化布隆过滤器地址缓存
	addrCache, err := NewAddressCache(db)
	if err != nil {
		log.Error("init address cache fail", "err", err)
		return nil, err
	}

	baseSyncer := &BaseSynchronizer{
		fromBlock:        fromBlock,
		loopInterval:     cfg.ChainNode.SynchronizerInterval,
		headerBufferSize: cfg.ChainNode.BlocksStep,
		confirmations:    uint64(cfg.ChainNode.Confirmations),
		rpcClient:        rpcClient,
		database:         db,
		addrCache:        addrCache,
	}

	resCtx, resCancel := context.WithCancel(context.Background())

	return &Deposit{
		BaseSynchronizer: baseSyncer,
		confirms:         uint8(cfg.ChainNode.Confirmations),
		resourceCtx:      resCtx,
		resourceCancel:   resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in deposit: %w", err))
		}},
	}, nil
}

func (deposit *Deposit) Close() error {
	var result error
	if err := deposit.BaseSynchronizer.Close(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to close internal base synchronizer: %w", err))
	}
	deposit.resourceCancel()
	if err := deposit.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await batch handler completion: %w", err))
	}
	return result
}

func (deposit *Deposit) Start() error {
	log.Info("starting deposit...")

	// 启动地址缓存定期刷新（每5分钟）
	deposit.addrCache.StartRefresher(deposit.resourceCtx, 5*time.Minute)

	// 设置批量处理回调：tick() 收集完所有交易后一次性调用
	deposit.onBatchComplete = func(txs []TransactionChannel) error {
		return deposit.processBatch(txs)
	}

	// 启动 BaseSynchronizer 的 tick 循环
	if err := deposit.BaseSynchronizer.Start(); err != nil {
		return fmt.Errorf("failed to start internal Synchronizer: %w", err)
	}
	return nil
}

// processBatch 按 businessId 分组，每组一个事务批量写入 DB
func (deposit *Deposit) processBatch(txs []TransactionChannel) error {
	// 按 businessId 分组，过滤空标记
	grouped := make(map[string][]TransactionChannel)
	for _, tx := range txs {
		if tx.TxType == database.TxTypeUnKnow && tx.TxHash == "" {
			continue // 跳过空标记
		}
		grouped[tx.BusinessId] = append(grouped[tx.BusinessId], tx)
	}

	for businessId, txList := range grouped {
		if err := deposit.processBusinessBatch(businessId, txList); err != nil {
			log.Error("process business batch fail", "businessId", businessId, "err", err)
			// 不中断，继续处理其他 business
		}
	}
	return nil
}

// processBusinessBatch 处理单个 business 的全部交易，一个事务写入
func (deposit *Deposit) processBusinessBatch(businessId string, txList []TransactionChannel) error {
	var deposits []*database.Deposits
	var transactions []*database.Transactions
	var withdrawTxHashes []string
	var internalTxHashes []string

	for i := range txList {
		txItem := &txList[i]

		// 跳过空标记（失败区块的占位符）
		if txItem.TxType == database.TxTypeUnKnow && txItem.TxHash == "" {
			continue
		}

		// 构建通用交易记录
		txRecord, err := deposit.BuildTransaction(txItem)
		if err != nil {
			log.Error("build transaction fail", "txHash", txItem.TxHash, "err", err)
			continue
		}
		transactions = append(transactions, txRecord)

		// 按类型分类
		switch txItem.TxType {
		case database.TxTypeDeposit:
			depositRecord, err := deposit.BuildDeposit(txItem)
			if err != nil {
				log.Error("build deposit fail", "txHash", txItem.TxHash, "err", err)
				continue
			}
			deposits = append(deposits, depositRecord)

		case database.TxTypeWithdraw:
			withdrawTxHashes = append(withdrawTxHashes, txItem.TxHash)

		case database.TxTypeCollection, database.TxTypeHot2Cold, database.TxTypeCold2Hot:
			internalTxHashes = append(internalTxHashes, txItem.TxHash)
		}
	}

	// 一个事务批量写入 DB
	retryStrategy := &retry.ExponentialStrategy{Min: 1000, Max: 20_000, MaxJitter: 250}
	if _, err := retry.Do[interface{}](deposit.resourceCtx, 10, retryStrategy, func() (interface{}, error) {
		if err := deposit.database.Transaction(func(tx *database.DB) error {
			// 批量存入充值记录
			if len(deposits) > 0 {
				if err := tx.Deposits.StoreDeposits(businessId, deposits); err != nil {
					return fmt.Errorf("store deposits fail: %w", err)
				}
			}

			// 批量存入交易记录
			if len(transactions) > 0 {
				if err := tx.Transactions.StoreTransactions(businessId, transactions, uint64(len(transactions))); err != nil {
					return fmt.Errorf("store transactions fail: %w", err)
				}
			}

			// 批量更新提现状态
			for _, txHash := range withdrawTxHashes {
				if err := tx.Withdraws.UpdateWithdrawByTxHash(businessId, txHash, "", database.TxStatusWalletDone); err != nil {
					log.Error("update withdraw fail", "txHash", txHash, "err", err)
				}
			}

			// 批量更新内部交易状态
			for _, txHash := range internalTxHashes {
				if err := tx.Internals.UpdateInternalByTxHash(businessId, txHash, "", database.TxStatusWalletDone); err != nil {
					log.Error("update internal fail", "txHash", txHash, "err", err)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("batch write fail for business %s: %w", businessId, err)
	}

	log.Info("batch write success",
		"businessId", businessId,
		"deposits", len(deposits),
		"withdraws", len(withdrawTxHashes),
		"internals", len(internalTxHashes),
		"transactions", len(transactions),
	)

	// 记录 Prometheus 指标
	SyncTxProcessed.WithLabelValues("deposit").Add(float64(len(deposits)))
	SyncTxProcessed.WithLabelValues("withdraw").Add(float64(len(withdrawTxHashes)))
	SyncTxProcessed.WithLabelValues("internal").Add(float64(len(internalTxHashes)))
	return nil
}

// BuildDeposit 构建充值记录
func (deposit *Deposit) BuildDeposit(txItem *TransactionChannel) (*database.Deposits, error) {
	amountBig := new(big.Int)
	amountBig.SetString(txItem.Amount, 10)

	depositRecord := &database.Deposits{
		GUID:         uuid.New(),
		Timestamp:    uint64(time.Now().Unix()),
		Status:       database.TxStatusBroadcasted,
		BlockHash:    txItem.BlockHash,
		BlockNumber:  txItem.BlockNumber,
		TxHash:       txItem.TxHash,
		TxType:       txItem.TxType,
		FromAddress:  txItem.FromAddress,
		ToAddress:    txItem.ToAddress,
		Amount:       amountBig,
		TokenAddress: txItem.TokenAddress,
		TokenType:    database.TokenTypeNative,
	}
	if txItem.TokenAddress != "" {
		depositRecord.TokenType = database.TokenTypeToken
	}
	return depositRecord, nil
}

// BuildTransaction 构建通用交易记录
func (deposit *Deposit) BuildTransaction(txItem *TransactionChannel) (*database.Transactions, error) {
	amountBig := new(big.Int)
	amountBig.SetString(txItem.Amount, 10)

	feeBig := new(big.Int)
	feeBig.SetString(txItem.TxFee, 10)

	transaction := &database.Transactions{
		GUID:         uuid.New(),
		BlockHash:    txItem.BlockHash,
		BlockNumber:  txItem.BlockNumber,
		Hash:         txItem.TxHash,
		FromAddress:  txItem.FromAddress,
		ToAddress:    txItem.ToAddress,
		TokenAddress: txItem.TokenAddress,
		Fee:          feeBig,
		Amount:       amountBig,
		TxType:       txItem.TxType,
		Timestamp:    uint64(time.Now().Unix()),
	}
	return transaction, nil
}
