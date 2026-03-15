package worker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/log"

	"github.com/Gavine-Gao/blockchain-sync-sol/common/tasks"
	"github.com/Gavine-Gao/blockchain-sync-sol/config"
	"github.com/Gavine-Gao/blockchain-sync-sol/database"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient"
)

type FallBack struct {
	deposit        *Deposit
	db             *database.DB
	rpcClient      *rpcclient.WalletChainAccountClient
	resourceCtx    context.Context
	resourceCancel context.CancelFunc
	tasks          tasks.Group
	ticker         *time.Ticker
	confirmations  uint64
}

func NewFallBack(cfg *config.Config, db *database.DB, rpcClient *rpcclient.WalletChainAccountClient, deposit *Deposit, shutdown context.CancelCauseFunc) (*FallBack, error) {
	resCtx, resCancel := context.WithCancel(context.Background())

	return &FallBack{
		deposit:        deposit,
		db:             db,
		rpcClient:      rpcClient,
		confirmations:  uint64(cfg.ChainNode.Confirmations),
		resourceCtx:    resCtx,
		resourceCancel: resCancel,
		tasks: tasks.Group{HandleCrit: func(err error) {
			shutdown(fmt.Errorf("critical error in fallback: %w", err))
		}},
		ticker: time.NewTicker(time.Second * 30),
	}, nil
}

func (fb *FallBack) Start() error {
	log.Info("starting fallback...")
	fb.tasks.Go(func() error {
		for {
			select {
			case <-fb.ticker.C:
				fb.processFallBack()
			case <-fb.resourceCtx.Done():
				log.Info("stop fallback in worker")
				return nil
			}
		}
	})
	return nil
}

func (fb *FallBack) Close() error {
	var result error
	fb.resourceCancel()
	fb.ticker.Stop()
	if err := fb.tasks.Wait(); err != nil {
		result = errors.Join(result, fmt.Errorf("failed to await fallback completion: %w", err))
	}
	return result
}

func (fb *FallBack) processFallBack() {
	// 获取链上最新高度
	chainHeader, err := fb.rpcClient.GetBlockHeader(nil)
	if err != nil {
		log.Error("fallback: get latest block header fail", "err", err)
		return
	}

	log.Info("fallback: chain latest", "number", chainHeader.BlockHeader.Number)

	// 获取 DB 中最新区块
	dbLatestBlock, err := fb.db.Blocks.LatestBlocks()
	if err != nil || dbLatestBlock == nil {
		log.Warn("fallback: no blocks in db yet")
		return
	}

	// 计算回退范围
	endBlock := dbLatestBlock.Number
	startBlock := new(big.Int).Sub(endBlock, big.NewInt(int64(fb.confirmations*2)))
	if startBlock.Sign() < 0 {
		startBlock = big.NewInt(0)
	}

	log.Info("fallback: processing", "start", startBlock, "end", endBlock)

	businessList, err := fb.db.Business.QueryBusinessList()
	if err != nil {
		log.Error("fallback: query business list fail", "err", err)
		return
	}

	for _, business := range businessList {
		requestId := business.BusinessUid

		if err := fb.db.Deposits.HandleFallBackDeposits(requestId, startBlock, endBlock); err != nil {
			log.Error("fallback: handle deposits fail", "requestId", requestId, "err", err)
		}
		if err := fb.db.Withdraws.HandleFallBackWithdraw(requestId, startBlock, endBlock); err != nil {
			log.Error("fallback: handle withdraws fail", "requestId", requestId, "err", err)
		}
		if err := fb.db.Internals.HandleFallBackInternals(requestId, startBlock, endBlock); err != nil {
			log.Error("fallback: handle internals fail", "requestId", requestId, "err", err)
		}
		if err := fb.db.Transactions.HandleFallBackTransactions(requestId, startBlock, endBlock); err != nil {
			log.Error("fallback: handle transactions fail", "requestId", requestId, "err", err)
		}
	}
}
