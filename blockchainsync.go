package blockchain_transaction_syncs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Gavine-Gao/blockchain-sync-sol/config"
	"github.com/Gavine-Gao/blockchain-sync-sol/database"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient"
	"github.com/Gavine-Gao/blockchain-sync-sol/rpcclient/chain-account/account"
	"github.com/Gavine-Gao/blockchain-sync-sol/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type BlockChainSync struct {
	Synchronizer *worker.BaseSynchronizer
	Deposit      *worker.Deposit
	Withdraw     *worker.Withdraw
	Internal     *worker.Internal
	FallBack     *worker.FallBack

	shutdown context.CancelCauseFunc
	stopped  atomic.Bool
}

func NewBlockChainSync(ctx context.Context, cfg *config.Config, shutdown context.CancelCauseFunc) (*BlockChainSync, error) {
	db, err := database.NewDB(ctx, cfg.MasterDB)
	if err != nil {
		log.Error("init database fail", err)
		return nil, err
	}

	log.Info("New deposit", "ChainAccountRpc", cfg.ChainAccountRpc)
	conn, err := grpc.NewClient(cfg.ChainAccountRpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("Connect to da retriever fail", "err", err)
		return nil, err
	}
	client := account.NewWalletAccountServiceClient(conn)
	accountClient, err := rpcclient.NewWalletChainAccountClient(context.Background(), client, cfg.ChainNode.ChainName)
	if err != nil {
		log.Error("new wallet account client fail", "err", err)
		return nil, err
	}

	deposit, err := worker.NewDeposit(cfg, db, accountClient, shutdown)
	if err != nil {
		log.Error("new deposit worker fail", "err", err)
		return nil, err
	}
	withdraw, err := worker.NewWithdraw(cfg, db, accountClient, shutdown)
	if err != nil {
		log.Error("new withdraw worker fail", "err", err)
		return nil, err
	}
	internal, err := worker.NewInternal(cfg, db, accountClient, shutdown)
	if err != nil {
		log.Error("new internal worker fail", "err", err)
		return nil, err
	}
	fallback, err := worker.NewFallBack(cfg, db, accountClient, deposit, shutdown)
	if err != nil {
		log.Error("new fallback worker fail", "err", err)
		return nil, err
	}

	out := &BlockChainSync{
		Deposit:  deposit,
		Withdraw: withdraw,
		Internal: internal,
		FallBack: fallback,
		shutdown: shutdown,
	}
	return out, nil
}

func (bcs *BlockChainSync) Start(ctx context.Context) error {
	// 启动 Prometheus metrics HTTP 端点
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		metricsAddr := ":9090"
		log.Info("starting metrics server", "addr", metricsAddr)
		if err := http.ListenAndServe(metricsAddr, nil); err != nil {
			log.Error("metrics server fail", "err", err)
		}
	}()

	err := bcs.Deposit.Start()
	if err != nil {
		return err
	}
	//err = bcs.Withdraw.Start()
	//if err != nil {
	//	return err
	//}
	//err = bcs.Internal.Start()
	//if err != nil {
	//	return err
	//}
	err = bcs.FallBack.Start()
	if err != nil {
		return err
	}
	return nil
}

func (bcs *BlockChainSync) Stop(ctx context.Context) error {
	// 设置 30 秒关闭超时
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	type closer struct {
		name string
		fn   func() error
	}
	closers := []closer{
		{"Deposit", bcs.Deposit.Close},
		{"FallBack", bcs.FallBack.Close},
		// 取消注释以启用:
		// {"Withdraw", bcs.Withdraw.Close},
		// {"Internal", bcs.Internal.Close},
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		result error
	)

	for _, c := range closers {
		wg.Add(1)
		go func(name string, fn func() error) {
			defer wg.Done()
			if err := fn(); err != nil {
				mu.Lock()
				result = errors.Join(result, fmt.Errorf("%s close fail: %w", name, err))
				mu.Unlock()
			}
			log.Info("worker stopped", "name", name)
		}(c.name, c.fn)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
		log.Info("all workers stopped gracefully")
	case <-ctx.Done():
		result = errors.Join(result, fmt.Errorf("shutdown timeout after 30s"))
	}

	bcs.stopped.Store(true)
	return result
}

func (bcs *BlockChainSync) Stopped() bool {
	return bcs.stopped.Load()
}
