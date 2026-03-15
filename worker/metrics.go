package worker

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// SyncBlockLatency 同步延迟：链上最新高度 - 当前已同步高度
	SyncBlockLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "blockchain_sync",
		Name:      "block_latency",
		Help:      "链上高度与已同步高度的差值",
	})

	// SyncBatchDuration 每批次扫块总耗时
	SyncBatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "blockchain_sync",
		Name:      "batch_duration_seconds",
		Help:      "每批次扫块处理耗时（秒）",
		Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60},
	})

	// SyncRPCCalls RPC 调用统计
	SyncRPCCalls = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockchain_sync",
		Name:      "rpc_calls_total",
		Help:      "RPC 调用计数",
	}, []string{"method", "status"})

	// SyncTxProcessed 已处理交易统计
	SyncTxProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockchain_sync",
		Name:      "tx_processed_total",
		Help:      "已处理交易笔数",
	}, []string{"type"}) // deposit, withdraw, collection, hot2cold, cold2hot

	// SyncBlocksScanned 已扫描区块数
	SyncBlocksScanned = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "blockchain_sync",
		Name:      "blocks_scanned_total",
		Help:      "已扫描区块总数",
	})

	// SyncRetriedBlocks 重试区块数
	SyncRetriedBlocks = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "blockchain_sync",
		Name:      "retried_blocks_total",
		Help:      "重试获取的区块次数",
	})

	// AddressCacheOps 地址缓存操作统计
	AddressCacheOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "blockchain_sync",
		Name:      "address_cache_ops_total",
		Help:      "地址缓存操作统计",
	}, []string{"result"}) // hit, miss, false_positive
)
