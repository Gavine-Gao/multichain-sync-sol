package worker

import (
	"container/heap"
	"math/big"
	"sync"

	"github.com/Gavine-Gao/blockchain-sync-sol/database"
)

type TransactionChannel struct {
	BusinessId      string
	BlockNumber     *big.Int
	BlockHash       string
	TxHash          string
	FromAddress     string
	ToAddress       string
	TokenAddress    string
	Amount          string
	TxFee           string
	TxStatus        uint8
	TxType          database.TransactionType
	ContractAddress string
}

// ==================== 交易 ChannelBank（流式） ====================

type txMinHeap []TransactionChannel

func (h txMinHeap) Len() int { return len(h) }
func (h txMinHeap) Less(i, j int) bool {
	return h[i].BlockNumber.Cmp(h[j].BlockNumber) < 0
}
func (h txMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *txMinHeap) Push(x interface{}) {
	*h = append(*h, x.(TransactionChannel))
}

func (h *txMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type ChannelBank struct {
	inputCh      chan TransactionChannel
	outputCh     chan TransactionChannel
	buffer       txMinHeap
	mu           sync.Mutex
	nextExpected uint64 // 期望的下一个区块号
}

// NewChannelBank 创建流式 ChannelBank
// startBlock: 期望的起始区块号，sorter 会从这个区块号开始有序输出
func NewChannelBank(bufferSize int, startBlock uint64) *ChannelBank {
	cb := &ChannelBank{
		inputCh:      make(chan TransactionChannel, bufferSize),
		outputCh:     make(chan TransactionChannel, bufferSize),
		buffer:       make(txMinHeap, 0),
		nextExpected: startBlock,
	}
	go cb.sorter()
	return cb
}

func (cb *ChannelBank) Push(tx TransactionChannel) {
	cb.inputCh <- tx
}

func (cb *ChannelBank) Channel() <-chan TransactionChannel {
	return cb.outputCh
}

func (cb *ChannelBank) Close() {
	close(cb.inputCh)
}

// sorter 流式排序：边收边输出
// 每次收到新数据入堆后，循环检查堆顶是否 == nextExpected，
// 是则立刻 Pop 输出，nextExpected++，继续检查（连续区块会连续输出）
func (cb *ChannelBank) sorter() {
	for tx := range cb.inputCh {
		cb.mu.Lock()
		heap.Push(&cb.buffer, tx)

		// 尝试输出所有连续的区块交易
		for len(cb.buffer) > 0 {
			top := cb.buffer[0]
			topBlock := top.BlockNumber.Uint64()

			if topBlock == cb.nextExpected || topBlock < cb.nextExpected {
				// 当前区块或更早的区块（同区块多笔交易），直接输出
				item := heap.Pop(&cb.buffer).(TransactionChannel)
				cb.mu.Unlock()
				cb.outputCh <- item
				cb.mu.Lock()

				// 检查堆里下一个是否还是同一个区块，不是才递增
				if len(cb.buffer) == 0 || cb.buffer[0].BlockNumber.Uint64() > cb.nextExpected {
					cb.nextExpected++
				}
			} else {
				// 堆顶超过期望区块号，等更多数据
				break
			}
		}
		cb.mu.Unlock()
	}

	// inputCh 关闭后，把堆里剩余的数据全部按序输出
	cb.mu.Lock()
	for len(cb.buffer) > 0 {
		tx := heap.Pop(&cb.buffer).(TransactionChannel)
		cb.mu.Unlock()
		cb.outputCh <- tx
		cb.mu.Lock()
	}
	cb.mu.Unlock()
	close(cb.outputCh)
}

// ==================== 区块头 BlockHeaderBank（流式） ====================

type BlockHeader struct {
	Number     *big.Int
	Hash       string
	ParentHash string
	Timestamp  uint64
}

type bhMinHeap []BlockHeader

func (h bhMinHeap) Len() int { return len(h) }
func (h bhMinHeap) Less(i, j int) bool {
	return h[i].Number.Cmp(h[j].Number) < 0
}
func (h bhMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *bhMinHeap) Push(x interface{}) {
	*h = append(*h, x.(BlockHeader))
}

func (h *bhMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type BlockHeaderBank struct {
	inputCh      chan BlockHeader
	outputCh     chan BlockHeader
	buffer       bhMinHeap
	mu           sync.Mutex
	nextExpected uint64
}

func NewBlockHeaderBank(bufferSize int, startBlock uint64) *BlockHeaderBank {
	bhb := &BlockHeaderBank{
		inputCh:      make(chan BlockHeader, bufferSize),
		outputCh:     make(chan BlockHeader, bufferSize),
		buffer:       make(bhMinHeap, 0),
		nextExpected: startBlock,
	}
	go bhb.sorter()
	return bhb
}

func (bhb *BlockHeaderBank) Push(bh BlockHeader) {
	bhb.inputCh <- bh
}

func (bhb *BlockHeaderBank) Channel() <-chan BlockHeader {
	return bhb.outputCh
}

func (bhb *BlockHeaderBank) Close() {
	close(bhb.inputCh)
}

// sorter 流式排序（与 ChannelBank 相同逻辑）
func (bhb *BlockHeaderBank) sorter() {
	for bh := range bhb.inputCh {
		bhb.mu.Lock()
		heap.Push(&bhb.buffer, bh)

		for len(bhb.buffer) > 0 {
			top := bhb.buffer[0]
			if top.Number.Uint64() == bhb.nextExpected {
				item := heap.Pop(&bhb.buffer).(BlockHeader)
				bhb.mu.Unlock()
				bhb.outputCh <- item
				bhb.mu.Lock()
				bhb.nextExpected++
			} else {
				break
			}
		}
		bhb.mu.Unlock()
	}

	bhb.mu.Lock()
	for len(bhb.buffer) > 0 {
		bh := heap.Pop(&bhb.buffer).(BlockHeader)
		bhb.mu.Unlock()
		bhb.outputCh <- bh
		bhb.mu.Lock()
	}
	bhb.mu.Unlock()
	close(bhb.outputCh)
}
