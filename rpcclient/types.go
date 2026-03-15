package rpcclient

import "math/big"

// BlockHeader 区块头信息，用于同步和重组检测
type BlockHeader struct {
	Hash       string
	ParentHash string
	Number     *big.Int
	Timestamp  uint64
}
