package worker

import (
	"context"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/ethereum/go-ethereum/log"

	"github.com/Gavine-Gao/blockchain-sync-sol/database"
)

// AddressInfo 地址信息
type AddressInfo struct {
	BusinessId  string
	AddressType database.AddressType
}

// AddressCache 布隆过滤器 + 内存 map 双层地址缓存
// 布隆过滤器快速判断"一定不存在"，map 获取地址类型
type AddressCache struct {
	mu     sync.RWMutex
	filter *bloom.BloomFilter
	store  map[string][]AddressInfo // key: address, value: 可能属于多个 business
	db     *database.DB
}

// NewAddressCache 从 DB 加载所有地址，构建布隆过滤器和 map
func NewAddressCache(db *database.DB) (*AddressCache, error) {
	cache := &AddressCache{
		filter: bloom.New(100_000_000, 3),
		store:  make(map[string][]AddressInfo),
		db:     db,
	}

	if err := cache.reload(); err != nil {
		return nil, err
	}
	return cache, nil
}

// reload 从 DB 全量加载地址并原子替换缓存
func (c *AddressCache) reload() error {
	businessList, err := c.db.Business.QueryBusinessList()
	if err != nil {
		return err
	}

	newFilter := bloom.New(100_000_000, 3)
	newStore := make(map[string][]AddressInfo)
	totalAddresses := 0

	for _, business := range businessList {
		addresses, err := c.db.Addresses.GetAllAddresses(business.BusinessUid)
		if err != nil {
			log.Error("load addresses fail", "businessId", business.BusinessUid, "err", err)
			continue
		}

		for _, addr := range addresses {
			newFilter.AddString(addr.Address)
			newStore[addr.Address] = append(newStore[addr.Address], AddressInfo{
				BusinessId:  business.BusinessUid,
				AddressType: addr.AddressType,
			})
			totalAddresses++
		}
	}

	// 原子替换
	c.mu.Lock()
	c.filter = newFilter
	c.store = newStore
	c.mu.Unlock()

	log.Info("address cache loaded", "totalAddresses", totalAddresses, "businesses", len(businessList))
	return nil
}

// StartRefresher 定期从 DB 全量刷新缓存
func (c *AddressCache) StartRefresher(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := c.reload(); err != nil {
					log.Error("address cache reload fail", "err", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// MayExist 布隆过滤器快速判断：返回 false 表示一定不存在
func (c *AddressCache) MayExist(address string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.filter.TestString(address)
}

// Lookup 查找地址信息：返回该地址所属的 business 和类型
func (c *AddressCache) Lookup(address string) ([]AddressInfo, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	infos, exists := c.store[address]
	return infos, exists
}

// AddAddress 动态添加地址（新注册地址时调用）
func (c *AddressCache) AddAddress(address string, businessId string, addrType database.AddressType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.filter.AddString(address)
	c.store[address] = append(c.store[address], AddressInfo{
		BusinessId:  businessId,
		AddressType: addrType,
	})
}
