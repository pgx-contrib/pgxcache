package pgxcache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
)

var _ QueryCacher = &MemoryCacher{}

// MemoryCacher is a simple in-memory cache implementation.
type MemoryCacher struct {
	cache *ristretto.Cache
}

// NewMemoryCacher creates a new MemoryCacher.
func NewMemoryCacher() *MemoryCacher {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		MaxCost:     1 << 30,
		NumCounters: 1e7,
		BufferItems: 64,
	})

	return &MemoryCacher{
		cache: cache,
	}
}

// Get implements Cacher.
func (x *MemoryCacher) Get(_ context.Context, key *QueryKey) (*QueryResult, error) {
	item, ok := x.cache.Get(key.String())
	if !ok {
		return nil, nil
	}

	data, ok := item.(*QueryResult)
	if !ok {
		return nil, fmt.Errorf("invalid cache item type %T", item)
	}

	return data, nil
}

// Set implements Cacher.
func (x *MemoryCacher) Set(_ context.Context, key *QueryKey, item *QueryResult, ttl time.Duration) error {
	// using # of rows as cost
	_ = x.cache.SetWithTTL(key.String(), item, int64(len(item.Rows)), ttl)
	return nil
}
