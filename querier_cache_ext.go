package pgxcache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
)

var _ QueryCacher = &MemoryQueryCacher{}

// MemoryQueryCacher is a simple in-memory cache implementation.
type MemoryQueryCacher struct {
	cache *ristretto.Cache[string, *QueryResult]
}

// NewMemoryQueryCacher creates a new MemoryCacher.
func NewMemoryQueryCacher() *MemoryQueryCacher {
	cache, _ := ristretto.NewCache(&ristretto.Config[string, *QueryResult]{
		MaxCost:     1 << 30,
		NumCounters: 1e7,
		BufferItems: 64,
	})

	return &MemoryQueryCacher{
		cache: cache,
	}
}

// Get implements Cacher.
func (x *MemoryQueryCacher) Get(_ context.Context, key *QueryKey) (*QueryResult, error) {
	item, ok := x.cache.Get(key.String())
	if !ok {
		return nil, nil
	}

	return item, nil
}

// Set implements Cacher.
func (x *MemoryQueryCacher) Set(_ context.Context, key *QueryKey, item *QueryResult, ttl time.Duration) error {
	// using # of rows as cost
	if ok := x.cache.SetWithTTL(key.String(), item, int64(len(item.Rows)), ttl); !ok {
		return fmt.Errorf("unable to set the item for key: %v", key.String())
	}

	return nil
}
