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
	cache *ristretto.Cache[string, *QueryResult]
}

// NewMemoryCacher creates a new MemoryCacher.
func NewMemoryCacher() *MemoryCacher {
	cache, _ := ristretto.NewCache(&ristretto.Config[string, *QueryResult]{
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

	return item, nil
}

// Set implements Cacher.
func (x *MemoryCacher) Set(_ context.Context, key *QueryKey, item *QueryResult, ttl time.Duration) error {
	// using # of rows as cost
	if ok := x.cache.SetWithTTL(key.String(), item, int64(len(item.Rows)), ttl); !ok {
		return fmt.Errorf("unable to set the item for key: %v", key.String())
	}

	return nil
}
