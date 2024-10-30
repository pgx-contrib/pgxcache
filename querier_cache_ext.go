package pgxcache

import (
	"context"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

var _ QueryCacher = &MemoryQueryCacher{}

// MemoryQueryCacher is a simple in-memory cache implementation.
type MemoryQueryCacher struct {
	cache *ristretto.Cache[string, []byte]
}

// NewMemoryQueryCacher creates a new MemoryCacher.
func NewMemoryQueryCacher() *MemoryQueryCacher {
	cache, _ := ristretto.NewCache(&ristretto.Config[string, []byte]{
		MaxCost:     1 << 30,
		NumCounters: 1e7,
		BufferItems: 64,
	})

	return &MemoryQueryCacher{
		cache: cache,
	}
}

// Get implements Cacher.
func (x *MemoryQueryCacher) Get(_ context.Context, key *QueryKey) (*QueryItem, error) {
	// get the data from the cache
	data, ok := x.cache.Get(key.String())
	if !ok {
		return nil, nil
	}

	item := &QueryItem{}
	// unmarshal the data into the item
	if err := item.UnmarshalText(data); err != nil {
		return nil, err
	}

	return item, nil
}

// Set implements Cacher.
func (x *MemoryQueryCacher) Set(_ context.Context, key *QueryKey, item *QueryItem, lifetime time.Duration) error {
	// marshal the item into bytes
	data, err := item.MarshalText()
	if err != nil {
		return err
	}

	// using # of rows as cost
	if ok := x.cache.SetWithTTL(key.String(), data, int64(len(data)), lifetime); !ok {
		return fmt.Errorf("unable to set the item for key: %v", key.String())
	}

	return nil
}

// Reset resets the cache.
func (x *MemoryQueryCacher) Reset(_ context.Context) error {
	x.cache.Clear()
	return nil
}
