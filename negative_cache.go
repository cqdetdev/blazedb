package blazedb

import "sync"

const negativeCacheShardCount = 16
const negativeCacheMaxShardEntries = 4096

type negativeCache struct {
	shards [negativeCacheShardCount]negativeCacheShard
}

type negativeCacheShard struct {
	mu   sync.RWMutex
	keys map[chunkKey]struct{}
}

func newNegativeCache() *negativeCache {
	c := &negativeCache{}
	for i := range c.shards {
		c.shards[i].keys = make(map[chunkKey]struct{}, 128)
	}
	return c
}

func (c *negativeCache) shard(key chunkKey) *negativeCacheShard {
	h := uint32(key.pos[0])*31 + uint32(key.pos[1])
	return &c.shards[h&(negativeCacheShardCount-1)]
}

func (c *negativeCache) has(key chunkKey) bool {
	shard := c.shard(key)
	shard.mu.RLock()
	_, ok := shard.keys[key]
	shard.mu.RUnlock()
	return ok
}

func (c *negativeCache) put(key chunkKey) {
	shard := c.shard(key)
	shard.mu.Lock()
	if len(shard.keys) >= negativeCacheMaxShardEntries {
		shard.keys = make(map[chunkKey]struct{}, 128)
	}
	shard.keys[key] = struct{}{}
	shard.mu.Unlock()
}

func (c *negativeCache) delete(key chunkKey) {
	shard := c.shard(key)
	shard.mu.Lock()
	delete(shard.keys, key)
	shard.mu.Unlock()
}
