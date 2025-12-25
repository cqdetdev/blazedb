package blazedb

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/df-mc/dragonfly/server/world/chunk"
)

// chunkCache implements a high-performance thread-safe LRU cache for chunks.
// Uses container/list for O(1) LRU operations and sharding to reduce lock contention.
type chunkCache struct {
	shards    [cacheShardCount]*cacheShard
	hits      atomic.Int64
	misses    atomic.Int64
	reads     atomic.Int64
	writes    atomic.Int64
	maxSize   int64
	shardSize int64
}

const cacheShardCount = 16

// cacheShard is a single shard of the cache.
type cacheShard struct {
	mu      sync.RWMutex
	entries map[chunkKey]*list.Element
	lru     *list.List
	size    atomic.Int64
	maxSize int64
}

// cacheEntry represents a cached chunk with metadata.
type cacheEntry struct {
	key        chunkKey
	col        *chunk.Column
	lastAccess time.Time
	size       int64
	dirty      bool
}

// newChunkCache creates a new chunk cache with the given max size in bytes.
func newChunkCache(maxSize int64) *chunkCache {
	c := &chunkCache{
		maxSize:   maxSize,
		shardSize: maxSize / cacheShardCount,
	}
	for i := 0; i < cacheShardCount; i++ {
		c.shards[i] = &cacheShard{
			entries: make(map[chunkKey]*list.Element, 256),
			lru:     list.New(),
			maxSize: c.shardSize,
		}
	}
	return c
}

// shard returns the shard for a given key using fast hash.
func (c *chunkCache) shard(key chunkKey) *cacheShard {
	// Fast hash using position only - dimension is already encoded in the key
	// Avoid calling key.dim.Range() which is expensive
	h := uint32(key.pos[0])*31 + uint32(key.pos[1])
	return c.shards[h&(cacheShardCount-1)] // Bitwise AND is faster than modulo for power of 2
}

// get retrieves a chunk from the cache, returning nil if not found.
// Uses probabilistic LRU updates to avoid lock contention on every read.
func (c *chunkCache) get(key chunkKey) *chunk.Column {
	c.reads.Add(1)
	shard := c.shard(key)

	shard.mu.RLock()
	elem, ok := shard.entries[key]
	if !ok {
		shard.mu.RUnlock()
		c.misses.Add(1)
		return nil
	}
	entry := elem.Value.(*cacheEntry)
	col := entry.col
	shard.mu.RUnlock()

	c.hits.Add(1)

	// Probabilistic LRU update: only move to front ~1/16 of the time
	// This dramatically reduces write lock contention while maintaining
	// approximate LRU ordering. Uses fast atomic counter instead of random.
	if c.reads.Load()&0xF == 0 {
		shard.mu.Lock()
		// Double-check entry still exists (could have been evicted)
		if _, exists := shard.entries[key]; exists {
			shard.lru.MoveToFront(elem)
		}
		shard.mu.Unlock()
	}

	return col
}

// put adds a chunk to the cache, marking it as dirty.
func (c *chunkCache) put(key chunkKey, col *chunk.Column) {
	c.writes.Add(1)
	shard := c.shard(key)

	// Estimate size
	size := int64(len(col.Chunk.Sub()) * 4096 * 2)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Update existing entry
	if elem, ok := shard.entries[key]; ok {
		entry := elem.Value.(*cacheEntry)
		entry.col = col
		entry.lastAccess = time.Now()
		entry.dirty = true
		shard.lru.MoveToFront(elem)
		return
	}

	// Evict if needed - O(1) eviction from back
	for shard.size.Load()+size > shard.maxSize && shard.lru.Len() > 0 {
		oldest := shard.lru.Back()
		if oldest == nil {
			break
		}
		oldEntry := oldest.Value.(*cacheEntry)
		shard.size.Add(-oldEntry.size)
		delete(shard.entries, oldEntry.key)
		shard.lru.Remove(oldest)
	}

	// Add new entry
	entry := &cacheEntry{
		key:        key,
		col:        col,
		lastAccess: time.Now(),
		size:       size,
		dirty:      true,
	}
	elem := shard.lru.PushFront(entry)
	shard.entries[key] = elem
	shard.size.Add(size)
}

// putClean adds a chunk loaded from disk (not dirty).
func (c *chunkCache) putClean(key chunkKey, col *chunk.Column) {
	shard := c.shard(key)
	size := int64(len(col.Chunk.Sub()) * 4096 * 2)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Evict if needed
	for shard.size.Load()+size > shard.maxSize && shard.lru.Len() > 0 {
		oldest := shard.lru.Back()
		if oldest == nil {
			break
		}
		oldEntry := oldest.Value.(*cacheEntry)
		shard.size.Add(-oldEntry.size)
		delete(shard.entries, oldEntry.key)
		shard.lru.Remove(oldest)
	}

	entry := &cacheEntry{
		key:        key,
		col:        col,
		lastAccess: time.Now(),
		size:       size,
		dirty:      false,
	}
	elem := shard.lru.PushFront(entry)
	shard.entries[key] = elem
	shard.size.Add(size)
}

// markClean marks a chunk as clean (saved to disk).
func (c *chunkCache) markClean(key chunkKey) {
	shard := c.shard(key)
	shard.mu.Lock()
	if elem, ok := shard.entries[key]; ok {
		elem.Value.(*cacheEntry).dirty = false
	}
	shard.mu.Unlock()
}

// getDirtyChunks returns all chunks that have been modified.
func (c *chunkCache) getDirtyChunks() []chunkKey {
	dirty := make([]chunkKey, 0, 64)
	for _, shard := range c.shards {
		shard.mu.RLock()
		for key, elem := range shard.entries {
			if elem.Value.(*cacheEntry).dirty {
				dirty = append(dirty, key)
			}
		}
		shard.mu.RUnlock()
	}
	return dirty
}

// currentSize returns the current estimated size of the cache.
func (c *chunkCache) currentSize() int64 {
	var total int64
	for _, shard := range c.shards {
		total += shard.size.Load()
	}
	return total
}

// stats returns performance statistics.
func (c *chunkCache) stats() *Stats {
	return &Stats{
		ChunkReads:  c.reads.Load(),
		ChunkWrites: c.writes.Load(),
		CacheHits:   c.hits.Load(),
		CacheMisses: c.misses.Load(),
	}
}

// clear empties the cache.
func (c *chunkCache) clear() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.entries = make(map[chunkKey]*list.Element, 256)
		shard.lru.Init()
		shard.size.Store(0)
		shard.mu.Unlock()
	}
}

// len returns the number of entries in the cache.
func (c *chunkCache) len() int {
	var total int
	for _, shard := range c.shards {
		shard.mu.RLock()
		total += len(shard.entries)
		shard.mu.RUnlock()
	}
	return total
}

// hitRate returns the current cache hit rate (0.0 to 1.0).
func (c *chunkCache) hitRate() float64 {
	reads := c.reads.Load()
	if reads == 0 {
		return 1.0 // No reads yet, assume perfect
	}
	return float64(c.hits.Load()) / float64(reads)
}

// AutoTune adjusts cache size based on hit rate and available memory.
// Call this periodically (e.g., every 30 seconds).
// minSize and maxSize define the bounds for auto-tuning.
func (c *chunkCache) AutoTune(minSize, maxSize int64) {
	hitRate := c.hitRate()
	currentSize := c.maxSize

	if hitRate < 0.8 && currentSize < maxSize {
		// Low hit rate - grow cache by 20%
		newSize := int64(float64(currentSize) * 1.2)
		if newSize > maxSize {
			newSize = maxSize
		}
		c.resize(newSize)
	} else if hitRate > 0.95 && currentSize > minSize {
		// Very high hit rate - can shrink to save memory (10%)
		newSize := int64(float64(currentSize) * 0.9)
		if newSize < minSize {
			newSize = minSize
		}
		c.resize(newSize)
	}
}

// resize changes the maximum cache size.
func (c *chunkCache) resize(newMaxSize int64) {
	c.maxSize = newMaxSize
	newShardSize := newMaxSize / cacheShardCount
	c.shardSize = newShardSize

	// Update each shard's max size
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.maxSize = newShardSize
		// Evict excess if shrinking
		for shard.size.Load() > newShardSize && shard.lru.Len() > 0 {
			oldest := shard.lru.Back()
			if oldest == nil {
				break
			}
			oldEntry := oldest.Value.(*cacheEntry)
			shard.size.Add(-oldEntry.size)
			delete(shard.entries, oldEntry.key)
			shard.lru.Remove(oldest)
		}
		shard.mu.Unlock()
	}
}

// GetMaxSize returns the current maximum cache size.
func (c *chunkCache) GetMaxSize() int64 {
	return c.maxSize
}
