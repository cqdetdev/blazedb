package blazedb

import (
	"sync"
)

const (
	blockSize      = 4096 // 4KB blocks, matches OS page size
	blockCacheSize = 64   // Number of blocks to cache (256KB total)
	blockCacheMask = blockCacheSize - 1
)

// blockCache provides a simple LRU cache for disk blocks.
// This improves read performance when loading nearby chunks
// by avoiding redundant disk reads for adjacent data.
type blockCache struct {
	mu      sync.RWMutex
	blocks  [blockCacheSize]cachedBlock
	counter uint64 // Access counter for LRU
}

type cachedBlock struct {
	offset   int64
	data     []byte
	lastUsed uint64
	valid    bool
}

// newBlockCache creates a new block cache.
func newBlockCache() *blockCache {
	return &blockCache{}
}

// read reads data from the cache or falls back to disk.
// The readFn is called when data is not in cache.
func (bc *blockCache) read(offset, length int64, readFn func(offset, length int64) ([]byte, error)) ([]byte, error) {
	// For small reads, check if we have the block cached
	blockOffset := offset - (offset % blockSize)
	blockEnd := offset + length

	// If read spans multiple blocks or is very large, read directly
	if blockEnd-blockOffset > blockSize*2 || length > blockSize*2 {
		return readFn(offset, length)
	}

	bc.mu.RLock()
	// Check if block is in cache
	for i := range bc.blocks {
		if bc.blocks[i].valid && bc.blocks[i].offset == blockOffset {
			block := bc.blocks[i]
			bc.mu.RUnlock()

			// Extract the requested portion
			startInBlock := int(offset - blockOffset)
			endInBlock := startInBlock + int(length)
			if endInBlock > len(block.data) {
				// Data extends beyond cached block, read directly
				return readFn(offset, length)
			}
			return block.data[startInBlock:endInBlock], nil
		}
	}
	bc.mu.RUnlock()

	// Cache miss - read from disk
	data, err := readFn(offset, length)
	if err != nil {
		return nil, err
	}

	// Optionally cache the block for future reads
	// Only cache if the read was smaller than a block (likely part of sequential access)
	if length <= blockSize/2 {
		bc.cacheBlock(blockOffset, readFn)
	}

	return data, nil
}

// cacheBlock reads and caches a block from disk.
func (bc *blockCache) cacheBlock(blockOffset int64, readFn func(offset, length int64) ([]byte, error)) {
	data, err := readFn(blockOffset, blockSize)
	if err != nil {
		return // Ignore errors, caching is best-effort
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Find LRU slot or invalid slot
	minIdx := 0
	minUsed := bc.blocks[0].lastUsed

	for i := range bc.blocks {
		if !bc.blocks[i].valid {
			minIdx = i
			break
		}
		if bc.blocks[i].lastUsed < minUsed {
			minUsed = bc.blocks[i].lastUsed
			minIdx = i
		}
	}

	bc.counter++
	bc.blocks[minIdx] = cachedBlock{
		offset:   blockOffset,
		data:     data,
		lastUsed: bc.counter,
		valid:    true,
	}
}

// invalidate removes all cached blocks.
func (bc *blockCache) invalidate() {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	for i := range bc.blocks {
		bc.blocks[i].valid = false
		bc.blocks[i].data = nil
	}
}
