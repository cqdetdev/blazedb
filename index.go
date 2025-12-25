package blazedb

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/df-mc/dragonfly/server/world"
)

// spatialIndex provides fast lookups of chunk positions to file offsets
// using Z-order (Morton) curve encoding for spatial locality.
type spatialIndex struct {
	mu      sync.RWMutex
	entries map[uint64]indexEntry // Morton code -> entry
}

// indexEntry holds the file offset and size of a chunk.
type indexEntry struct {
	offset int64
	size   int64
	dim    int32
}

// newSpatialIndex creates a new spatial index.
func newSpatialIndex() *spatialIndex {
	return &spatialIndex{
		entries: make(map[uint64]indexEntry),
	}
}

// mortonEncode encodes 2D chunk coordinates into a Z-order (Morton) code.
// This arranges chunks spatially so nearby chunks are close in storage.
//
// The Morton code interleaves the bits of X and Z coordinates:
//
//	X: 0b1010 -> bits at positions 1, 3, 5, 7
//	Z: 0b1100 -> bits at positions 0, 2, 4, 6
//	Result: interleaved bits forming the Morton code
func mortonEncode(x, z int32) uint64 {
	// Convert to unsigned, handling negative coordinates
	ux := uint64(uint32(x))
	uz := uint64(uint32(z))

	// Spread bits using magic numbers
	ux = (ux | (ux << 16)) & 0x0000FFFF0000FFFF
	ux = (ux | (ux << 8)) & 0x00FF00FF00FF00FF
	ux = (ux | (ux << 4)) & 0x0F0F0F0F0F0F0F0F
	ux = (ux | (ux << 2)) & 0x3333333333333333
	ux = (ux | (ux << 1)) & 0x5555555555555555

	uz = (uz | (uz << 16)) & 0x0000FFFF0000FFFF
	uz = (uz | (uz << 8)) & 0x00FF00FF00FF00FF
	uz = (uz | (uz << 4)) & 0x0F0F0F0F0F0F0F0F
	uz = (uz | (uz << 2)) & 0x3333333333333333
	uz = (uz | (uz << 1)) & 0x5555555555555555

	return ux | (uz << 1)
}

// mortonDecode decodes a Morton code back to X and Z coordinates.
func mortonDecode(code uint64) (x, z int32) {
	ux := code & 0x5555555555555555
	uz := (code >> 1) & 0x5555555555555555

	ux = (ux | (ux >> 1)) & 0x3333333333333333
	ux = (ux | (ux >> 2)) & 0x0F0F0F0F0F0F0F0F
	ux = (ux | (ux >> 4)) & 0x00FF00FF00FF00FF
	ux = (ux | (ux >> 8)) & 0x0000FFFF0000FFFF
	ux = (ux | (ux >> 16)) & 0x00000000FFFFFFFF

	uz = (uz | (uz >> 1)) & 0x3333333333333333
	uz = (uz | (uz >> 2)) & 0x0F0F0F0F0F0F0F0F
	uz = (uz | (uz >> 4)) & 0x00FF00FF00FF00FF
	uz = (uz | (uz >> 8)) & 0x0000FFFF0000FFFF
	uz = (uz | (uz >> 16)) & 0x00000000FFFFFFFF

	return int32(uint32(ux)), int32(uint32(uz))
}

// makeKey creates an index key from chunk position and dimension.
func makeKey(key chunkKey) (uint64, int32) {
	dimID, _ := world.DimensionID(key.dim)
	morton := mortonEncode(key.pos[0], key.pos[1])
	// Combine Morton code with dimension in high bits
	return morton, int32(dimID)
}

// get retrieves the offset and size for a chunk, returning false if not found.
func (idx *spatialIndex) get(key chunkKey) (offset, size int64, exists bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	morton, dimID := makeKey(key)
	// Include dimension in lookup key
	lookupKey := morton ^ (uint64(dimID) << 48)

	entry, ok := idx.entries[lookupKey]
	if !ok {
		return 0, 0, false
	}
	return entry.offset, entry.size, true
}

// put adds or updates an entry in the index.
func (idx *spatialIndex) put(key chunkKey, offset, size int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	morton, dimID := makeKey(key)
	lookupKey := morton ^ (uint64(dimID) << 48)

	idx.entries[lookupKey] = indexEntry{
		offset: offset,
		size:   size,
		dim:    dimID,
	}
}

// delete removes an entry from the index.
func (idx *spatialIndex) delete(key chunkKey) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	morton, dimID := makeKey(key)
	lookupKey := morton ^ (uint64(dimID) << 48)
	delete(idx.entries, lookupKey)
}

// save persists the index to a file.
func (idx *spatialIndex) save(path string) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write header: magic + count
	header := make([]byte, 12)
	copy(header[:4], "BIDX")
	binary.LittleEndian.PutUint64(header[4:], uint64(len(idx.entries)))
	if _, err := f.Write(header); err != nil {
		return err
	}

	// Write entries: key (8) + offset (8) + size (8) + dim (4) = 28 bytes each
	entry := make([]byte, 28)
	for key, e := range idx.entries {
		binary.LittleEndian.PutUint64(entry[0:], key)
		binary.LittleEndian.PutUint64(entry[8:], uint64(e.offset))
		binary.LittleEndian.PutUint64(entry[16:], uint64(e.size))
		binary.LittleEndian.PutUint32(entry[24:], uint32(e.dim))
		if _, err := f.Write(entry); err != nil {
			return err
		}
	}

	return nil
}

// load reads the index from a file.
func (idx *spatialIndex) load(path string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	if len(data) < 12 || string(data[:4]) != "BIDX" {
		return os.ErrInvalid
	}

	count := binary.LittleEndian.Uint64(data[4:12])
	expectedLen := 12 + count*28
	if uint64(len(data)) < expectedLen {
		return os.ErrInvalid
	}

	idx.entries = make(map[uint64]indexEntry, count)

	for i := uint64(0); i < count; i++ {
		off := 12 + i*28
		key := binary.LittleEndian.Uint64(data[off:])
		offset := int64(binary.LittleEndian.Uint64(data[off+8:]))
		size := int64(binary.LittleEndian.Uint64(data[off+16:]))
		dim := int32(binary.LittleEndian.Uint32(data[off+24:]))

		idx.entries[key] = indexEntry{
			offset: offset,
			size:   size,
			dim:    dim,
		}
	}

	return nil
}

// count returns the number of entries in the index.
func (idx *spatialIndex) count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

// iterate calls fn for each entry in the index in Morton order.
func (idx *spatialIndex) iterate(fn func(key chunkKey, offset, size int64) bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	for code, entry := range idx.entries {
		// Extract dimension and Morton code
		morton := code & 0x0000FFFFFFFFFFFF
		x, z := mortonDecode(morton)

		dim, _ := world.DimensionByID(int(entry.dim))
		key := chunkKey{
			pos: world.ChunkPos{x, z},
			dim: dim,
		}

		if !fn(key, entry.offset, entry.size) {
			return
		}
	}
}
