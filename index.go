package blazedb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/df-mc/dragonfly/server/world"
)

// spatialIndex provides fast lookups of chunk positions to file offsets
// using Z-order (Morton) curve encoding for spatial locality.
type spatialIndex struct {
	mu            sync.RWMutex
	entries       map[indexKey]indexEntry
	savedDataSize int64
}

type indexKey struct {
	morton uint64
	dim    int32
}

// indexEntry holds the file offset and size of a chunk.
type indexEntry struct {
	offset int64
	size   int64
	dim    int32
}

type indexUpdate struct {
	key    chunkKey
	offset int64
	size   int64
}

type indexSnapshotEntry struct {
	key   indexKey
	entry indexEntry
}

// newSpatialIndex creates a new spatial index.
func newSpatialIndex() *spatialIndex {
	return &spatialIndex{
		entries:       make(map[indexKey]indexEntry),
		savedDataSize: -1,
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
	morton := mortonEncode(key.x, key.z)
	return morton, key.dimID
}

func indexLookupKey(key chunkKey) (uint64, int32) {
	morton, dimID := makeKey(key)
	return morton, dimID
}

// get retrieves the offset and size for a chunk, returning false if not found.
func (idx *spatialIndex) get(key chunkKey) (offset, size int64, exists bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	morton, dimID := indexLookupKey(key)
	lookupKey := indexKey{morton: morton, dim: dimID}

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

	morton, dimID := indexLookupKey(key)
	lookupKey := indexKey{morton: morton, dim: dimID}

	idx.entries[lookupKey] = indexEntry{
		offset: offset,
		size:   size,
		dim:    dimID,
	}
}

// putBatch adds or updates multiple entries while holding the index lock once.
func (idx *spatialIndex) putBatch(updates []indexUpdate) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	for _, update := range updates {
		morton, dimID := indexLookupKey(update.key)
		lookupKey := indexKey{morton: morton, dim: dimID}
		idx.entries[lookupKey] = indexEntry{
			offset: update.offset,
			size:   update.size,
			dim:    dimID,
		}
	}
}

// delete removes an entry from the index.
func (idx *spatialIndex) delete(key chunkKey) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	morton, dimID := indexLookupKey(key)
	lookupKey := indexKey{morton: morton, dim: dimID}
	delete(idx.entries, lookupKey)
}

// save persists the index to a file.
func (idx *spatialIndex) save(path string) error {
	return idx.saveWithDataSize(path, -1, false)
}

func (idx *spatialIndex) saveWithDataSize(path string, dataSize int64, syncFile bool) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	w := bufio.NewWriterSize(tmp, 256*1024)

	// Write header: magic + count
	header := make([]byte, 12)
	copy(header[:4], "BIDX")
	binary.LittleEndian.PutUint64(header[4:], uint64(len(idx.entries)))
	if _, err := w.Write(header); err != nil {
		_ = tmp.Close()
		return err
	}

	// Write entries: key (8) + offset (8) + size (8) + dim (4) = 28 bytes each
	entry := make([]byte, 28)
	for key, e := range idx.entries {
		binary.LittleEndian.PutUint64(entry[0:], key.morton)
		binary.LittleEndian.PutUint64(entry[8:], uint64(e.offset))
		binary.LittleEndian.PutUint64(entry[16:], uint64(e.size))
		binary.LittleEndian.PutUint32(entry[24:], uint32(e.dim))
		if _, err := w.Write(entry); err != nil {
			_ = tmp.Close()
			return err
		}
	}

	if dataSize >= 0 {
		footer := make([]byte, 12)
		copy(footer[:4], "BDAT")
		binary.LittleEndian.PutUint64(footer[4:], uint64(dataSize))
		if _, err := w.Write(footer); err != nil {
			_ = tmp.Close()
			return err
		}
	}

	if err := w.Flush(); err != nil {
		_ = tmp.Close()
		return err
	}
	if syncFile {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceFile(tmpPath, path); err != nil {
		return err
	}
	removeTmp = false
	return nil
}

func replaceFile(tmpPath, path string) error {
	backupPath := path + ".old"
	_ = os.Remove(backupPath)

	targetExists := fileExists(path)
	if targetExists {
		if err := os.Rename(path, backupPath); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if targetExists {
			_ = os.Rename(backupPath, path)
		}
		return err
	}
	if targetExists {
		if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove old index backup: %w", err)
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
	if count > (uint64(len(data))-12)/28 {
		return os.ErrInvalid
	}
	expectedLen := 12 + count*28

	idx.entries = make(map[indexKey]indexEntry, count)
	idx.savedDataSize = -1

	for i := uint64(0); i < count; i++ {
		off := 12 + i*28
		morton := binary.LittleEndian.Uint64(data[off:])
		offset := int64(binary.LittleEndian.Uint64(data[off+8:]))
		size := int64(binary.LittleEndian.Uint64(data[off+16:]))
		dim := int32(binary.LittleEndian.Uint32(data[off+24:]))
		if offset < 0 || size <= 0 {
			return os.ErrInvalid
		}

		idx.entries[indexKey{morton: morton, dim: dim}] = indexEntry{
			offset: offset,
			size:   size,
			dim:    dim,
		}
	}
	footerOffset := int(expectedLen)
	if len(data)-footerOffset >= 12 && string(data[footerOffset:footerOffset+4]) == "BDAT" {
		idx.savedDataSize = int64(binary.LittleEndian.Uint64(data[footerOffset+4:]))
	}

	return nil
}

// count returns the number of entries in the index.
func (idx *spatialIndex) count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.entries)
}

func (idx *spatialIndex) needsRebuild(dataSize int64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.savedDataSize >= 0 {
		return idx.savedDataSize != dataSize
	}
	return idx.maxEndLocked() != dataSize
}

func (idx *spatialIndex) maxEndLocked() int64 {
	var maxEnd int64
	for _, entry := range idx.entries {
		end := entry.offset + entry.size
		if end > maxEnd {
			maxEnd = end
		}
	}
	return maxEnd
}

func (idx *spatialIndex) snapshotSorted() []indexSnapshotEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := make([]indexSnapshotEntry, 0, len(idx.entries))
	for key, entry := range idx.entries {
		entries = append(entries, indexSnapshotEntry{key: key, entry: entry})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].key.dim != entries[j].key.dim {
			return entries[i].key.dim < entries[j].key.dim
		}
		return entries[i].key.morton < entries[j].key.morton
	})
	return entries
}

func (idx *spatialIndex) replace(entries []indexSnapshotEntry) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	next := make(map[indexKey]indexEntry, len(entries))
	for _, snapshot := range entries {
		next[snapshot.key] = snapshot.entry
	}
	idx.entries = next
}

// iterate calls fn for each entry in the index in Morton order.
func (idx *spatialIndex) iterate(fn func(key chunkKey, offset, size int64) bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	keys := make([]indexKey, 0, len(idx.entries))
	for key := range idx.entries {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].dim != keys[j].dim {
			return keys[i].dim < keys[j].dim
		}
		return keys[i].morton < keys[j].morton
	})

	for _, idxKey := range keys {
		entry := idx.entries[idxKey]

		x, z := mortonDecode(idxKey.morton)

		if _, ok := world.DimensionByID(int(idxKey.dim)); !ok {
			continue
		}
		chunk := chunkKey{x: x, z: z, dimID: idxKey.dim}

		if !fn(chunk, entry.offset, entry.size) {
			return
		}
	}
}
