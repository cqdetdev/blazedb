package blazedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

// ErrDeltaRecordsDisabled is returned when StoreBlockDeltas is called without
// Options.EnableDeltaRecords.
var ErrDeltaRecordsDisabled = errors.New("delta records are disabled")

const (
	deltaRecordHeaderSize = 34
	deltaRecordEntrySize  = 9
	maxDeltaRecordSize    = 16 * 1024 * 1024
)

// BlockDelta is a single block mutation inside a chunk column.
//
// Pos may be an absolute world block position. StoreBlockDeltas uses Pos.X()
// and Pos.Z() modulo 16 to address the block within the provided chunk
// position, and Pos.Y() as the dimension-relative Y coordinate.
type BlockDelta struct {
	Pos       cube.Pos
	Layer     uint8
	RuntimeID uint32
}

type deltaIndex struct {
	mu      sync.RWMutex
	entries map[chunkKey][]deltaEntry
}

type deltaEntry struct {
	offset     int64
	size       int64
	baseOffset int64
}

func newDeltaIndex() *deltaIndex {
	return &deltaIndex{entries: make(map[chunkKey][]deltaEntry)}
}

func (idx *deltaIndex) put(key chunkKey, entry deltaEntry) {
	idx.mu.Lock()
	idx.entries[key] = append(idx.entries[key], entry)
	idx.mu.Unlock()
}

func (idx *deltaIndex) getForBase(key chunkKey, baseOffset int64) []deltaEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	entries := idx.entries[key]
	if len(entries) == 0 {
		return nil
	}
	matched := make([]deltaEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.baseOffset == baseOffset {
			matched = append(matched, entry)
		}
	}
	return matched
}

func (idx *deltaIndex) clear(key chunkKey) {
	idx.mu.Lock()
	delete(idx.entries, key)
	idx.mu.Unlock()
}

func (idx *deltaIndex) count() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var n int64
	for _, entries := range idx.entries {
		n += int64(len(entries))
	}
	return n
}

func (db *DB) clearDeltasForFullWrite(key chunkKey) {
	if db.delta != nil {
		db.delta.clear(key)
		db.deltaRecords.Store(db.delta.count())
	}
}

// StoreBlockDeltas appends a compact delta record for a chunk that already has
// a full base record. It is intended for servers that know the exact block
// changes from their event layer and want to avoid re-encoding a full chunk
// column for small edits.
//
// Delta records are opt-in through Options.EnableDeltaRecords. A later
// StoreColumn for the same chunk snapshots a new full base record and causes
// older deltas to be ignored.
func (db *DB) StoreBlockDeltas(pos world.ChunkPos, dim world.Dimension, deltas []BlockDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	if db.delta == nil || db.deltaFd == nil {
		return ErrDeltaRecordsDisabled
	}
	key := newChunkKey(pos, dim)

	db.writeBufMu.Lock()
	if col, ok := db.writeBuf[key]; ok {
		err := applyBlockDeltasToColumn(col, deltas)
		db.writeBufMu.Unlock()
		if err != nil {
			return err
		}
		if cached := db.cache.get(key); cached != nil && cached != col {
			_ = applyBlockDeltasToColumn(cached, deltas)
		}
		return nil
	}
	if col, ok := db.flushingBuf[key]; ok {
		err := applyBlockDeltasToColumn(col, deltas)
		db.writeBufMu.Unlock()
		if err != nil {
			return err
		}
		if cached := db.cache.get(key); cached != nil && cached != col {
			_ = applyBlockDeltasToColumn(cached, deltas)
		}
		return nil
	}
	db.writeBufMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	baseOffset, _, exists := db.index.get(key)
	if !exists {
		return ErrNotFound
	}
	data, err := encodeDeltaRecord(key, baseOffset, deltas)
	if err != nil {
		return err
	}

	offset := db.deltaOffset.Load()
	if _, err := db.deltaFd.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write delta record: %w", err)
	}
	if db.conf.Options.Durability >= DurabilityBalanced {
		if err := db.deltaFd.Sync(); err != nil {
			return fmt.Errorf("sync delta record: %w", err)
		}
	}
	db.deltaOffset.Add(int64(len(data)))
	db.delta.put(key, deltaEntry{offset: offset, size: int64(len(data)), baseOffset: baseOffset})
	db.deltaRecords.Store(db.delta.count())

	if cached := db.cache.get(key); cached != nil {
		if err := applyBlockDeltasToColumn(cached, deltas); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) applyIndexedDeltas(key chunkKey, baseOffset int64, col *chunk.Column) error {
	if db.delta == nil || db.deltaFd == nil {
		return nil
	}
	entries := db.delta.getForBase(key, baseOffset)
	if len(entries) == 0 {
		return nil
	}
	for _, entry := range entries {
		deltas, err := db.readDeltaRecord(entry)
		if err != nil {
			return err
		}
		if err := applyBlockDeltasToColumn(col, deltas); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) readDeltaRecord(entry deltaEntry) ([]BlockDelta, error) {
	if entry.size < deltaRecordHeaderSize || entry.size > maxDeltaRecordSize {
		return nil, fmt.Errorf("invalid delta record size %d", entry.size)
	}
	data := make([]byte, entry.size)
	if _, err := db.deltaFd.ReadAt(data, entry.offset); err != nil {
		return nil, fmt.Errorf("read delta record: %w", err)
	}
	_, _, deltas, err := decodeDeltaRecord(data)
	return deltas, err
}

func (db *DB) rebuildDeltaIndex() error {
	if db.delta == nil || db.deltaFd == nil {
		return nil
	}
	stat, err := db.deltaFd.Stat()
	if err != nil {
		return fmt.Errorf("stat deltas.dat: %w", err)
	}

	idx := newDeltaIndex()
	fileSize := stat.Size()
	offset := int64(0)
	header := make([]byte, deltaRecordHeaderSize)
	for offset < fileSize {
		remaining := fileSize - offset
		if remaining < deltaRecordHeaderSize {
			if err := db.truncateDeltaFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}
		if _, err := db.deltaFd.ReadAt(header, offset); err != nil {
			return fmt.Errorf("read delta header at %d: %w", offset, err)
		}
		size := int64(binary.LittleEndian.Uint32(header[4:8]))
		if header[0] != 'B' || header[1] != 'D' || header[2] != 'L' || header[3] != 'T' ||
			size < deltaRecordHeaderSize || size > maxDeltaRecordSize {
			if err := db.truncateDeltaFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}
		if size > remaining {
			if err := db.truncateDeltaFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}
		record := make([]byte, size)
		copy(record, header)
		if _, err := db.deltaFd.ReadAt(record[deltaRecordHeaderSize:], offset+deltaRecordHeaderSize); err != nil {
			return fmt.Errorf("read delta record at %d: %w", offset, err)
		}
		key, baseOffset, _, err := decodeDeltaRecord(record)
		if err != nil {
			offset += size
			continue
		}
		idx.put(key, deltaEntry{offset: offset, size: size, baseOffset: baseOffset})
		offset += size
	}
	db.delta = idx
	db.deltaOffset.Store(fileSize)
	db.deltaRecords.Store(idx.count())
	return nil
}

func (db *DB) truncateDeltaFile(size int64) error {
	if err := db.deltaFd.Truncate(size); err != nil {
		return fmt.Errorf("truncate deltas.dat to %d: %w", size, err)
	}
	db.deltaOffset.Store(size)
	return nil
}

func encodeDeltaRecord(key chunkKey, baseOffset int64, deltas []BlockDelta) ([]byte, error) {
	if len(deltas) > 65535 {
		return nil, fmt.Errorf("too many block deltas: %d", len(deltas))
	}
	size := deltaRecordHeaderSize + len(deltas)*deltaRecordEntrySize
	if size > maxDeltaRecordSize {
		return nil, fmt.Errorf("delta record too large: %d", size)
	}

	buf := make([]byte, size)
	copy(buf[:4], "BDLT")
	binary.LittleEndian.PutUint32(buf[4:8], uint32(size))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(key.x))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(key.z))
	binary.LittleEndian.PutUint32(buf[20:24], uint32(key.dimID))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(baseOffset))
	binary.LittleEndian.PutUint16(buf[32:34], uint16(len(deltas)))

	off := deltaRecordHeaderSize
	for _, delta := range deltas {
		buf[off] = byte(delta.Pos.X() & 15)
		binary.LittleEndian.PutUint16(buf[off+1:off+3], uint16(int16(delta.Pos.Y())))
		buf[off+3] = byte(delta.Pos.Z() & 15)
		buf[off+4] = delta.Layer
		binary.LittleEndian.PutUint32(buf[off+5:off+9], delta.RuntimeID)
		off += deltaRecordEntrySize
	}
	binary.LittleEndian.PutUint32(buf[8:12], computeCRC32(buf[12:]))
	return buf, nil
}

func decodeDeltaRecord(data []byte) (chunkKey, int64, []BlockDelta, error) {
	if len(data) < deltaRecordHeaderSize || string(data[:4]) != "BDLT" {
		return chunkKey{}, 0, nil, os.ErrInvalid
	}
	size := int(binary.LittleEndian.Uint32(data[4:8]))
	if size != len(data) || size > maxDeltaRecordSize {
		return chunkKey{}, 0, nil, os.ErrInvalid
	}
	if stored, computed := binary.LittleEndian.Uint32(data[8:12]), computeCRC32(data[12:]); stored != computed {
		return chunkKey{}, 0, nil, os.ErrInvalid
	}

	key := chunkKey{
		x:     int32(binary.LittleEndian.Uint32(data[12:16])),
		z:     int32(binary.LittleEndian.Uint32(data[16:20])),
		dimID: int32(binary.LittleEndian.Uint32(data[20:24])),
	}
	baseOffset := int64(binary.LittleEndian.Uint64(data[24:32]))
	count := int(binary.LittleEndian.Uint16(data[32:34]))
	if deltaRecordHeaderSize+count*deltaRecordEntrySize != len(data) {
		return chunkKey{}, 0, nil, os.ErrInvalid
	}

	deltas := make([]BlockDelta, count)
	off := deltaRecordHeaderSize
	for i := range deltas {
		y := int16(binary.LittleEndian.Uint16(data[off+1 : off+3]))
		deltas[i] = BlockDelta{
			Pos:       cube.Pos{int(data[off]), int(y), int(data[off+3])},
			Layer:     data[off+4],
			RuntimeID: binary.LittleEndian.Uint32(data[off+5 : off+9]),
		}
		off += deltaRecordEntrySize
	}
	return key, baseOffset, deltas, nil
}

func applyBlockDeltasToColumn(col *chunk.Column, deltas []BlockDelta) error {
	if col == nil || col.Chunk == nil {
		return errors.New("apply block deltas: nil chunk column")
	}
	r := col.Chunk.Range()
	for _, delta := range deltas {
		if delta.Pos.Y() < r[0] || delta.Pos.Y() > r[1] {
			return fmt.Errorf("apply block delta: y %d out of range [%d,%d]", delta.Pos.Y(), r[0], r[1])
		}
		col.Chunk.SetBlock(uint8(delta.Pos.X()&15), int16(delta.Pos.Y()), uint8(delta.Pos.Z()&15), delta.Layer, delta.RuntimeID)
	}
	return nil
}
