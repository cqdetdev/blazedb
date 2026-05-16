package blazedb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
	"github.com/df-mc/dragonfly/server/world/mcdb/leveldat"
	"github.com/df-mc/goleveldb/leveldb"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/nbt"
)

// ErrNotFound is returned when a chunk is not found in the database.
var ErrNotFound = leveldb.ErrNotFound

// DB implements a high-performance world provider for Dragonfly,
// optimized for Minecraft Bedrock worlds using Z-order spatial indexing
// and Minecraft-specific compression.
type DB struct {
	conf    Config
	dir     string
	ldat    *leveldat.Data
	set     *world.Settings
	cache   *chunkCache
	misses  *negativeCache
	index   *spatialIndex
	dataFd  *os.File
	delta   *deltaIndex
	deltaFd *os.File
	mu      sync.RWMutex

	// Atomic offset tracking - avoids Stat() syscall on every write
	fileOffset  atomic.Int64
	deltaOffset atomic.Int64

	// Write buffer for batched writes - stores raw columns for lazy encoding
	writeBuf     map[chunkKey]*chunk.Column
	flushingBuf  map[chunkKey]*chunk.Column
	writeBufMu   sync.Mutex
	flushMu      sync.Mutex
	writeBufSize atomic.Int64 // Estimated buffer size tracking
	flushTimer   *time.Timer
	stopFlush    chan struct{}

	// Async write queue for non-blocking writes
	writeQueue chan writeRequest
	writeWg    sync.WaitGroup
	stopWrite  chan struct{}

	// Predictive prefetcher for chunk loading
	prefetcher *Prefetcher

	writeSkips   atomic.Int64
	deltaRecords atomic.Int64

	// Player spawn positions (stored in-memory, persisted on close)
	playerSpawns   map[uuid.UUID]cube.Pos
	playerSpawnsMu sync.RWMutex
}

// writeRequest represents an async write operation
type writeRequest struct {
	key chunkKey
	col *chunk.Column
}

// pendingWrite is a buffered column prepared for sequential disk append.
type pendingWrite struct {
	key      chunkKey
	sortKey  uint64
	data     []byte
	dataSize int64
	skipped  bool
}

type pendingEncode struct {
	key     chunkKey
	sortKey uint64
	col     *chunk.Column
}

const (
	chunkRecordHeaderSize = 26
	maxChunkRecordSize    = 256 * 1024 * 1024
)

// Open creates a new provider reading and writing from/to files under the path
// passed using default options.
func Open(dir string) (*DB, error) {
	var conf Config
	return conf.Open(dir)
}

// newDB creates a new DB instance.
func newDB(conf Config, dir string) (*DB, error) {
	db := &DB{
		conf:         conf,
		dir:          dir,
		ldat:         &leveldat.Data{},
		playerSpawns: make(map[uuid.UUID]cube.Pos),
		writeBuf:     make(map[chunkKey]*chunk.Column),
		flushingBuf:  make(map[chunkKey]*chunk.Column),
		stopFlush:    make(chan struct{}),
	}

	// Initialize cache
	db.cache = newChunkCache(conf.Options.CacheSize)
	db.misses = newNegativeCache()

	// Initialize or load spatial index
	db.index = newSpatialIndex()

	// Load or create level.dat
	ldatPath := filepath.Join(dir, "level.dat")
	if fileExists(ldatPath) {
		ldat, err := leveldat.ReadFile(ldatPath)
		if err != nil {
			return nil, fmt.Errorf("open db: read level.dat: %w", err)
		}
		ver := ldat.Ver()
		if ver != leveldat.Version && ver >= 10 {
			return nil, fmt.Errorf("open db: level.dat version %v is unsupported", ver)
		}
		if err = ldat.Unmarshal(db.ldat); err != nil {
			return nil, fmt.Errorf("open db: unmarshal level.dat: %w", err)
		}
	} else {
		db.ldat.FillDefault()
	}
	db.set = db.ldat.Settings()

	// Open or create the chunks data file
	dataFile := dataPath(dir)
	var err error
	db.dataFd, err = os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("open db: open chunks.dat: %w", err)
	}

	// Initialize atomic file offset from current file size (avoids Stat() on every write)
	if stat, err := db.dataFd.Stat(); err == nil {
		db.fileOffset.Store(stat.Size())
	}

	if conf.Options.EnableDeltaRecords {
		db.delta = newDeltaIndex()
		db.deltaFd, err = os.OpenFile(deltaPath(dir), os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = db.dataFd.Close()
			return nil, fmt.Errorf("open db: open deltas.dat: %w", err)
		}
		if stat, err := db.deltaFd.Stat(); err == nil {
			db.deltaOffset.Store(stat.Size())
		}
		if err := db.rebuildDeltaIndex(); err != nil {
			_ = db.deltaFd.Close()
			_ = db.dataFd.Close()
			return nil, fmt.Errorf("open db: rebuild delta index: %w", err)
		}
	}

	// Load or rebuild the index. Rebuilds only happen on open when index.dat
	// is missing/corrupt, so the normal read/write paths remain unchanged.
	indexFile := indexPath(dir)
	if fileExists(indexFile) {
		if err := db.index.load(indexFile); err != nil {
			conf.Options.Log.Warn("failed to load index, rebuilding", "error", err)
			if err := db.rebuildIndex(); err != nil {
				return nil, fmt.Errorf("open db: rebuild index: %w", err)
			}
			if err := db.saveIndex(); err != nil {
				return nil, fmt.Errorf("open db: save rebuilt index: %w", err)
			}
		} else if db.index.needsRebuild(db.fileOffset.Load()) {
			conf.Options.Log.Warn("index stale, rebuilding", "path", indexFile)
			if err := db.rebuildIndex(); err != nil {
				return nil, fmt.Errorf("open db: rebuild stale index: %w", err)
			}
			if err := db.saveIndex(); err != nil {
				return nil, fmt.Errorf("open db: save rebuilt stale index: %w", err)
			}
		}
	} else if db.fileOffset.Load() > 0 {
		conf.Options.Log.Warn("index missing, rebuilding", "path", indexFile)
		if err := db.rebuildIndex(); err != nil {
			return nil, fmt.Errorf("open db: rebuild index: %w", err)
		}
		if err := db.saveIndex(); err != nil {
			return nil, fmt.Errorf("open db: save rebuilt index: %w", err)
		}
	}

	// Load player spawns if metadata exists
	if err := db.loadMetadata(); err != nil && !os.IsNotExist(err) {
		conf.Options.Log.Warn("failed to load metadata", "error", err)
	}

	// Start periodic flush if write buffering is enabled
	if conf.Options.FlushInterval > 0 && conf.Options.WriteBufferSize > 0 {
		db.startPeriodicFlush()
	}

	// Start async write worker
	db.writeQueue = make(chan writeRequest, 1024) // Buffered for non-blocking
	db.stopWrite = make(chan struct{})
	db.startWriteWorker()

	// Start prefetcher with 2 workers
	db.prefetcher = NewPrefetcher(db, 2)

	return db, nil
}

// startPeriodicFlush starts the periodic flush goroutine.
func (db *DB) startPeriodicFlush() {
	interval := time.Duration(db.conf.Options.FlushInterval) * time.Millisecond
	db.flushTimer = time.NewTimer(interval)

	go func() {
		for {
			select {
			case <-db.flushTimer.C:
				if err := db.flushWriteBuffer(); err != nil {
					db.conf.Options.Log.Error("periodic flush", "error", err)
				}
				db.flushTimer.Reset(interval)
			case <-db.stopFlush:
				db.flushTimer.Stop()
				return
			}
		}
	}()
}

// startWriteWorker starts the async background write worker.
func (db *DB) startWriteWorker() {
	db.writeWg.Add(1)
	go func() {
		defer db.writeWg.Done()
		for {
			select {
			case req := <-db.writeQueue:
				// Process write request - add to buffer
				bufSize := db.bufferWrite(req.key, req.col)

				// Flush if buffer exceeds size limit
				if bufSize >= db.conf.Options.WriteBufferSize {
					if err := db.flushWriteBuffer(); err != nil {
						db.conf.Options.Log.Error("write buffer flush", "error", err)
					}
				}
			case <-db.stopWrite:
				// Drain remaining writes before stopping
				for {
					select {
					case req := <-db.writeQueue:
						db.bufferWrite(req.key, req.col)
					default:
						if err := db.flushWriteBuffer(); err != nil {
							db.conf.Options.Log.Error("final write buffer flush", "error", err)
						}
						return
					}
				}
			}
		}
	}()
}

// flushWriteBuffer writes all buffered chunks to disk with lazy encoding.
func (db *DB) flushWriteBuffer() error {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	db.writeBufMu.Lock()
	if len(db.writeBuf) == 0 {
		db.writeBufMu.Unlock()
		return nil
	}

	// Copy buffer and clear
	toWrite := db.writeBuf
	for key, col := range toWrite {
		db.flushingBuf[key] = col
	}
	db.writeBuf = make(map[chunkKey]*chunk.Column)
	db.writeBufSize.Store(0) // Reset atomic buffer size
	db.writeBufMu.Unlock()
	defer db.clearFlushingBuffer(toWrite)

	writes := db.encodePendingWrites(toWrite)
	if len(writes) != len(toWrite) {
		db.requeueUnencodedWrites(toWrite, writes)
	}
	if len(writes) == 0 {
		return fmt.Errorf("flush: no chunks encoded")
	}

	totalSize := 0
	persisted := 0
	for _, write := range writes {
		if write.skipped {
			continue
		}
		totalSize += len(write.data)
		persisted++
	}
	if persisted == 0 {
		if len(writes) != len(toWrite) {
			return fmt.Errorf("flush: no chunks encoded")
		}
		return nil
	}

	sort.Slice(writes, func(i, j int) bool {
		return writes[i].sortKey < writes[j].sortKey
	})

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.fileOffset.Load()
	batch := make([]byte, 0, totalSize)
	updates := make([]indexUpdate, 0, len(writes))
	for _, write := range writes {
		if write.skipped {
			continue
		}
		writeOffset := offset + int64(len(batch))
		batch = append(batch, write.data...)
		updates = append(updates, indexUpdate{
			key:    write.key,
			offset: writeOffset,
			size:   write.dataSize,
		})
	}

	if _, err := db.dataFd.WriteAt(batch, offset); err != nil {
		db.requeueWrites(toWrite)
		if truncateErr := db.truncateDataFile(offset); truncateErr != nil {
			return fmt.Errorf("flush: write chunk batch: %w; additionally failed to truncate failed batch: %v", err, truncateErr)
		}
		return fmt.Errorf("flush: write chunk batch: %w", err)
	}
	if err := db.syncDataFile("flush"); err != nil {
		db.requeueWrites(toWrite)
		if truncateErr := db.truncateDataFile(offset); truncateErr != nil {
			return fmt.Errorf("%w; additionally failed to truncate unsynced batch: %v", err, truncateErr)
		}
		return err
	}

	offset += int64(len(batch))
	db.fileOffset.Store(offset)
	db.index.putBatch(updates)
	for _, write := range writes {
		if !write.skipped {
			db.clearDeltasForFullWrite(write.key)
			db.cache.putRecord(write.key, write.data)
		}
	}
	return nil
}

func (db *DB) bufferWrite(key chunkKey, col *chunk.Column) int64 {
	db.writeBufMu.Lock()
	defer db.writeBufMu.Unlock()

	estimatedSize := int64(len(col.Chunk.Sub()) * 4096)
	if _, exists := db.writeBuf[key]; !exists {
		db.writeBufSize.Add(estimatedSize)
	}
	db.writeBuf[key] = col
	return db.writeBufSize.Load()
}

func (db *DB) requeueUnencodedWrites(toWrite map[chunkKey]*chunk.Column, writes []pendingWrite) {
	encoded := make(map[chunkKey]struct{}, len(writes))
	for _, write := range writes {
		encoded[write.key] = struct{}{}
	}
	failed := make(map[chunkKey]*chunk.Column, len(toWrite)-len(encoded))
	for key, col := range toWrite {
		if _, ok := encoded[key]; !ok {
			failed[key] = col
		}
	}
	db.requeueWrites(failed)
}

func (db *DB) requeueWrites(toWrite map[chunkKey]*chunk.Column) {
	db.writeBufMu.Lock()
	defer db.writeBufMu.Unlock()

	for key, col := range toWrite {
		if _, exists := db.writeBuf[key]; exists {
			continue
		}
		estimatedSize := int64(len(col.Chunk.Sub()) * 4096)
		db.writeBufSize.Add(estimatedSize)
		db.writeBuf[key] = col
	}
}

func (db *DB) encodePendingWrites(toWrite map[chunkKey]*chunk.Column) []pendingWrite {
	tasks := make([]pendingEncode, 0, len(toWrite))
	for key, col := range toWrite {
		sortKey, _ := indexLookupKey(key)
		tasks = append(tasks, pendingEncode{key: key, sortKey: sortKey, col: col})
	}
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].sortKey < tasks[j].sortKey
	})

	if !db.conf.Options.ParallelCompression || len(tasks) < 32 {
		return db.encodePendingWritesSequential(tasks)
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(tasks) {
		workers = len(tasks)
	}
	if workers < 2 {
		return db.encodePendingWritesSequential(tasks)
	}

	writes := make([]pendingWrite, len(tasks))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				task := tasks[idx]
				data, err := db.encodeColumnForKey(task.key, task.col)
				if err != nil {
					db.conf.Options.Log.Error("flush: encode chunk", "error", err)
					continue
				}
				if db.isUnchangedRecord(task.key, data) {
					db.writeSkips.Add(1)
					writes[idx] = pendingWrite{
						key:     task.key,
						sortKey: task.sortKey,
						skipped: true,
					}
					continue
				}
				writes[idx] = pendingWrite{
					key:      task.key,
					sortKey:  task.sortKey,
					data:     data,
					dataSize: int64(len(data)),
				}
			}
		}()
	}
	for i := range tasks {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	n := 0
	for _, write := range writes {
		if write.data != nil || write.skipped {
			writes[n] = write
			n++
		}
	}
	return writes[:n]
}

func (db *DB) encodePendingWritesSequential(tasks []pendingEncode) []pendingWrite {
	writes := make([]pendingWrite, 0, len(tasks))
	for _, task := range tasks {
		data, err := db.encodeColumnForKey(task.key, task.col)
		if err != nil {
			db.conf.Options.Log.Error("flush: encode chunk", "error", err)
			continue
		}
		if db.isUnchangedRecord(task.key, data) {
			db.writeSkips.Add(1)
			writes = append(writes, pendingWrite{
				key:     task.key,
				sortKey: task.sortKey,
				skipped: true,
			})
			continue
		}
		writes = append(writes, pendingWrite{
			key:      task.key,
			sortKey:  task.sortKey,
			data:     data,
			dataSize: int64(len(data)),
		})
	}
	return writes
}

func (db *DB) clearFlushingBuffer(flushed map[chunkKey]*chunk.Column) {
	db.writeBufMu.Lock()
	for key, col := range flushed {
		if db.flushingBuf[key] == col {
			delete(db.flushingBuf, key)
		}
	}
	db.writeBufMu.Unlock()
}

// Settings returns the world.Settings of the world loaded by the DB.
func (db *DB) Settings() *world.Settings {
	return db.set
}

// SaveSettings saves the world.Settings passed to the level.dat.
func (db *DB) SaveSettings(s *world.Settings) {
	db.ldat.PutSettings(s)
}

// LoadPlayerSpawnPosition loads the player spawn point if found.
func (db *DB) LoadPlayerSpawnPosition(id uuid.UUID) (pos cube.Pos, exists bool, err error) {
	db.playerSpawnsMu.RLock()
	defer db.playerSpawnsMu.RUnlock()

	pos, exists = db.playerSpawns[id]
	return pos, exists, nil
}

// SavePlayerSpawnPosition saves the player spawn point.
func (db *DB) SavePlayerSpawnPosition(id uuid.UUID, pos cube.Pos) error {
	db.playerSpawnsMu.Lock()
	defer db.playerSpawnsMu.Unlock()

	db.playerSpawns[id] = pos
	return nil
}

// LoadColumn reads a chunk.Column from the DB at a position and dimension.
// If no column at that position exists, errors.Is(err, ErrNotFound) equals true.
func (db *DB) LoadColumn(pos world.ChunkPos, dim world.Dimension) (*chunk.Column, error) {
	key := newChunkKey(pos, dim)

	// Check cache first (no lock needed - cache is thread-safe)
	if col := db.cache.get(key); col != nil {
		return col, nil
	}

	return db.loadColumnAfterCacheMiss(key, dim)
}

// loadColumnAfterCacheMiss loads a column when the caller has already checked
// the chunk cache. This keeps batched area loads from double-counting misses.
func (db *DB) loadColumnAfterCacheMiss(key chunkKey, dim world.Dimension) (*chunk.Column, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check write buffer - return directly if found (no need to decode)
	db.writeBufMu.Lock()
	if col, ok := db.writeBuf[key]; ok {
		db.writeBufMu.Unlock()
		db.cache.putClean(key, col)
		return col, nil
	}
	if col, ok := db.flushingBuf[key]; ok {
		db.writeBufMu.Unlock()
		db.cache.putClean(key, col)
		return col, nil
	}
	db.writeBufMu.Unlock()

	// Load from disk using spatial index
	offset, size, exists := db.index.get(key)
	if !exists {
		db.misses.put(key)
		return nil, ErrNotFound
	}

	// Read compressed data from disk
	data := make([]byte, size)
	if _, err := db.dataFd.ReadAt(data, offset); err != nil {
		return nil, fmt.Errorf("read chunk data: %w", err)
	}

	// Decompress and decode
	col, err := db.decodeColumn(data, dim)
	if err != nil {
		return nil, fmt.Errorf("decode column: %w", err)
	}
	if err := db.applyIndexedDeltas(key, offset, col); err != nil {
		return nil, err
	}

	// Add to cache with the exact encoded record so unchanged future saves can
	// skip append/fsync after byte-for-byte comparison.
	db.cache.putCleanWithRecord(key, col, data)

	return col, nil
}

// StoreColumn stores a chunk.Column at a position and dimension in the DB.
func (db *DB) StoreColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) error {
	key := newChunkKey(pos, dim)

	// Update cache immediately
	db.misses.delete(key)
	db.cache.put(key, col)

	if db.conf.Options.Durability == DurabilitySafeBatch {
		db.bufferWrite(key, col)
		return db.flushWriteBuffer()
	}

	// If write buffering is enabled, send to async write queue (non-blocking)
	if db.conf.Options.WriteBufferSize > 0 {
		select {
		case db.writeQueue <- writeRequest{key: key, col: col}:
			// Successfully queued
		default:
			// Queue full - add directly to buffer (fallback)
			db.bufferWrite(key, col)
		}
		return nil
	}

	// Direct write (no buffering) - encode now
	data, err := db.encodeColumn(pos, dim, col)
	if err != nil {
		return fmt.Errorf("encode column: %w", err)
	}
	if db.isUnchangedRecord(key, data) {
		db.writeSkips.Add(1)
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	offset := db.fileOffset.Load()

	if _, err := db.dataFd.WriteAt(data, offset); err != nil {
		return fmt.Errorf("write chunk data: %w", err)
	}

	// Atomically update file offset
	db.fileOffset.Add(int64(len(data)))

	db.index.put(key, offset, int64(len(data)))
	if err := db.syncDataFile("store chunk"); err != nil {
		return err
	}
	db.clearDeltasForFullWrite(key)
	db.cache.putRecord(key, data)

	return nil
}

func (db *DB) isUnchangedRecord(key chunkKey, data []byte) bool {
	record := db.cache.record(key)
	if len(record) == 0 || len(record) != len(data) {
		return false
	}
	return bytes.Equal(record, data)
}

// LoadArea loads multiple chunks in the given radius around center position.
// This is optimized for fast render distance loading.
func (db *DB) LoadArea(center world.ChunkPos, radius int, dim world.Dimension) ([]*chunk.Column, error) {
	total := (radius*2 + 1) * (radius*2 + 1)
	columns := make([]*chunk.Column, 0, total)
	missing := make([]chunkKey, 0)

	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			key := newChunkKey(world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}, dim)
			if col := db.cache.get(key); col != nil {
				columns = append(columns, col)
				continue
			}
			if db.misses.has(key) {
				continue
			}
			missing = append(missing, key)
		}
	}

	if len(missing) == 0 {
		return columns, nil
	}
	sort.Slice(missing, func(i, j int) bool {
		left, _ := indexLookupKey(missing[i])
		right, _ := indexLookupKey(missing[j])
		return left < right
	})

	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	workers := runtime.GOMAXPROCS(0)
	if workers > len(missing) {
		workers = len(missing)
	}
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan chunkKey)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				col, err := db.loadColumnAfterCacheMiss(key, dim)
				if err != nil {
					if !errors.Is(err, ErrNotFound) {
						select {
						case errCh <- err:
						default:
						}
					}
					continue
				}
				mu.Lock()
				columns = append(columns, col)
				mu.Unlock()
			}
		}()
	}
	for _, key := range missing {
		jobs <- key
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return nil, err
	}

	return columns, nil
}

// encodeColumn encodes a chunk column to compressed bytes.
func (db *DB) encodeColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) ([]byte, error) {
	dimID, _ := world.DimensionID(dim)
	return db.encodeColumnWithDimID(pos, int32(dimID), col)
}

func (db *DB) encodeColumnForKey(key chunkKey, col *chunk.Column) ([]byte, error) {
	return db.encodeColumnWithDimID(key.pos(), key.dimID, col)
}

func (db *DB) encodeColumnWithDimID(pos world.ChunkPos, dimID int32, col *chunk.Column) ([]byte, error) {
	// Encode chunk data
	data := chunk.Encode(col.Chunk, chunk.DiskEncoding)

	capacity := chunkRecordHeaderSize + 4 + compressedBound(len(data.Biomes), db.conf.Options.Compression)
	for _, sub := range data.SubChunks {
		capacity += 4 + compressedBound(len(sub), db.conf.Options.Compression)
	}
	capacity += 12
	buf := make([]byte, 0, capacity)

	// Write header: "BLAZ" magic
	buf = append(buf, 'B', 'L', 'A', 'Z')

	// Placeholder for size (will be filled after compression)
	sizePos := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Placeholder for CRC32
	crcPos := len(buf)
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	// Write coordinates
	buf = binary.LittleEndian.AppendUint32(buf, uint32(pos[0]))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(pos[1]))

	// Write dimension ID
	buf = binary.LittleEndian.AppendUint32(buf, uint32(dimID))

	// Write compression type
	buf = append(buf, byte(db.conf.Options.Compression))

	// Write subchunk count
	buf = append(buf, byte(len(data.SubChunks)))

	appendBytes := func(payload []byte) {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(payload)))
		buf = append(buf, payload...)
	}

	// Compress and write biomes
	buf = appendCompressed(buf, data.Biomes, db.conf.Options.Compression)

	// Compress and write each subchunk
	for _, sub := range data.SubChunks {
		buf = appendCompressed(buf, sub, db.conf.Options.Compression)
	}

	// Encode entities
	entitiesData := db.encodeEntities(col.Entities)
	appendBytes(entitiesData)

	// Encode block entities
	blockEntitiesData := db.encodeBlockEntities(col.BlockEntities)
	appendBytes(blockEntitiesData)

	// Encode scheduled updates
	scheduledData := db.encodeScheduledUpdates(col.Tick, col.ScheduledBlocks)
	appendBytes(scheduledData)

	// Calculate and write size
	result := buf
	binary.LittleEndian.PutUint32(result[sizePos:], uint32(len(result)))

	// Calculate and write CRC32
	crc := computeCRC32(result[crcPos+4:])
	binary.LittleEndian.PutUint32(result[crcPos:], crc)

	return result, nil
}

// decodeColumn decodes a compressed chunk column.
func (db *DB) decodeColumn(data []byte, dim world.Dimension) (*chunk.Column, error) {
	if len(data) < 26 || data[0] != 'B' || data[1] != 'L' || data[2] != 'A' || data[3] != 'Z' {
		return nil, errors.New("invalid chunk header")
	}

	// Read and verify size
	size := binary.LittleEndian.Uint32(data[4:8])
	if size > uint32(len(data)) {
		return nil, errors.New("invalid chunk size")
	}

	// Read CRC32
	storedCRC := binary.LittleEndian.Uint32(data[8:12])

	// Verify checksum if enabled
	if db.conf.Options.VerifyChecksums {
		computedCRC := computeCRC32(data[12:])
		if storedCRC != computedCRC {
			return nil, errors.New("CRC32 checksum mismatch")
		}
	}

	off := 24 // magic + size + crc + x + z + dimension id
	compression := CompressionType(data[off])
	off++
	subCount := int(data[off])
	off++

	readBytes := func() ([]byte, error) {
		if len(data)-off < 4 {
			return nil, errors.New("truncated chunk data")
		}
		n := int(binary.LittleEndian.Uint32(data[off:]))
		off += 4
		if n < 0 || n > len(data)-off {
			return nil, errors.New("truncated chunk data")
		}
		v := data[off : off+n]
		off += n
		return v, nil
	}

	// Read and decompress biomes
	compressedBiomes, err := readBytes()
	if err != nil {
		return nil, err
	}
	biomes := decompress(compressedBiomes, compression)

	// Read all compressed subchunk data FIRST (single sequential read)
	compressedSubChunks := make([][]byte, subCount)
	totalCompressedSubChunkBytes := 0
	for i := range compressedSubChunks {
		compressedSubChunks[i], err = readBytes()
		if err != nil {
			return nil, err
		}
		totalCompressedSubChunkBytes += len(compressedSubChunks[i])
	}

	// Decompress subchunks in PARALLEL
	subChunks := make([][]byte, subCount)
	if db.conf.Options.ParallelCompression && subCount >= 8 && totalCompressedSubChunkBytes >= 512*1024 {
		// Parallel decompression only pays off for unusually large chunk payloads.
		var wg sync.WaitGroup
		wg.Add(int(subCount))
		for i := range compressedSubChunks {
			go func(idx int) {
				defer wg.Done()
				subChunks[idx] = decompress(compressedSubChunks[idx], compression)
			}(i)
		}
		wg.Wait()
	} else {
		// Sequential for small chunks (goroutine overhead not worth it)
		for i := range compressedSubChunks {
			subChunks[i] = decompress(compressedSubChunks[i], compression)
		}
	}

	// Decode chunk
	cdata := chunk.SerialisedData{
		SubChunks: subChunks,
		Biomes:    biomes,
	}
	c, err := chunk.DiskDecode(cdata, dim.Range())
	if err != nil {
		return nil, fmt.Errorf("decode chunk: %w", err)
	}

	col := &chunk.Column{Chunk: c}

	// Read entities
	entitiesData, err := readBytes()
	if err != nil {
		return nil, err
	}
	if len(entitiesData) > 0 {
		col.Entities = db.decodeEntities(entitiesData)
	}

	// Read block entities
	blockEntitiesData, err := readBytes()
	if err != nil {
		return nil, err
	}
	if len(blockEntitiesData) > 0 {
		col.BlockEntities = db.decodeBlockEntities(blockEntitiesData)
	}

	// Read scheduled updates
	scheduledData, err := readBytes()
	if err != nil {
		return nil, err
	}
	if len(scheduledData) > 0 {
		col.ScheduledBlocks, col.Tick = db.decodeScheduledUpdates(scheduledData)
	}

	return col, nil
}

// encodeEntities encodes entities to NBT bytes.
func (db *DB) encodeEntities(entities []chunk.Entity) []byte {
	if len(entities) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(nil)
	enc := nbt.NewEncoderWithEncoding(buf, nbt.LittleEndian)
	for _, e := range entities {
		e.Data["UniqueID"] = e.ID
		enc.Encode(e.Data)
	}
	return buf.Bytes()
}

// decodeEntities decodes entities from NBT bytes.
func (db *DB) decodeEntities(data []byte) []chunk.Entity {
	var entities []chunk.Entity
	buf := bytes.NewBuffer(data)
	dec := nbt.NewDecoderWithEncoding(buf, nbt.LittleEndian)
	for buf.Len() > 0 {
		ent := chunk.Entity{Data: make(map[string]any)}
		if err := dec.Decode(&ent.Data); err != nil {
			break
		}
		if id, ok := ent.Data["UniqueID"].(int64); ok {
			ent.ID = id
		}
		entities = append(entities, ent)
	}
	return entities
}

// encodeBlockEntities encodes block entities to NBT bytes.
func (db *DB) encodeBlockEntities(blockEntities []chunk.BlockEntity) []byte {
	if len(blockEntities) == 0 {
		return nil
	}
	buf := bytes.NewBuffer(nil)
	enc := nbt.NewEncoderWithEncoding(buf, nbt.LittleEndian)
	for _, be := range blockEntities {
		be.Data["x"], be.Data["y"], be.Data["z"] = int32(be.Pos[0]), int32(be.Pos[1]), int32(be.Pos[2])
		enc.Encode(be.Data)
	}
	return buf.Bytes()
}

// decodeBlockEntities decodes block entities from NBT bytes.
func (db *DB) decodeBlockEntities(data []byte) []chunk.BlockEntity {
	var blockEntities []chunk.BlockEntity
	buf := bytes.NewBuffer(data)
	dec := nbt.NewDecoderWithEncoding(buf, nbt.LittleEndian)
	for buf.Len() > 0 {
		be := chunk.BlockEntity{Data: make(map[string]any)}
		if err := dec.Decode(&be.Data); err != nil {
			break
		}
		x, _ := be.Data["x"].(int32)
		y, _ := be.Data["y"].(int32)
		z, _ := be.Data["z"].(int32)
		be.Pos = cube.Pos{int(x), int(y), int(z)}
		blockEntities = append(blockEntities, be)
	}
	return blockEntities
}

type scheduledUpdates struct {
	CurrentTick int32            `nbt:"currentTick"`
	TickList    []map[string]any `nbt:"tickList"`
}

// encodeScheduledUpdates encodes scheduled updates to NBT bytes.
func (db *DB) encodeScheduledUpdates(tick int64, updates []chunk.ScheduledBlockUpdate) []byte {
	if len(updates) == 0 {
		return nil
	}
	list := make([]map[string]any, len(updates))
	for i, update := range updates {
		list[i] = map[string]any{
			"x": int32(update.Pos[0]), "y": int32(update.Pos[1]), "z": int32(update.Pos[2]),
			"time": update.Tick, "blockState": chunk.BlockPaletteEncoding.EncodeBlockState(update.Block),
		}
	}
	data, _ := nbt.MarshalEncoding(scheduledUpdates{CurrentTick: int32(tick), TickList: list}, nbt.LittleEndian)
	return data
}

// decodeScheduledUpdates decodes scheduled updates from NBT bytes.
func (db *DB) decodeScheduledUpdates(data []byte) ([]chunk.ScheduledBlockUpdate, int64) {
	var m scheduledUpdates
	if err := nbt.UnmarshalEncoding(data, &m, nbt.LittleEndian); err != nil {
		return nil, 0
	}
	updates := make([]chunk.ScheduledBlockUpdate, 0, len(m.TickList))
	for _, tick := range m.TickList {
		t, _ := tick["time"].(int64)
		bl, _ := tick["blockState"].(map[string]any)
		block, err := chunk.BlockPaletteEncoding.DecodeBlockState(bl)
		if err != nil {
			continue
		}
		x, _ := tick["x"].(int32)
		y, _ := tick["y"].(int32)
		z, _ := tick["z"].(int32)
		updates = append(updates, chunk.ScheduledBlockUpdate{
			Pos:   cube.Pos{int(x), int(y), int(z)},
			Block: block,
			Tick:  t,
		})
	}
	return updates, int64(m.CurrentTick)
}

// rebuildIndex rebuilds the spatial index from the data file.
func (db *DB) rebuildIndex() error {
	stat, err := db.dataFd.Stat()
	if err != nil {
		return fmt.Errorf("stat chunks.dat: %w", err)
	}

	idx := newSpatialIndex()
	fileSize := stat.Size()
	offset := int64(0)
	header := make([]byte, chunkRecordHeaderSize)

	for offset < fileSize {
		remaining := fileSize - offset
		if remaining < chunkRecordHeaderSize {
			db.conf.Options.Log.Warn("truncating partial chunk header", "offset", offset, "bytes", remaining)
			if err := db.truncateDataFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}

		if _, err := db.dataFd.ReadAt(header, offset); err != nil {
			return fmt.Errorf("read chunk header at %d: %w", offset, err)
		}
		if header[0] != 'B' || header[1] != 'L' || header[2] != 'A' || header[3] != 'Z' {
			db.conf.Options.Log.Warn("truncating invalid chunk header", "offset", offset)
			if err := db.truncateDataFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}

		size := int64(binary.LittleEndian.Uint32(header[4:8]))
		if size < chunkRecordHeaderSize || size > maxChunkRecordSize {
			db.conf.Options.Log.Warn("truncating invalid chunk size", "offset", offset, "size", size)
			if err := db.truncateDataFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}
		if size > remaining {
			db.conf.Options.Log.Warn("truncating partial chunk record", "offset", offset, "size", size, "remaining", remaining)
			if err := db.truncateDataFile(offset); err != nil {
				return err
			}
			fileSize = offset
			break
		}

		record := make([]byte, size)
		copy(record, header)
		if _, err := db.dataFd.ReadAt(record[chunkRecordHeaderSize:], offset+chunkRecordHeaderSize); err != nil {
			return fmt.Errorf("read chunk record at %d: %w", offset, err)
		}
		storedCRC := binary.LittleEndian.Uint32(record[8:12])
		if computedCRC := computeCRC32(record[12:]); storedCRC != computedCRC {
			db.conf.Options.Log.Warn("skipping corrupt chunk record", "offset", offset, "size", size)
			offset += size
			continue
		}

		dimID := int(int32(binary.LittleEndian.Uint32(record[20:24])))
		_, ok := world.DimensionByID(dimID)
		if !ok {
			db.conf.Options.Log.Warn("skipping chunk with unknown dimension", "offset", offset, "dimension", dimID)
			offset += size
			continue
		}
		key := chunkKey{
			x:     int32(binary.LittleEndian.Uint32(record[12:16])),
			z:     int32(binary.LittleEndian.Uint32(record[16:20])),
			dimID: int32(dimID),
		}
		morton, indexDimID := indexLookupKey(key)
		idx.entries[indexKey{morton: morton, dim: indexDimID}] = indexEntry{
			offset: offset,
			size:   size,
			dim:    indexDimID,
		}
		offset += size
	}

	db.index = idx
	db.fileOffset.Store(fileSize)
	db.conf.Options.Log.Info("rebuilt index", "entries", idx.count(), "bytes", fileSize)
	return nil
}

func (db *DB) truncateDataFile(size int64) error {
	if err := db.dataFd.Truncate(size); err != nil {
		return fmt.Errorf("truncate chunks.dat to %d: %w", size, err)
	}
	db.fileOffset.Store(size)
	return nil
}

func (db *DB) syncDataFile(context string) error {
	if db.conf.Options.Durability < DurabilityBalanced {
		return nil
	}
	if err := db.dataFd.Sync(); err != nil {
		return fmt.Errorf("%s: sync chunks.dat: %w", context, err)
	}
	return nil
}

func (db *DB) saveIndex() error {
	return db.index.saveWithDataSize(
		indexPath(db.dir),
		db.fileOffset.Load(),
		db.conf.Options.Durability >= DurabilityBalanced,
	)
}

// metadata holds persisted metadata for the database.
type metadata struct {
	PlayerSpawns map[string][3]int `json:"player_spawns"`
}

// loadMetadata loads metadata from disk.
func (db *DB) loadMetadata() error {
	data, err := os.ReadFile(metadataPath(db.dir))
	if err != nil {
		return err
	}
	var m metadata
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	for idStr, pos := range m.PlayerSpawns {
		id, err := uuid.Parse(idStr)
		if err != nil {
			continue
		}
		db.playerSpawns[id] = cube.Pos{pos[0], pos[1], pos[2]}
	}
	return nil
}

// saveMetadata saves metadata to disk.
func (db *DB) saveMetadata() error {
	m := metadata{
		PlayerSpawns: make(map[string][3]int),
	}
	db.playerSpawnsMu.RLock()
	for id, pos := range db.playerSpawns {
		m.PlayerSpawns[id.String()] = [3]int{pos.X(), pos.Y(), pos.Z()}
	}
	db.playerSpawnsMu.RUnlock()

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metadataPath(db.dir), data, 0666)
}

// NewColumnIterator returns a ColumnIterator for iterating over all chunks.
func (db *DB) NewColumnIterator(r *IteratorRange) *ColumnIterator {
	if r == nil {
		r = &IteratorRange{}
	}
	return newColumnIterator(db, r)
}

// GetStats returns performance statistics for the database.
func (db *DB) GetStats() *Stats {
	stats := db.cache.stats()
	stats.ChunkWriteSkips = db.writeSkips.Load()
	stats.PinnedChunks = int64(db.cache.pinnedCount())
	stats.DeltaRecords = db.deltaRecords.Load()
	return stats
}

// Prefetcher returns the prefetcher for registering player positions.
func (db *DB) Prefetcher() *Prefetcher {
	return db.prefetcher
}

// PinColumn loads a column into the cache and prevents it from being evicted
// until UnpinColumn is called. This is useful for fixed arenas, spawns, KOTH
// regions, and other hot areas known ahead of time.
func (db *DB) PinColumn(pos world.ChunkPos, dim world.Dimension) error {
	key := newChunkKey(pos, dim)
	if _, err := db.LoadColumn(pos, dim); err != nil {
		return err
	}
	db.cache.pin(key)
	return nil
}

// UnpinColumn allows a previously pinned column to be evicted again.
func (db *DB) UnpinColumn(pos world.ChunkPos, dim world.Dimension) {
	db.cache.unpin(newChunkKey(pos, dim))
}

// PinArea pins all existing columns inside radius around center. Missing
// columns are skipped and non-not-found errors stop the operation.
func (db *DB) PinArea(center world.ChunkPos, radius int, dim world.Dimension) (int, error) {
	pinned := 0
	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			pos := world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}
			if err := db.PinColumn(pos, dim); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return pinned, err
			}
			pinned++
		}
	}
	return pinned, nil
}

// UnpinArea allows all columns inside radius around center to be evicted again.
func (db *DB) UnpinArea(center world.ChunkPos, radius int, dim world.Dimension) {
	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			db.UnpinColumn(world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}, dim)
		}
	}
}

// Compact performs background compaction to optimize storage.
func (db *DB) Compact() error {
	if err := db.flushWriteBuffer(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entries := db.index.snapshotSorted()
	tmpPath := filepath.Join(db.dir, "chunks.dat.compact")
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("compact: create temp data file: %w", err)
	}

	offset := int64(0)
	compacted := make([]indexSnapshotEntry, 0, len(entries))
	for _, snapshot := range entries {
		if snapshot.entry.size <= 0 {
			continue
		}
		key := chunkKey{dimID: snapshot.key.dim}
		key.x, key.z = mortonDecode(snapshot.key.morton)

		size := snapshot.entry.size
		if db.delta != nil {
			if deltas := db.delta.getForBase(key, snapshot.entry.offset); len(deltas) > 0 {
				dim, ok := world.DimensionByID(int(key.dimID))
				if !ok {
					continue
				}
				record := make([]byte, snapshot.entry.size)
				if _, err := db.dataFd.ReadAt(record, snapshot.entry.offset); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: read chunk with deltas: %w", err)
				}
				col, err := db.decodeColumn(record, dim)
				if err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: decode chunk with deltas: %w", err)
				}
				if err := db.applyIndexedDeltas(key, snapshot.entry.offset, col); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: apply deltas: %w", err)
				}
				record, err = db.encodeColumnForKey(key, col)
				if err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: encode chunk with deltas: %w", err)
				}
				if _, err := tmp.Write(record); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: write chunk with deltas: %w", err)
				}
				size = int64(len(record))
				snapshot.entry.size = size
			} else {
				reader := io.NewSectionReader(db.dataFd, snapshot.entry.offset, snapshot.entry.size)
				if _, err := io.CopyN(tmp, reader, snapshot.entry.size); err != nil {
					tmp.Close()
					os.Remove(tmpPath)
					return fmt.Errorf("compact: copy chunk: %w", err)
				}
			}
		} else {
			reader := io.NewSectionReader(db.dataFd, snapshot.entry.offset, snapshot.entry.size)
			if _, err := io.CopyN(tmp, reader, snapshot.entry.size); err != nil {
				tmp.Close()
				os.Remove(tmpPath)
				return fmt.Errorf("compact: copy chunk: %w", err)
			}
		}
		snapshot.entry.offset = offset
		offset += size
		compacted = append(compacted, snapshot)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("compact: close temp data file: %w", err)
	}

	currentPath := dataPath(db.dir)
	backupPath := filepath.Join(db.dir, "chunks.dat.old")
	if err := db.dataFd.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("compact: close current data file: %w", err)
	}
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("compact: remove old backup: %w", err)
	}
	if err := os.Rename(currentPath, backupPath); err != nil {
		return fmt.Errorf("compact: backup current data file: %w", err)
	}
	if err := os.Rename(tmpPath, currentPath); err != nil {
		_ = os.Rename(backupPath, currentPath)
		return fmt.Errorf("compact: replace data file: %w", err)
	}

	fd, err := os.OpenFile(currentPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		_ = os.Rename(backupPath, currentPath)
		return fmt.Errorf("compact: reopen data file: %w", err)
	}
	db.dataFd = fd
	db.fileOffset.Store(offset)
	db.index.replace(compacted)
	db.cache.clear()
	if db.delta != nil && db.deltaFd != nil {
		if err := db.truncateDeltaFile(0); err != nil {
			return err
		}
		if db.conf.Options.Durability >= DurabilityBalanced {
			if err := db.deltaFd.Sync(); err != nil {
				return fmt.Errorf("compact: sync truncated deltas.dat: %w", err)
			}
		}
		db.delta = newDeltaIndex()
		db.deltaRecords.Store(0)
	}
	if err := db.syncDataFile("compact"); err != nil {
		return err
	}
	if err := db.saveIndex(); err != nil {
		return fmt.Errorf("compact: save index: %w", err)
	}
	if err := os.Remove(backupPath); err != nil && !os.IsNotExist(err) {
		db.conf.Options.Log.Warn("compact: remove backup", "error", err)
	}
	return nil
}

// Close closes the database and saves all pending data.
func (db *DB) Close() error {
	// Stop periodic flush
	if db.stopFlush != nil {
		close(db.stopFlush)
	}

	// Stop async write worker and wait for it to drain
	if db.stopWrite != nil {
		close(db.stopWrite)
		db.writeWg.Wait()
	}

	// Stop prefetcher
	if db.prefetcher != nil {
		db.prefetcher.Stop()
	}

	// Flush any remaining writes
	if err := db.flushWriteBuffer(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.syncDataFile("close"); err != nil {
		return err
	}

	db.ldat.LastPlayed = time.Now().Unix()

	// Save level.dat
	var ldat leveldat.LevelDat
	if err := ldat.Marshal(*db.ldat); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := ldat.WriteFile(filepath.Join(db.dir, "level.dat")); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	// Save levelname.txt
	if err := os.WriteFile(filepath.Join(db.dir, "levelname.txt"), []byte(db.ldat.LevelName), 0644); err != nil {
		return fmt.Errorf("close: write levelname.txt: %w", err)
	}

	// Save index
	if err := db.saveIndex(); err != nil {
		return fmt.Errorf("close: save index: %w", err)
	}

	// Save metadata
	if err := db.saveMetadata(); err != nil {
		return fmt.Errorf("close: save metadata: %w", err)
	}

	if db.deltaFd != nil {
		if db.conf.Options.Durability >= DurabilityBalanced {
			if err := db.deltaFd.Sync(); err != nil {
				return fmt.Errorf("close: sync deltas.dat: %w", err)
			}
		}
		if err := db.deltaFd.Close(); err != nil {
			return fmt.Errorf("close: close deltas.dat: %w", err)
		}
	}

	// Close data file
	if err := db.dataFd.Close(); err != nil {
		return fmt.Errorf("close: close data file: %w", err)
	}

	return nil
}

// chunkKey uniquely identifies a chunk by position and dimension.
type chunkKey struct {
	x     int32
	z     int32
	dimID int32
}

func newChunkKey(pos world.ChunkPos, dim world.Dimension) chunkKey {
	dimID, _ := world.DimensionID(dim)
	return chunkKey{x: pos[0], z: pos[1], dimID: int32(dimID)}
}

func (key chunkKey) pos() world.ChunkPos {
	return world.ChunkPos{key.x, key.z}
}

func (key chunkKey) dimension() world.Dimension {
	dim, _ := world.DimensionByID(int(key.dimID))
	return dim
}

// Stats holds performance statistics for the database.
type Stats struct {
	ChunkReads      int64
	ChunkWrites     int64
	ChunkWriteSkips int64
	CacheHits       int64
	CacheMisses     int64
	PinnedChunks    int64
	DeltaRecords    int64
	CompressionTime time.Duration
}

// StartStatsLogger starts a background goroutine that logs performance stats
// at the specified interval. Returns a function to stop the logger.
func (db *DB) StartStatsLogger(interval time.Duration) (stop func()) {
	ticker := time.NewTicker(interval)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ticker.C:
				stats := db.GetStats()
				hitRate := float64(0)
				if stats.ChunkReads > 0 {
					hitRate = float64(stats.CacheHits) / float64(stats.ChunkReads) * 100
				}
				db.conf.Options.Log.Info("BlazeDB Stats",
					"reads", stats.ChunkReads,
					"writes", stats.ChunkWrites,
					"write_skips", stats.ChunkWriteSkips,
					"cache_hits", stats.CacheHits,
					"cache_misses", stats.CacheMisses,
					"cache_hit_rate", fmt.Sprintf("%.1f%%", hitRate),
					"index_entries", db.index.count(),
					"write_buffer", len(db.writeBuf),
				)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	return func() {
		close(done)
	}
}

// LogStats logs current performance statistics once.
func (db *DB) LogStats() {
	stats := db.GetStats()
	hitRate := float64(0)
	if stats.ChunkReads > 0 {
		hitRate = float64(stats.CacheHits) / float64(stats.ChunkReads) * 100
	}
	db.conf.Options.Log.Info("BlazeDB Stats",
		"reads", stats.ChunkReads,
		"writes", stats.ChunkWrites,
		"write_skips", stats.ChunkWriteSkips,
		"cache_hits", stats.CacheHits,
		"cache_hit_rate", fmt.Sprintf("%.1f%%", hitRate),
		"index_entries", db.index.count(),
	)
}
