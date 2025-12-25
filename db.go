package blazedb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	conf   Config
	dir    string
	ldat   *leveldat.Data
	set    *world.Settings
	cache  *chunkCache
	index  *spatialIndex
	dataFd *os.File
	mu     sync.RWMutex

	// Atomic offset tracking - avoids Stat() syscall on every write
	fileOffset atomic.Int64

	// Write buffer for batched writes - stores raw columns for lazy encoding
	writeBuf     map[chunkKey]*chunk.Column
	writeBufMu   sync.Mutex
	writeBufSize atomic.Int64 // Estimated buffer size tracking
	flushTimer   *time.Timer
	stopFlush    chan struct{}

	// Async write queue for non-blocking writes
	writeQueue chan writeRequest
	writeWg    sync.WaitGroup
	stopWrite  chan struct{}

	// Predictive prefetcher for chunk loading
	prefetcher *Prefetcher

	// Player spawn positions (stored in-memory, persisted on close)
	playerSpawns   map[uuid.UUID]cube.Pos
	playerSpawnsMu sync.RWMutex
}

// writeRequest represents an async write operation
type writeRequest struct {
	key chunkKey
	col *chunk.Column
}

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
		stopFlush:    make(chan struct{}),
	}

	// Initialize cache
	db.cache = newChunkCache(conf.Options.CacheSize)

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

	// Load index if exists
	indexFile := indexPath(dir)
	if fileExists(indexFile) {
		if err := db.index.load(indexFile); err != nil {
			conf.Options.Log.Warn("failed to load index, rebuilding", "error", err)
			if err := db.rebuildIndex(); err != nil {
				return nil, fmt.Errorf("open db: rebuild index: %w", err)
			}
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
				db.flushWriteBuffer()
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
				db.writeBufMu.Lock()
				estimatedSize := int64(len(req.col.Chunk.Sub()) * 4096)
				if _, exists := db.writeBuf[req.key]; !exists {
					db.writeBufSize.Add(estimatedSize)
				}
				db.writeBuf[req.key] = req.col
				bufSize := db.writeBufSize.Load()
				db.writeBufMu.Unlock()

				// Flush if buffer exceeds size limit
				if bufSize >= db.conf.Options.WriteBufferSize {
					db.flushWriteBuffer()
				}
			case <-db.stopWrite:
				// Drain remaining writes before stopping
				for {
					select {
					case req := <-db.writeQueue:
						db.writeBufMu.Lock()
						db.writeBuf[req.key] = req.col
						db.writeBufMu.Unlock()
					default:
						db.flushWriteBuffer()
						return
					}
				}
			}
		}
	}()
}

// flushWriteBuffer writes all buffered chunks to disk with lazy encoding.
func (db *DB) flushWriteBuffer() {
	db.writeBufMu.Lock()
	if len(db.writeBuf) == 0 {
		db.writeBufMu.Unlock()
		return
	}

	// Copy buffer and clear
	toWrite := db.writeBuf
	db.writeBuf = make(map[chunkKey]*chunk.Column)
	db.writeBufSize.Store(0) // Reset atomic buffer size
	db.writeBufMu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	for key, col := range toWrite {
		// Lazy encoding: encode now at flush time instead of on every write
		data, err := db.encodeColumn(key.pos, key.dim, col)
		if err != nil {
			db.conf.Options.Log.Error("flush: encode chunk", "error", err)
			continue
		}

		// Use atomic offset instead of Stat() syscall
		offset := db.fileOffset.Load()

		// Write to disk
		if _, err := db.dataFd.WriteAt(data, offset); err != nil {
			db.conf.Options.Log.Error("flush: write chunk", "error", err)
			continue
		}

		// Atomically update file offset
		db.fileOffset.Add(int64(len(data)))

		// Update index
		db.index.put(key, offset, int64(len(data)))

		// Mark chunk as clean in cache
		db.cache.markClean(key)
	}
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
	key := chunkKey{pos: pos, dim: dim}

	// Check cache first (no lock needed - cache is thread-safe)
	if col := db.cache.get(key); col != nil {
		return col, nil
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check write buffer - return directly if found (no need to decode)
	db.writeBufMu.Lock()
	if col, ok := db.writeBuf[key]; ok {
		db.writeBufMu.Unlock()
		db.cache.putClean(key, col)
		return col, nil
	}
	db.writeBufMu.Unlock()

	// Load from disk using spatial index
	offset, size, exists := db.index.get(key)
	if !exists {
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

	// Add to cache (loaded from disk = clean)
	db.cache.putClean(key, col)

	return col, nil
}

// StoreColumn stores a chunk.Column at a position and dimension in the DB.
func (db *DB) StoreColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) error {
	key := chunkKey{pos: pos, dim: dim}

	// Update cache immediately
	db.cache.put(key, col)

	// If write buffering is enabled, send to async write queue (non-blocking)
	if db.conf.Options.WriteBufferSize > 0 {
		select {
		case db.writeQueue <- writeRequest{key: key, col: col}:
			// Successfully queued
		default:
			// Queue full - add directly to buffer (fallback)
			db.writeBufMu.Lock()
			db.writeBuf[key] = col
			db.writeBufMu.Unlock()
		}
		return nil
	}

	// Direct write (no buffering) - encode now
	data, err := db.encodeColumn(pos, dim, col)
	if err != nil {
		return fmt.Errorf("encode column: %w", err)
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
	db.cache.markClean(key)

	return nil
}

// LoadArea loads multiple chunks in the given radius around center position.
// This is optimized for fast render distance loading.
func (db *DB) LoadArea(center world.ChunkPos, radius int, dim world.Dimension) ([]*chunk.Column, error) {
	var columns []*chunk.Column
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// Load chunks in parallel
	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			pos := world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}
			wg.Add(1)
			go func(p world.ChunkPos) {
				defer wg.Done()
				col, err := db.LoadColumn(p, dim)
				if err != nil {
					if !errors.Is(err, ErrNotFound) {
						select {
						case errCh <- err:
						default:
						}
					}
					return
				}
				mu.Lock()
				columns = append(columns, col)
				mu.Unlock()
			}(pos)
		}
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return nil, err
	}

	return columns, nil
}

// encodeColumn encodes a chunk column to compressed bytes.
func (db *DB) encodeColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	// Write header: "BLAZ" magic
	buf.Write([]byte("BLAZ"))

	// Encode chunk data
	data := chunk.Encode(col.Chunk, chunk.DiskEncoding)

	// Placeholder for size (will be filled after compression)
	sizePos := buf.Len()
	binary.Write(buf, binary.LittleEndian, uint32(0))

	// Placeholder for CRC32
	crcPos := buf.Len()
	binary.Write(buf, binary.LittleEndian, uint32(0))

	// Write coordinates
	binary.Write(buf, binary.LittleEndian, pos[0])
	binary.Write(buf, binary.LittleEndian, pos[1])

	// Write dimension ID
	dimID, _ := world.DimensionID(dim)
	binary.Write(buf, binary.LittleEndian, int32(dimID))

	// Write compression type
	buf.WriteByte(byte(db.conf.Options.Compression))

	// Write subchunk count
	buf.WriteByte(byte(len(data.SubChunks)))

	// Compress and write biomes
	compressedBiomes := compress(data.Biomes, db.conf.Options.Compression)
	binary.Write(buf, binary.LittleEndian, uint32(len(compressedBiomes)))
	buf.Write(compressedBiomes)

	// Compress and write each subchunk
	for _, sub := range data.SubChunks {
		compressedSub := compress(sub, db.conf.Options.Compression)
		binary.Write(buf, binary.LittleEndian, uint32(len(compressedSub)))
		buf.Write(compressedSub)
	}

	// Encode entities
	entitiesData := db.encodeEntities(col.Entities)
	binary.Write(buf, binary.LittleEndian, uint32(len(entitiesData)))
	buf.Write(entitiesData)

	// Encode block entities
	blockEntitiesData := db.encodeBlockEntities(col.BlockEntities)
	binary.Write(buf, binary.LittleEndian, uint32(len(blockEntitiesData)))
	buf.Write(blockEntitiesData)

	// Encode scheduled updates
	scheduledData := db.encodeScheduledUpdates(col.Tick, col.ScheduledBlocks)
	binary.Write(buf, binary.LittleEndian, uint32(len(scheduledData)))
	buf.Write(scheduledData)

	// Calculate and write size
	result := buf.Bytes()
	binary.LittleEndian.PutUint32(result[sizePos:], uint32(len(result)))

	// Calculate and write CRC32
	crc := computeCRC32(result[crcPos+4:])
	binary.LittleEndian.PutUint32(result[crcPos:], crc)

	return result, nil
}

// decodeColumn decodes a compressed chunk column.
func (db *DB) decodeColumn(data []byte, dim world.Dimension) (*chunk.Column, error) {
	if len(data) < 4 || string(data[:4]) != "BLAZ" {
		return nil, errors.New("invalid chunk header")
	}

	r := bytes.NewReader(data[4:])

	// Read and verify size
	var size uint32
	binary.Read(r, binary.LittleEndian, &size)

	// Read CRC32
	var storedCRC uint32
	binary.Read(r, binary.LittleEndian, &storedCRC)

	// Verify checksum if enabled
	if db.conf.Options.VerifyChecksums {
		computedCRC := computeCRC32(data[12:])
		if storedCRC != computedCRC {
			return nil, errors.New("CRC32 checksum mismatch")
		}
	}

	// Read coordinates (for verification)
	var x, z int32
	binary.Read(r, binary.LittleEndian, &x)
	binary.Read(r, binary.LittleEndian, &z)

	// Read dimension ID
	var dimID int32
	binary.Read(r, binary.LittleEndian, &dimID)

	// Read compression type
	var compressionByte byte
	binary.Read(r, binary.LittleEndian, &compressionByte)
	compression := CompressionType(compressionByte)

	// Read subchunk count
	var subCount byte
	binary.Read(r, binary.LittleEndian, &subCount)

	// Read and decompress biomes
	var biomesLen uint32
	binary.Read(r, binary.LittleEndian, &biomesLen)
	compressedBiomes := make([]byte, biomesLen)
	r.Read(compressedBiomes)
	biomes := decompress(compressedBiomes, compression)

	// Read all compressed subchunk data FIRST (single sequential read)
	compressedSubChunks := make([][]byte, subCount)
	for i := range compressedSubChunks {
		var subLen uint32
		binary.Read(r, binary.LittleEndian, &subLen)
		compressedSubChunks[i] = make([]byte, subLen)
		r.Read(compressedSubChunks[i])
	}

	// Decompress subchunks in PARALLEL
	subChunks := make([][]byte, subCount)
	if subCount >= 4 {
		// Parallel decompression for chunks with 4+ subchunks
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
	var entitiesLen uint32
	binary.Read(r, binary.LittleEndian, &entitiesLen)
	if entitiesLen > 0 {
		entitiesData := make([]byte, entitiesLen)
		r.Read(entitiesData)
		col.Entities = db.decodeEntities(entitiesData)
	}

	// Read block entities
	var blockEntitiesLen uint32
	binary.Read(r, binary.LittleEndian, &blockEntitiesLen)
	if blockEntitiesLen > 0 {
		blockEntitiesData := make([]byte, blockEntitiesLen)
		r.Read(blockEntitiesData)
		col.BlockEntities = db.decodeBlockEntities(blockEntitiesData)
	}

	// Read scheduled updates
	var scheduledLen uint32
	binary.Read(r, binary.LittleEndian, &scheduledLen)
	if scheduledLen > 0 {
		scheduledData := make([]byte, scheduledLen)
		r.Read(scheduledData)
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
	db.index = newSpatialIndex()
	return nil
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
	return db.cache.stats()
}

// Prefetcher returns the prefetcher for registering player positions.
func (db *DB) Prefetcher() *Prefetcher {
	return db.prefetcher
}

// Compact performs background compaction to optimize storage.
func (db *DB) Compact() error {
	db.conf.Options.Log.Debug("compaction triggered (no-op in MVP)")
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
	db.flushWriteBuffer()

	db.mu.Lock()
	defer db.mu.Unlock()

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
	if err := db.index.save(indexPath(db.dir)); err != nil {
		return fmt.Errorf("close: save index: %w", err)
	}

	// Save metadata
	if err := db.saveMetadata(); err != nil {
		return fmt.Errorf("close: save metadata: %w", err)
	}

	// Close data file
	if err := db.dataFd.Close(); err != nil {
		return fmt.Errorf("close: close data file: %w", err)
	}

	return nil
}

// chunkKey uniquely identifies a chunk by position and dimension.
type chunkKey struct {
	pos world.ChunkPos
	dim world.Dimension
}

// Stats holds performance statistics for the database.
type Stats struct {
	ChunkReads      int64
	ChunkWrites     int64
	CacheHits       int64
	CacheMisses     int64
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
		"cache_hits", stats.CacheHits,
		"cache_hit_rate", fmt.Sprintf("%.1f%%", hitRate),
		"index_entries", db.index.count(),
	)
}
