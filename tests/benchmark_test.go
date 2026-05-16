package tests

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
	"github.com/df-mc/dragonfly/server/world/mcdb"
	"github.com/df-mc/goleveldb/leveldb/opt"
)

// createTestColumn creates a simple test chunk column for benchmarking.
func createTestColumn(r cube.Range) *chunk.Column {
	// We just need a chunk.Column, the actual content doesn't matter for benchmarks
	c := chunk.New(0, r) // 0 is typically air
	return &chunk.Column{Chunk: c}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func benchmarkPositions(width, height int32) []world.ChunkPos {
	positions := make([]world.ChunkPos, 0, width*height)
	for z := int32(0); z < height; z++ {
		for x := int32(0); x < width; x++ {
			positions = append(positions, world.ChunkPos{x, z})
		}
	}
	return positions
}

func blazeOptions(cacheSize, writeBufferSize int64) *blazedb.Options {
	opts := blazedb.DefaultOptions()
	opts.CacheSize = cacheSize
	opts.WriteBufferSize = writeBufferSize
	opts.FlushInterval = 0
	opts.Log = discardLogger()
	return opts
}

func levelOptionsNoCache() *opt.Options {
	return &opt.Options{
		BlockCacheCapacity: -1,
		DisableBlockCache:  true,
		NoSync:             true,
	}
}

func populateBlazeDB(tb testing.TB, dir string, positions []world.ChunkPos, opts *blazedb.Options) {
	tb.Helper()

	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}
	col := createTestColumn(world.Overworld.Range())
	for _, pos := range positions {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			tb.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

func populateLevelDB(tb testing.TB, dir string, positions []world.ChunkPos, opts *opt.Options) {
	tb.Helper()

	db, err := mcdb.Config{Log: discardLogger(), LDBOptions: opts}.Open(dir)
	if err != nil {
		tb.Fatal(err)
	}
	col := createTestColumn(world.Overworld.Range())
	for _, pos := range positions {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			tb.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

func loadArea(db interface {
	LoadColumn(world.ChunkPos, world.Dimension) (*chunk.Column, error)
}, center world.ChunkPos, radius int, dim world.Dimension) error {
	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			pos := world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}
			if _, err := db.LoadColumn(pos, dim); err != nil {
				return err
			}
		}
	}
	return nil
}

// BenchmarkBlazeDBLoad benchmarks chunk loading from BlazeDB.
func BenchmarkBlazeDBLoad(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate with some chunks
	for i := 0; i < 100; i++ {
		pos := world.ChunkPos{int32(i % 10), int32(i / 10)}
		col := createTestColumn(world.Overworld.Range())
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 10), int32(i / 10)}
		_, _ = db.LoadColumn(pos, world.Overworld)
	}
}

// BenchmarkBlazeDBStore benchmarks chunk storing to BlazeDB.
func BenchmarkBlazeDBStore(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

// BenchmarkBlazeDBStoreImmediate benchmarks end-to-end writes without the async write buffer.
func BenchmarkBlazeDBStoreImmediate(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Config{Options: blazeOptions(256*1024*1024, 0)}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

func BenchmarkBlazeDBStoreNoCompression(b *testing.B) {
	dir := b.TempDir()
	opts := blazeOptions(256*1024*1024, 4*1024*1024)
	opts.Compression = blazedb.CompressionNone
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

func BenchmarkBlazeDBStoreBalanced(b *testing.B) {
	dir := b.TempDir()
	opts := blazedb.BalancedOptions()
	opts.Log = discardLogger()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

func BenchmarkBlazeDBStoreSafe(b *testing.B) {
	dir := b.TempDir()
	opts := blazedb.SafeOptions()
	opts.Log = discardLogger()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

// BenchmarkLevelDBLoad benchmarks chunk loading from LevelDB.
func BenchmarkLevelDBLoad(b *testing.B) {
	dir := b.TempDir()
	db, err := mcdb.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate with some chunks
	for i := 0; i < 100; i++ {
		pos := world.ChunkPos{int32(i % 10), int32(i / 10)}
		col := createTestColumn(world.Overworld.Range())
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 10), int32(i / 10)}
		_, _ = db.LoadColumn(pos, world.Overworld)
	}
}

// BenchmarkLevelDBStore benchmarks chunk storing to LevelDB.
func BenchmarkLevelDBStore(b *testing.B) {
	dir := b.TempDir()
	db, err := mcdb.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

// BenchmarkLevelDBStoreNoSync gives LevelDB the same no-fsync durability profile BlazeDB uses.
func BenchmarkLevelDBStoreNoSync(b *testing.B) {
	dir := b.TempDir()
	db, err := mcdb.Config{Log: discardLogger(), LDBOptions: &opt.Options{NoSync: true}}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	col := createTestColumn(world.Overworld.Range())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 1000), int32(i / 1000)}
		_ = db.StoreColumn(pos, world.Overworld, col)
	}
}

// BenchmarkBlazeDBLoadArea benchmarks parallel area loading (BlazeDB only).
func BenchmarkBlazeDBLoadArea(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate with chunks in a 20x20 area
	for x := int32(0); x < 20; x++ {
		for z := int32(0); z < 20; z++ {
			pos := world.ChunkPos{x, z}
			col := createTestColumn(world.Overworld.Range())
			if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		center := world.ChunkPos{10, 10}
		_, _ = db.LoadArea(center, 5, world.Overworld) // 11x11 area
	}
}

func BenchmarkBlazeDBLoadAreaSparseMissing(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Config{Options: blazeOptions(256*1024*1024, 0)}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	center := world.ChunkPos{10000, 10000}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.LoadArea(center, 5, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlazeDBLoadAreaSparseMissingCold(b *testing.B) {
	dir := b.TempDir()
	db, err := blazedb.Config{Options: blazeOptions(256*1024*1024, 0)}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		center := world.ChunkPos{int32(10000 + i*32), int32(10000 + i*32)}
		if _, err := db.LoadArea(center, 5, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBlazeDBLoadReopenedTinyCache measures reads after reopening the DB,
// with the chunk cache too small to keep the working set hot.
func BenchmarkBlazeDBLoadReopenedTinyCache(b *testing.B) {
	dir := b.TempDir()
	positions := benchmarkPositions(64, 64)
	populateBlazeDB(b, dir, positions, blazeOptions(1, 0))

	db, err := blazedb.Config{Options: blazeOptions(1, 0)}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := positions[i%len(positions)]
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlazeDBLoadReopenedNoCompressionTinyCache(b *testing.B) {
	dir := b.TempDir()
	positions := benchmarkPositions(64, 64)
	opts := blazeOptions(1, 0)
	opts.Compression = blazedb.CompressionNone
	populateBlazeDB(b, dir, positions, opts)

	opts = blazeOptions(1, 0)
	opts.Compression = blazedb.CompressionNone
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := positions[i%len(positions)]
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkLevelDBLoadReopenedNoBlockCache measures LevelDB reads after reopening,
// with the LevelDB block cache disabled to expose storage/decode overhead.
func BenchmarkLevelDBLoadReopenedNoBlockCache(b *testing.B) {
	dir := b.TempDir()
	positions := benchmarkPositions(64, 64)
	populateLevelDB(b, dir, positions, levelOptionsNoCache())

	db, err := mcdb.Config{Log: discardLogger(), LDBOptions: levelOptionsNoCache()}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := positions[i%len(positions)]
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBlazeDBLoadAreaReopenedTinyCache(b *testing.B) {
	dir := b.TempDir()
	positions := benchmarkPositions(32, 32)
	populateBlazeDB(b, dir, positions, blazeOptions(1, 0))

	db, err := blazedb.Config{Options: blazeOptions(1, 0)}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	center := world.ChunkPos{16, 16}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := loadArea(db, center, 5, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLevelDBLoadAreaReopenedNoBlockCache(b *testing.B) {
	dir := b.TempDir()
	positions := benchmarkPositions(32, 32)
	populateLevelDB(b, dir, positions, levelOptionsNoCache())

	db, err := mcdb.Config{Log: discardLogger(), LDBOptions: levelOptionsNoCache()}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	center := world.ChunkPos{16, 16}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := loadArea(db, center, 5, world.Overworld); err != nil {
			b.Fatal(err)
		}
	}
}

// TestCleanup removes any leftover test directories
func TestCleanup(t *testing.T) {
	os.RemoveAll("testworld")
}
