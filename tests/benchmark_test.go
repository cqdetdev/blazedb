package tests

import (
	"os"
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
	"github.com/df-mc/dragonfly/server/world/mcdb"
)

// createTestColumn creates a simple test chunk column for benchmarking.
func createTestColumn(r cube.Range) *chunk.Column {
	// We just need a chunk.Column, the actual content doesn't matter for benchmarks
	c := chunk.New(0, r) // 0 is typically air
	return &chunk.Column{Chunk: c}
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

// TestCleanup removes any leftover test directories
func TestCleanup(t *testing.T) {
	os.RemoveAll("testworld")
}
