package tests

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/block"
	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

func TestOverlayProviderFallsBackAndResets(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "base")
	overlayDir := filepath.Join(t.TempDir(), "overlay")

	base, err := blazedb.Open(baseDir)
	if err != nil {
		t.Fatal(err)
	}
	defer base.Close()

	pos := world.ChunkPos{0, 0}
	stone := runtimeIDForBlock(block.Stone{})
	dirt := runtimeIDForBlock(block.Dirt{})
	baseCol := columnWithBlock(world.Overworld, cube.Pos{1, 0, 1}, stone)
	if err := base.StoreColumn(pos, world.Overworld, baseCol); err != nil {
		t.Fatal(err)
	}

	overlay, err := blazedb.OpenOverlayProvider(base, overlayDir, blazedb.EphemeralWorldOptions(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer overlay.Close()

	col, err := overlay.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	col.Chunk.SetBlock(1, 0, 1, 0, dirt)
	if err := overlay.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}

	baseLoaded, err := base.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := baseLoaded.Chunk.Block(1, 0, 1, 0); got != stone {
		t.Fatalf("base template was mutated: got runtime ID %d, want %d", got, stone)
	}

	overlayLoaded, err := overlay.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := overlayLoaded.Chunk.Block(1, 0, 1, 0); got != dirt {
		t.Fatalf("overlay block = %d, want %d", got, dirt)
	}

	if err := overlay.ResetOverlay(); err != nil {
		t.Fatal(err)
	}
	resetLoaded, err := overlay.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := resetLoaded.Chunk.Block(1, 0, 1, 0); got != stone {
		t.Fatalf("reset overlay block = %d, want base %d", got, stone)
	}
}

func TestDeltaRecordsPersistAndCompact(t *testing.T) {
	dir := t.TempDir()
	opts := blazedb.EphemeralWorldOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	pos := world.ChunkPos{3, -2}
	stone := runtimeIDForBlock(block.Stone{})
	dirt := runtimeIDForBlock(block.Dirt{})
	if err := storeBaseAndDelta(dir, opts, pos, stone, dirt); err != nil {
		t.Fatal(err)
	}

	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := col.Chunk.Block(1, 0, 1, 0); got != dirt {
		t.Fatalf("delta block = %d, want %d", got, dirt)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if size := fileSize(t, filepath.Join(dir, "deltas.dat")); size != 0 {
		t.Fatalf("delta log size after compact = %d, want 0", size)
	}

	db, err = blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, err = db.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := col.Chunk.Block(1, 0, 1, 0); got != dirt {
		t.Fatalf("compacted delta block = %d, want %d", got, dirt)
	}
}

func TestStoreColumnSupersedesDeltaRecords(t *testing.T) {
	dir := t.TempDir()
	opts := blazedb.EphemeralWorldOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	pos := world.ChunkPos{5, 5}
	stone := runtimeIDForBlock(block.Stone{})
	dirt := runtimeIDForBlock(block.Dirt{})
	gold := runtimeIDForBlock(block.Gold{})
	if err := storeBaseAndDelta(dir, opts, pos, stone, dirt); err != nil {
		t.Fatal(err)
	}

	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.StoreColumn(pos, world.Overworld, columnWithBlock(world.Overworld, cube.Pos{1, 0, 1}, gold)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, err := db.LoadColumn(pos, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if got := col.Chunk.Block(1, 0, 1, 0); got != gold {
		t.Fatalf("full store after delta = %d, want %d", got, gold)
	}
}

func TestPinAreaKeepsPinnedStats(t *testing.T) {
	dir := t.TempDir()
	opts := blazedb.DefaultOptions()
	opts.CacheSize = 16 * 1024
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{0, 0}
	if err := db.StoreColumn(pos, world.Overworld, columnWithBlock(world.Overworld, cube.Pos{0, 0, 0}, runtimeIDForBlock(block.Stone{}))); err != nil {
		t.Fatal(err)
	}
	pinned, err := db.PinArea(pos, 0, world.Overworld)
	if err != nil {
		t.Fatal(err)
	}
	if pinned != 1 {
		t.Fatalf("pinned = %d, want 1", pinned)
	}
	if stats := db.GetStats(); stats.PinnedChunks != 1 {
		t.Fatalf("PinnedChunks = %d, want 1", stats.PinnedChunks)
	}
	db.UnpinArea(pos, 0, world.Overworld)
	if stats := db.GetStats(); stats.PinnedChunks != 0 {
		t.Fatalf("PinnedChunks after unpin = %d, want 0", stats.PinnedChunks)
	}
}

func TestSegmentedProviderRoutesAndDeletesSegments(t *testing.T) {
	dir := t.TempDir()
	opts := blazedb.EphemeralWorldOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	provider, err := blazedb.OpenSegmentedProvider(dir, &blazedb.SegmentOptions{
		SegmentSize: 4,
		Options:     opts,
	})
	if err != nil {
		t.Fatal(err)
	}

	left := world.ChunkPos{0, 0}
	right := world.ChunkPos{8, 0}
	stone := runtimeIDForBlock(block.Stone{})
	dirt := runtimeIDForBlock(block.Dirt{})
	if err := provider.StoreColumn(left, world.Overworld, columnWithBlock(world.Overworld, cube.Pos{0, 0, 0}, stone)); err != nil {
		t.Fatal(err)
	}
	if err := provider.StoreColumn(right, world.Overworld, columnWithBlock(world.Overworld, cube.Pos{0, 0, 0}, dirt)); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}

	provider, err = blazedb.OpenSegmentedProvider(dir, &blazedb.SegmentOptions{
		SegmentSize: 4,
		Options:     opts,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	if col, err := provider.LoadColumn(left, world.Overworld); err != nil || col.Chunk.Block(0, 0, 0, 0) != stone {
		t.Fatalf("load left: col=%v err=%v", col, err)
	}
	if col, err := provider.LoadColumn(right, world.Overworld); err != nil || col.Chunk.Block(0, 0, 0, 0) != dirt {
		t.Fatalf("load right: col=%v err=%v", col, err)
	}
	if err := provider.DeleteSegmentFor(right, world.Overworld); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.LoadColumn(right, world.Overworld); !errors.Is(err, blazedb.ErrNotFound) {
		t.Fatalf("load deleted segment err = %v, want ErrNotFound", err)
	}
	if _, err := provider.LoadColumn(left, world.Overworld); err != nil {
		t.Fatalf("left segment should remain: %v", err)
	}
}

func storeBaseAndDelta(dir string, opts *blazedb.Options, pos world.ChunkPos, baseID, deltaID uint32) error {
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		return err
	}
	if err := db.StoreColumn(pos, world.Overworld, columnWithBlock(world.Overworld, cube.Pos{1, 0, 1}, baseID)); err != nil {
		_ = db.Close()
		return err
	}
	if err := db.StoreBlockDeltas(pos, world.Overworld, []blazedb.BlockDelta{{
		Pos:       cube.Pos{1, 0, 1},
		RuntimeID: deltaID,
	}}); err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}

func columnWithBlock(dim world.Dimension, pos cube.Pos, runtimeID uint32) *chunk.Column {
	c := chunk.New(0, dim.Range())
	c.SetBlock(uint8(pos.X()&15), int16(pos.Y()), uint8(pos.Z()&15), 0, runtimeID)
	return &chunk.Column{Chunk: c}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return stat.Size()
}
