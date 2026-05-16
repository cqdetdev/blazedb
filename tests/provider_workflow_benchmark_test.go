package tests

import (
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/block"
	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
)

func BenchmarkBlazeDBStoreBlockDeltas(b *testing.B) {
	dir := b.TempDir()
	opts := blazedb.EphemeralWorldOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{0, 0}
	if err := db.StoreColumn(pos, world.Overworld, createDenseTestColumn(world.Overworld.Range())); err != nil {
		b.Fatal(err)
	}
	stone := runtimeIDForBlock(block.Stone{})
	dirt := runtimeIDForBlock(block.Dirt{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rid := stone
		if i&1 == 0 {
			rid = dirt
		}
		err := db.StoreBlockDeltas(pos, world.Overworld, []blazedb.BlockDelta{{
			Pos:       cube.Pos{i & 15, 0, (i >> 4) & 15},
			RuntimeID: rid,
		}})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOverlayProviderReset(b *testing.B) {
	baseDir := b.TempDir()
	base, err := blazedb.Open(baseDir)
	if err != nil {
		b.Fatal(err)
	}
	defer base.Close()

	col := createDenseTestColumn(world.Overworld.Range())
	positions := benchmarkPositions(8, 8)
	for _, pos := range positions {
		if err := base.StoreColumn(pos, world.Overworld, col); err != nil {
			b.Fatal(err)
		}
	}

	opts := blazedb.EphemeralWorldOptions()
	opts.Log = discardLogger()
	overlay, err := blazedb.OpenOverlayProvider(base, b.TempDir(), opts, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer overlay.Close()

	dirt := runtimeIDForBlock(block.Dirt{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := positions[i%len(positions)]
		if err := overlay.StoreBlockDeltas(pos, world.Overworld, []blazedb.BlockDelta{{
			Pos:       cube.Pos{int(pos[0]<<4) + 1, 0, int(pos[1]<<4) + 1},
			RuntimeID: dirt,
		}}); err != nil {
			b.Fatal(err)
		}
		if i%len(positions) == len(positions)-1 {
			if err := overlay.ResetOverlay(); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkSegmentedProviderStore(b *testing.B) {
	opts := blazedb.EphemeralWorldOptions()
	opts.Log = discardLogger()
	provider, err := blazedb.OpenSegmentedProvider(b.TempDir(), &blazedb.SegmentOptions{
		SegmentSize: 16,
		Options:     opts,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer provider.Close()

	col := createTestColumn(world.Overworld.Range())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := world.ChunkPos{int32(i % 512), int32(i / 512)}
		if err := provider.StoreColumn(pos, world.Overworld, col); err != nil {
			b.Fatal(err)
		}
	}
}
