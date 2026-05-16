package blazedb

import (
	"testing"

	"github.com/df-mc/dragonfly/server/world"
)

func TestSpatialIndexPreservesCoordinatesAndDimensions(t *testing.T) {
	idx := newSpatialIndex()
	keys := []chunkKey{
		newChunkKey(world.ChunkPos{-1024, 512}, world.Overworld),
		newChunkKey(world.ChunkPos{-1024, 512}, world.Nether),
		newChunkKey(world.ChunkPos{2048, -4096}, world.End),
	}

	for i, key := range keys {
		idx.put(key, int64(100+i), int64(200+i))
	}

	for i, key := range keys {
		offset, size, ok := idx.get(key)
		if !ok {
			t.Fatalf("missing key %v", key)
		}
		if offset != int64(100+i) || size != int64(200+i) {
			t.Fatalf("unexpected entry for %v: offset=%d size=%d", key, offset, size)
		}
	}

	seen := make(map[chunkKey]struct{}, len(keys))
	idx.iterate(func(key chunkKey, _, _ int64) bool {
		seen[key] = struct{}{}
		return true
	})
	for _, key := range keys {
		if _, ok := seen[key]; !ok {
			t.Fatalf("iterator did not preserve key %v", key)
		}
	}

	path := t.TempDir() + "/index.dat"
	if err := idx.save(path); err != nil {
		t.Fatal(err)
	}
	loaded := newSpatialIndex()
	if err := loaded.load(path); err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if _, _, ok := loaded.get(key); !ok {
			t.Fatalf("loaded index missing key %v", key)
		}
	}
}
