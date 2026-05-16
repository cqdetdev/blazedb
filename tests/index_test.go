package tests

import (
	"os"
	"testing"

	"github.com/df-mc/dragonfly/server/world"
)

func TestSpatialIndexPreservesCoordinatesAndDimensions(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)

	cases := []struct {
		pos world.ChunkPos
		dim world.Dimension
		id  int64
	}{
		{pos: world.ChunkPos{-1024, 512}, dim: world.Overworld, id: 1},
		{pos: world.ChunkPos{-1024, 512}, dim: world.Nether, id: 2},
		{pos: world.ChunkPos{2048, -4096}, dim: world.End, id: 3},
	}

	for _, tc := range cases {
		storeRecoveryColumn(t, db, dir, tc.pos, tc.dim, recoveryTestColumnWithEntityForDim(tc.dim, tc.id))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	for _, tc := range cases {
		if got := loadEntityID(t, db, tc.pos, tc.dim); got != tc.id {
			t.Fatalf("loaded wrong entity for pos=%v dim=%T: got %d want %d", tc.pos, tc.dim, got, tc.id)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}
	db = openRecoveryDB(t, dir)
	defer db.Close()
	for _, tc := range cases {
		if got := loadEntityID(t, db, tc.pos, tc.dim); got != tc.id {
			t.Fatalf("rebuilt index loaded wrong entity for pos=%v dim=%T: got %d want %d", tc.pos, tc.dim, got, tc.id)
		}
	}
}
