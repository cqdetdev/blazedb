package blazedb

import (
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

func discardTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestCompactRewritesLiveChunks(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.Log = discardTestLogger()

	dir := t.TempDir()
	db, err := Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	col := &chunk.Column{Chunk: chunk.New(0, world.Overworld.Range())}
	positions := []world.ChunkPos{{0, 0}, {-32, 16}, {64, -64}}
	for i := 0; i < 5; i++ {
		for _, pos := range positions {
			if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
				t.Fatal(err)
			}
		}
	}

	before, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatalf("expected compaction to shrink data file: before=%d after=%d", before.Size(), after.Size())
	}

	for _, pos := range positions {
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			t.Fatalf("load compacted chunk %v: %v", pos, err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
