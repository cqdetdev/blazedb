package blazedb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

func recoveryTestOptions() *Options {
	opts := DefaultOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.VerifyChecksums = true
	opts.Log = discardTestLogger()
	return opts
}

func recoveryTestColumn() *chunk.Column {
	return &chunk.Column{Chunk: chunk.New(0, world.Overworld.Range())}
}

func recoveryTestColumnWithEntity(id int64) *chunk.Column {
	return &chunk.Column{
		Chunk: chunk.New(0, world.Overworld.Range()),
		Entities: []chunk.Entity{{
			ID:   id,
			Data: map[string]any{"identifier": "blazedb:test"},
		}},
	}
}

func openRecoveryDB(t *testing.T, dir string) *DB {
	t.Helper()

	db, err := Config{Options: recoveryTestOptions()}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func storeRecoveryChunk(t *testing.T, db *DB, pos world.ChunkPos) (offset, size int64) {
	t.Helper()

	if err := db.StoreColumn(pos, world.Overworld, recoveryTestColumn()); err != nil {
		t.Fatal(err)
	}
	offset, size, ok := db.index.get(newChunkKey(pos, world.Overworld))
	if !ok {
		t.Fatalf("index missing stored chunk %v", pos)
	}
	return offset, size
}

func storeRecoveryColumn(t *testing.T, db *DB, pos world.ChunkPos, col *chunk.Column) (offset, size int64) {
	t.Helper()

	if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}
	offset, size, ok := db.index.get(newChunkKey(pos, world.Overworld))
	if !ok {
		t.Fatalf("index missing stored chunk %v", pos)
	}
	return offset, size
}

func TestRebuildIndexWhenIndexDeleted(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	positions := []world.ChunkPos{{0, 0}, {-32, 16}, {64, -64}}
	for _, pos := range positions {
		storeRecoveryChunk(t, db, pos)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	if got := db.index.count(); got != len(positions) {
		t.Fatalf("expected rebuilt index to contain %d entries, got %d", len(positions), got)
	}
	for _, pos := range positions {
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			t.Fatalf("load recovered chunk %v: %v", pos, err)
		}
	}
}

func TestRebuildIndexWhenIndexCorrupted(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	positions := []world.ChunkPos{{0, 0}, {8, -8}}
	for _, pos := range positions {
		storeRecoveryChunk(t, db, pos)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath(dir), []byte("not a valid BlazeDB index"), 0666); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	for _, pos := range positions {
		if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
			t.Fatalf("load chunk after corrupt index rebuild %v: %v", pos, err)
		}
	}
}

func TestRebuildIndexKeepsLatestChunkVersion(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	pos := world.ChunkPos{4, 9}

	firstOffset, _ := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(1))
	latestOffset, _ := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(2))
	if latestOffset <= firstOffset {
		t.Fatalf("expected second write to append after first: first=%d latest=%d", firstOffset, latestOffset)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	offset, _, ok := db.index.get(newChunkKey(pos, world.Overworld))
	if !ok {
		t.Fatal("rebuilt index missing chunk")
	}
	if offset != latestOffset {
		t.Fatalf("rebuilt index did not keep latest version: got offset %d, want %d", offset, latestOffset)
	}
}

func TestRebuildIndexSkipsBadChecksumRecord(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	pos := world.ChunkPos{-7, 12}

	firstOffset, _ := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(1))
	latestOffset, latestSize := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(2))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(dataPath(dir), os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0x7f}, latestOffset+latestSize-1); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	offset, _, ok := db.index.get(newChunkKey(pos, world.Overworld))
	if !ok {
		t.Fatal("rebuilt index missing previous valid chunk version")
	}
	if offset != firstOffset {
		t.Fatalf("expected corrupt latest record to be skipped: got offset %d, want %d", offset, firstOffset)
	}
	if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
		t.Fatalf("load previous valid chunk after skipping corrupt latest: %v", err)
	}
}

func TestRebuildIndexTruncatesPartialTailWrite(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	pos := world.ChunkPos{2, 3}
	storeRecoveryChunk(t, db, pos)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(dataPath(dir), os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{'B', 'L', 'A', 'Z', 0xff}); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	after, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("expected partial tail to be truncated to %d bytes, got %d", before.Size(), after.Size())
	}
	if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
		t.Fatalf("load chunk after partial tail truncation: %v", err)
	}
}

func TestOpenRebuildsValidButStaleIndex(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	storeRecoveryChunk(t, db, world.ChunkPos{0, 0})
	laterPos := world.ChunkPos{9, -9}
	laterRecord, err := db.encodeColumn(laterPos, world.Overworld, recoveryTestColumn())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(dataPath(dir), os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(laterRecord); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

	if _, err := db.LoadColumn(laterPos, world.Overworld); err != nil {
		t.Fatalf("load chunk from stale-index tail rebuild: %v", err)
	}
}

func TestSafeDurabilityDisablesWriteBuffer(t *testing.T) {
	opts := DefaultOptions()
	opts.Durability = DurabilitySafe
	opts.WriteBufferSize = 4 * 1024 * 1024
	opts.FlushInterval = 1000
	opts.Log = discardTestLogger()

	db, err := Config{Options: opts}.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.conf.Options.WriteBufferSize != 0 {
		t.Fatalf("safe durability should disable write buffering, got %d", db.conf.Options.WriteBufferSize)
	}
	if db.conf.Options.FlushInterval != 0 {
		t.Fatalf("safe durability should disable periodic flushing, got %d", db.conf.Options.FlushInterval)
	}
}

func TestStoreColumnUnchangedImmediateSkipsAppend(t *testing.T) {
	opts := recoveryTestOptions()
	opts.Durability = DurabilityFast

	dir := t.TempDir()
	db, err := Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{3, -5}
	col := recoveryTestColumn()
	if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("unchanged writes should not append: before=%d after=%d", before.Size(), after.Size())
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 5 {
		t.Fatalf("expected 5 unchanged write skips, got %d", skips)
	}
}

func TestStoreColumnUnchangedSafeBatchSkipsAppend(t *testing.T) {
	opts := SafeBatchOptions()
	opts.Log = discardTestLogger()

	dir := t.TempDir()
	db, err := Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{-9, 4}
	col := recoveryTestColumn()
	if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("unchanged safe-batch writes should not append: before=%d after=%d", before.Size(), after.Size())
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 3 {
		t.Fatalf("expected 3 unchanged write skips, got %d", skips)
	}
}

func TestStoreColumnChangedStillAppends(t *testing.T) {
	opts := recoveryTestOptions()
	opts.Durability = DurabilityFast

	dir := t.TempDir()
	db, err := Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{6, 6}
	firstOffset, _ := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(1))
	secondOffset, _ := storeRecoveryColumn(t, db, pos, recoveryTestColumnWithEntity(2))
	if secondOffset <= firstOffset {
		t.Fatalf("changed write should append after first record: first=%d second=%d", firstOffset, secondOffset)
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 0 {
		t.Fatalf("changed writes should not be counted as skips, got %d", skips)
	}
}

func TestFlushWriteBufferRetainsWritesOnWriteError(t *testing.T) {
	opts := DefaultOptions()
	opts.WriteBufferSize = 4 * 1024 * 1024
	opts.FlushInterval = 0
	opts.Log = discardTestLogger()

	db, err := Config{Options: opts}.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	close(db.stopWrite)
	db.writeWg.Wait()
	db.prefetcher.Stop()

	key := newChunkKey(world.ChunkPos{11, -3}, world.Overworld)
	col := recoveryTestColumn()
	db.writeBufMu.Lock()
	db.writeBuf[key] = col
	db.writeBufSize.Store(int64(len(col.Chunk.Sub()) * 4096))
	db.writeBufMu.Unlock()

	if err := db.dataFd.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.flushWriteBuffer(); err == nil {
		t.Fatal("expected flush to fail after closing chunks.dat")
	}

	db.writeBufMu.Lock()
	_, retained := db.writeBuf[key]
	size := db.writeBufSize.Load()
	db.writeBufMu.Unlock()
	if !retained {
		t.Fatal("expected failed flush to retain buffered write")
	}
	if size == 0 {
		t.Fatal("expected retained write to keep buffer size accounting")
	}
}

func FuzzDecodeColumnRejectsCorruption(f *testing.F) {
	opts := recoveryTestOptions()
	db := &DB{conf: Config{Options: opts}}
	valid, err := db.encodeColumn(world.ChunkPos{1, -1}, world.Overworld, recoveryTestColumn())
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte{})
	f.Add([]byte("not a chunk"))
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("decodeColumn panicked: %v", r)
			}
		}()
		_, _ = db.decodeColumn(data, world.Overworld)
	})
}

func FuzzSpatialIndexLoadRejectsCorruption(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not an index"))
	f.Add([]byte{'B', 'I', 'D', 'X', 0, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("spatialIndex.load panicked: %v", r)
			}
		}()

		path := filepath.Join(t.TempDir(), "index.dat")
		if err := os.WriteFile(path, data, 0666); err != nil {
			t.Fatal(err)
		}
		_ = newSpatialIndex().load(path)
	})
}
