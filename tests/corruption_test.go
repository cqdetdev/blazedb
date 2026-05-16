package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

func recoveryTestOptions() *blazedb.Options {
	opts := blazedb.DefaultOptions()
	opts.WriteBufferSize = 0
	opts.FlushInterval = 0
	opts.VerifyChecksums = true
	opts.Log = discardLogger()
	return opts
}

func recoveryTestColumn() *chunk.Column {
	return recoveryTestColumnForDim(world.Overworld)
}

func recoveryTestColumnForDim(dim world.Dimension) *chunk.Column {
	return &chunk.Column{Chunk: chunk.New(0, dim.Range())}
}

func recoveryTestColumnWithEntity(id int64) *chunk.Column {
	return recoveryTestColumnWithEntityForDim(world.Overworld, id)
}

func recoveryTestColumnWithEntityForDim(dim world.Dimension, id int64) *chunk.Column {
	return &chunk.Column{
		Chunk: chunk.New(0, dim.Range()),
		Entities: []chunk.Entity{{
			ID:   id,
			Data: map[string]any{"identifier": "blazedb:test"},
		}},
	}
}

func openRecoveryDB(t *testing.T, dir string) *blazedb.DB {
	t.Helper()

	db, err := blazedb.Config{Options: recoveryTestOptions()}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func dataPath(dir string) string {
	return filepath.Join(dir, "chunks.dat")
}

func indexPath(dir string) string {
	return filepath.Join(dir, "index.dat")
}

func dataSize(t *testing.T, dir string) int64 {
	t.Helper()

	info, err := os.Stat(dataPath(dir))
	if os.IsNotExist(err) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func storeRecoveryChunk(t *testing.T, db *blazedb.DB, dir string, pos world.ChunkPos) (offset, size int64) {
	t.Helper()
	return storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumn())
}

func storeRecoveryColumn(t *testing.T, db *blazedb.DB, dir string, pos world.ChunkPos, dim world.Dimension, col *chunk.Column) (offset, size int64) {
	t.Helper()

	before := dataSize(t, dir)
	if err := db.StoreColumn(pos, dim, col); err != nil {
		t.Fatal(err)
	}
	after := dataSize(t, dir)
	if after < before {
		t.Fatalf("chunks.dat shrank after store: before=%d after=%d", before, after)
	}
	return before, after - before
}

func encodedRecord(t *testing.T, pos world.ChunkPos, dim world.Dimension, col *chunk.Column) []byte {
	t.Helper()

	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	if err := db.StoreColumn(pos, dim, col); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dataPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func loadEntityID(t *testing.T, db *blazedb.DB, pos world.ChunkPos, dim world.Dimension) int64 {
	t.Helper()

	col, err := db.LoadColumn(pos, dim)
	if err != nil {
		t.Fatalf("load chunk %v in %T: %v", pos, dim, err)
	}
	if len(col.Entities) == 0 {
		t.Fatalf("chunk %v had no entities", pos)
	}
	return col.Entities[0].ID
}

func TestRebuildIndexWhenIndexDeleted(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	positions := []world.ChunkPos{{0, 0}, {-32, 16}, {64, -64}}
	for _, pos := range positions {
		storeRecoveryChunk(t, db, dir, pos)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(indexPath(dir)); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()

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
		storeRecoveryChunk(t, db, dir, pos)
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

	firstOffset, _ := storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(1))
	latestOffset, _ := storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(2))
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

	if got := loadEntityID(t, db, pos, world.Overworld); got != 2 {
		t.Fatalf("rebuilt index did not keep latest version: got entity %d", got)
	}
}

func TestRebuildIndexSkipsBadChecksumRecord(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	pos := world.ChunkPos{-7, 12}

	storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(1))
	latestOffset, latestSize := storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(2))
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

	if got := loadEntityID(t, db, pos, world.Overworld); got != 1 {
		t.Fatalf("expected corrupt latest record to be skipped: got entity %d", got)
	}
}

func TestRebuildIndexTruncatesPartialTailWrite(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	pos := world.ChunkPos{2, 3}
	storeRecoveryChunk(t, db, dir, pos)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	before := dataSize(t, dir)
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

	after := dataSize(t, dir)
	if after != before {
		t.Fatalf("expected partial tail to be truncated to %d bytes, got %d", before, after)
	}
	if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
		t.Fatalf("load chunk after partial tail truncation: %v", err)
	}
}

func TestOpenRebuildsValidButStaleIndex(t *testing.T) {
	dir := t.TempDir()
	db := openRecoveryDB(t, dir)
	storeRecoveryChunk(t, db, dir, world.ChunkPos{0, 0})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	laterPos := world.ChunkPos{9, -9}
	laterRecord := encodedRecord(t, laterPos, world.Overworld, recoveryTestColumn())
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
	opts := blazedb.DefaultOptions()
	opts.Durability = blazedb.DurabilitySafe
	opts.WriteBufferSize = 4 * 1024 * 1024
	opts.FlushInterval = 1000
	opts.Log = discardLogger()

	dir := t.TempDir()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.StoreColumn(world.ChunkPos{1, 1}, world.Overworld, recoveryTestColumn()); err != nil {
		t.Fatal(err)
	}
	if size := dataSize(t, dir); size == 0 {
		t.Fatal("safe durability should write immediately instead of leaving data only in the write buffer")
	}
}

func TestStoreColumnUnchangedImmediateSkipsAppend(t *testing.T) {
	opts := recoveryTestOptions()
	opts.Durability = blazedb.DurabilityFast

	dir := t.TempDir()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{3, -5}
	col := recoveryTestColumn()
	if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}
	before := dataSize(t, dir)
	for i := 0; i < 5; i++ {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			t.Fatal(err)
		}
	}
	after := dataSize(t, dir)
	if after != before {
		t.Fatalf("unchanged writes should not append: before=%d after=%d", before, after)
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 5 {
		t.Fatalf("expected 5 unchanged write skips, got %d", skips)
	}
}

func TestStoreColumnUnchangedSafeBatchSkipsAppend(t *testing.T) {
	opts := blazedb.SafeBatchOptions()
	opts.Log = discardLogger()

	dir := t.TempDir()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{-9, 4}
	col := recoveryTestColumn()
	if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
		t.Fatal(err)
	}
	before := dataSize(t, dir)
	for i := 0; i < 3; i++ {
		if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
			t.Fatal(err)
		}
	}
	after := dataSize(t, dir)
	if after != before {
		t.Fatalf("unchanged safe-batch writes should not append: before=%d after=%d", before, after)
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 3 {
		t.Fatalf("expected 3 unchanged write skips, got %d", skips)
	}
}

func TestStoreColumnChangedStillAppends(t *testing.T) {
	opts := recoveryTestOptions()
	opts.Durability = blazedb.DurabilityFast

	dir := t.TempDir()
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pos := world.ChunkPos{6, 6}
	firstOffset, _ := storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(1))
	secondOffset, _ := storeRecoveryColumn(t, db, dir, pos, world.Overworld, recoveryTestColumnWithEntity(2))
	if secondOffset <= firstOffset {
		t.Fatalf("changed write should append after first record: first=%d second=%d", firstOffset, secondOffset)
	}
	if skips := db.GetStats().ChunkWriteSkips; skips != 0 {
		t.Fatalf("changed writes should not be counted as skips, got %d", skips)
	}
}

func TestBufferedWritesPersistOnClose(t *testing.T) {
	opts := blazedb.DefaultOptions()
	opts.WriteBufferSize = 4 * 1024 * 1024
	opts.FlushInterval = 0
	opts.Log = discardLogger()

	dir := t.TempDir()
	pos := world.ChunkPos{11, -3}
	db, err := blazedb.Config{Options: opts}.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.StoreColumn(pos, world.Overworld, recoveryTestColumn()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = openRecoveryDB(t, dir)
	defer db.Close()
	if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
		t.Fatalf("load buffered chunk after close: %v", err)
	}
}

func FuzzDecodeColumnRejectsCorruption(f *testing.F) {
	valid := encodedRecordForFuzz(f)
	f.Add([]byte{})
	f.Add([]byte("not a chunk"))
	f.Add(valid)

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(dataPath(dir), data, 0666); err != nil {
			t.Fatal(err)
		}
		db, err := blazedb.Config{Options: recoveryTestOptions()}.Open(dir)
		if err == nil {
			_ = db.Close()
		}
	})
}

func encodedRecordForFuzz(f *testing.F) []byte {
	dir := f.TempDir()
	db, err := blazedb.Config{Options: recoveryTestOptions()}.Open(dir)
	if err != nil {
		f.Fatal(err)
	}
	if err := db.StoreColumn(world.ChunkPos{1, -1}, world.Overworld, recoveryTestColumn()); err != nil {
		f.Fatal(err)
	}
	if err := db.Close(); err != nil {
		f.Fatal(err)
	}
	valid, err := os.ReadFile(dataPath(dir))
	if err != nil {
		f.Fatal(err)
	}
	return valid
}

func FuzzSpatialIndexLoadRejectsCorruption(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("not an index"))
	f.Add([]byte{'B', 'I', 'D', 'X', 0, 0, 0, 0, 0, 0, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		if err := os.WriteFile(indexPath(dir), data, 0666); err != nil {
			t.Fatal(err)
		}
		db, err := blazedb.Config{Options: recoveryTestOptions()}.Open(dir)
		if err == nil {
			_ = db.Close()
		}
	})
}
