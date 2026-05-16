package blazedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
	"github.com/google/uuid"
)

// SegmentOptions configures a SegmentedProvider.
type SegmentOptions struct {
	// SegmentSize is the chunk width/depth of each segment. Values <= 0 use 32.
	SegmentSize int32
	// Options are used for each segment DB.
	Options *Options
}

// SegmentedProvider routes chunks to many smaller BlazeDB databases by chunk
// region. It is useful when arenas or map regions need to be compacted,
// discarded, backed up, or moved independently.
type SegmentedProvider struct {
	dir         string
	segmentDir  string
	segmentSize int32
	opts        *Options

	meta *DB

	mu       sync.Mutex
	segments map[segmentKey]*DB
}

type segmentKey struct {
	x, z  int32
	dimID int32
}

// OpenSegmentedProvider opens a provider that stores chunks in per-region DBs.
func OpenSegmentedProvider(dir string, opts *SegmentOptions) (*SegmentedProvider, error) {
	segmentSize := int32(32)
	var dbOpts *Options
	if opts != nil {
		if opts.SegmentSize > 0 {
			segmentSize = opts.SegmentSize
		}
		dbOpts = opts.Options
	}
	if dbOpts == nil {
		dbOpts = DefaultOptions()
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	meta, err := Config{Options: cloneOptions(dbOpts)}.Open(filepath.Join(dir, "_meta"))
	if err != nil {
		return nil, err
	}
	p := &SegmentedProvider{
		dir:         dir,
		segmentDir:  filepath.Join(dir, "segments"),
		segmentSize: segmentSize,
		opts:        cloneOptions(dbOpts),
		meta:        meta,
		segments:    make(map[segmentKey]*DB),
	}
	return p, nil
}

// Settings returns world settings stored in the provider metadata DB.
func (p *SegmentedProvider) Settings() *world.Settings {
	return p.meta.Settings()
}

// SaveSettings saves world settings to the provider metadata DB.
func (p *SegmentedProvider) SaveSettings(s *world.Settings) {
	p.meta.SaveSettings(s)
}

// LoadPlayerSpawnPosition loads a player spawn from the metadata DB.
func (p *SegmentedProvider) LoadPlayerSpawnPosition(id uuid.UUID) (cube.Pos, bool, error) {
	return p.meta.LoadPlayerSpawnPosition(id)
}

// SavePlayerSpawnPosition saves a player spawn to the metadata DB.
func (p *SegmentedProvider) SavePlayerSpawnPosition(id uuid.UUID, pos cube.Pos) error {
	return p.meta.SavePlayerSpawnPosition(id, pos)
}

// LoadColumn loads a column from its segment DB.
func (p *SegmentedProvider) LoadColumn(pos world.ChunkPos, dim world.Dimension) (*chunk.Column, error) {
	db, err := p.segment(pos, dim, false)
	if err != nil {
		return nil, err
	}
	return db.LoadColumn(pos, dim)
}

// StoreColumn stores a column in its segment DB.
func (p *SegmentedProvider) StoreColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) error {
	db, err := p.segment(pos, dim, true)
	if err != nil {
		return err
	}
	return db.StoreColumn(pos, dim, col)
}

// StoreBlockDeltas stores explicit block deltas in the matching segment DB.
func (p *SegmentedProvider) StoreBlockDeltas(pos world.ChunkPos, dim world.Dimension, deltas []BlockDelta) error {
	db, err := p.segment(pos, dim, false)
	if err != nil {
		return err
	}
	return db.StoreBlockDeltas(pos, dim, deltas)
}

// PinArea pins existing chunks in all touched segment DBs.
func (p *SegmentedProvider) PinArea(center world.ChunkPos, radius int, dim world.Dimension) (int, error) {
	pinned := 0
	for dx := -radius; dx <= radius; dx++ {
		for dz := -radius; dz <= radius; dz++ {
			pos := world.ChunkPos{center[0] + int32(dx), center[1] + int32(dz)}
			db, err := p.segment(pos, dim, false)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return pinned, err
			}
			if err := db.PinColumn(pos, dim); err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return pinned, err
			}
			pinned++
		}
	}
	return pinned, nil
}

// DeleteSegmentFor closes and removes the segment containing pos and dim. This
// is intended for disposable arena/map shards that are no longer needed.
func (p *SegmentedProvider) DeleteSegmentFor(pos world.ChunkPos, dim world.Dimension) error {
	return p.deleteSegment(p.segmentKey(pos, dim))
}

func (p *SegmentedProvider) deleteSegment(key segmentKey) error {
	p.mu.Lock()
	db := p.segments[key]
	delete(p.segments, key)
	p.mu.Unlock()

	if db != nil {
		if err := db.Close(); err != nil {
			return err
		}
	}
	return os.RemoveAll(p.segmentPath(key))
}

// Close closes all open segment DBs and metadata.
func (p *SegmentedProvider) Close() error {
	p.mu.Lock()
	segments := p.segments
	p.segments = make(map[segmentKey]*DB)
	p.mu.Unlock()

	var err error
	for _, db := range segments {
		if closeErr := db.Close(); err == nil {
			err = closeErr
		}
	}
	if p.meta != nil {
		if closeErr := p.meta.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (p *SegmentedProvider) segment(pos world.ChunkPos, dim world.Dimension, create bool) (*DB, error) {
	key := p.segmentKey(pos, dim)

	p.mu.Lock()
	defer p.mu.Unlock()

	if db, ok := p.segments[key]; ok {
		return db, nil
	}
	path := p.segmentPath(key)
	if !create && !fileExists(path) {
		return nil, ErrNotFound
	}
	db, err := Config{Options: cloneOptions(p.opts)}.Open(path)
	if err != nil {
		return nil, err
	}
	p.segments[key] = db
	return db, nil
}

func (p *SegmentedProvider) segmentKey(pos world.ChunkPos, dim world.Dimension) segmentKey {
	dimID, _ := world.DimensionID(dim)
	return segmentKey{
		x:     floorDiv32(pos[0], p.segmentSize),
		z:     floorDiv32(pos[1], p.segmentSize),
		dimID: int32(dimID),
	}
}

func (p *SegmentedProvider) segmentPath(key segmentKey) string {
	return filepath.Join(p.segmentDir, fmt.Sprintf("dim_%d_x_%d_z_%d", key.dimID, key.x, key.z))
}

func floorDiv32(v, size int32) int32 {
	if v >= 0 {
		return v / size
	}
	return -((-v + size - 1) / size)
}
