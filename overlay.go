package blazedb

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
	"github.com/google/uuid"
)

// OverlayOptions controls OverlayProvider lifecycle behaviour.
type OverlayOptions struct {
	// CloseBase closes the template/base provider when the overlay provider is
	// closed. The default is false so one template DB can be shared by many
	// arenas.
	CloseBase bool
	// UnsafeShareBaseColumns skips cloning base/template columns before
	// returning them. Leave this false unless the caller guarantees loaded base
	// columns will never be mutated.
	UnsafeShareBaseColumns bool
}

// OverlayProvider writes all mutations to an overlay provider and falls back
// to a read-only base provider for missing chunks. It is designed for
// disposable maps where reset should be discarding the overlay rather than
// copying a whole map.
type OverlayProvider struct {
	base    world.Provider
	overlay world.Provider

	opts OverlayOptions

	overlayDir     string
	overlayDBOpts  *Options
	overlayResetMu sync.Mutex
}

type deltaProvider interface {
	StoreBlockDeltas(world.ChunkPos, world.Dimension, []BlockDelta) error
}

// NewOverlayProvider wraps an existing base/template provider and overlay
// provider. The overlay receives all stores; reads hit overlay first and base
// second.
func NewOverlayProvider(base, overlay world.Provider, opts *OverlayOptions) *OverlayProvider {
	var o OverlayOptions
	if opts != nil {
		o = *opts
	}
	return &OverlayProvider{base: base, overlay: overlay, opts: o}
}

// OpenOverlayProvider creates a BlazeDB overlay at overlayDir and wraps it
// around base. ResetOverlay may be used to discard the overlay and reopen an
// empty one.
func OpenOverlayProvider(base world.Provider, overlayDir string, dbOpts *Options, opts *OverlayOptions) (*OverlayProvider, error) {
	if dbOpts == nil {
		dbOpts = DefaultOptions()
	}
	overlay, err := Config{Options: cloneOptions(dbOpts)}.Open(overlayDir)
	if err != nil {
		return nil, err
	}
	overlay.SaveSettings(base.Settings())
	p := NewOverlayProvider(base, overlay, opts)
	p.overlayDir = overlayDir
	p.overlayDBOpts = cloneOptions(dbOpts)
	return p, nil
}

// Settings returns overlay settings. If the overlay has no custom settings,
// callers should initialise it from the base provider before opening a world.
func (p *OverlayProvider) Settings() *world.Settings {
	return p.overlay.Settings()
}

// SaveSettings saves settings to the overlay provider.
func (p *OverlayProvider) SaveSettings(s *world.Settings) {
	p.overlay.SaveSettings(s)
}

// LoadPlayerSpawnPosition loads a player spawn from the overlay first, then
// falls back to the base provider.
func (p *OverlayProvider) LoadPlayerSpawnPosition(id uuid.UUID) (cube.Pos, bool, error) {
	pos, ok, err := p.overlay.LoadPlayerSpawnPosition(id)
	if err != nil {
		return cube.Pos{}, false, err
	}
	if ok {
		return pos, true, nil
	}
	return p.base.LoadPlayerSpawnPosition(id)
}

// SavePlayerSpawnPosition saves a player spawn to the overlay.
func (p *OverlayProvider) SavePlayerSpawnPosition(id uuid.UUID, pos cube.Pos) error {
	return p.overlay.SavePlayerSpawnPosition(id, pos)
}

// LoadColumn loads a column from the overlay first, then from the base. Base
// columns are cloned by default so template chunks are not mutated in place.
func (p *OverlayProvider) LoadColumn(pos world.ChunkPos, dim world.Dimension) (*chunk.Column, error) {
	col, err := p.overlay.LoadColumn(pos, dim)
	if err == nil {
		return col, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	col, err = p.base.LoadColumn(pos, dim)
	if err != nil {
		return nil, err
	}
	if p.opts.UnsafeShareBaseColumns {
		return col, nil
	}
	return cloneColumn(col, dim)
}

// StoreColumn stores a full column into the overlay provider.
func (p *OverlayProvider) StoreColumn(pos world.ChunkPos, dim world.Dimension, col *chunk.Column) error {
	return p.overlay.StoreColumn(pos, dim, col)
}

// StoreBlockDeltas applies small block edits to the overlay. If the overlay
// already has the column and supports delta records, the delta record path is
// used. Otherwise the base column is cloned once, patched, and stored into the
// overlay as the arena's first divergent version.
func (p *OverlayProvider) StoreBlockDeltas(pos world.ChunkPos, dim world.Dimension, deltas []BlockDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	if _, err := p.overlay.LoadColumn(pos, dim); err == nil {
		if deltaOverlay, ok := p.overlay.(deltaProvider); ok {
			if err := deltaOverlay.StoreBlockDeltas(pos, dim, deltas); err == nil {
				return nil
			} else if !errors.Is(err, ErrDeltaRecordsDisabled) {
				return err
			}
		}
		col, err := p.overlay.LoadColumn(pos, dim)
		if err != nil {
			return err
		}
		if err := applyBlockDeltasToColumn(col, deltas); err != nil {
			return err
		}
		return p.overlay.StoreColumn(pos, dim, col)
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}

	baseCol, err := p.base.LoadColumn(pos, dim)
	if err != nil {
		return err
	}
	col, err := cloneColumn(baseCol, dim)
	if err != nil {
		return err
	}
	if err := applyBlockDeltasToColumn(col, deltas); err != nil {
		return err
	}
	return p.overlay.StoreColumn(pos, dim, col)
}

// ResetOverlay closes, removes, and recreates the overlay DB created by
// OpenOverlayProvider. It leaves the base/template provider open.
func (p *OverlayProvider) ResetOverlay() error {
	p.overlayResetMu.Lock()
	defer p.overlayResetMu.Unlock()

	if p.overlayDir == "" || p.overlayDBOpts == nil {
		return errors.New("reset overlay: provider was not opened with OpenOverlayProvider")
	}
	if err := p.overlay.Close(); err != nil {
		return fmt.Errorf("reset overlay: close overlay: %w", err)
	}
	if err := os.RemoveAll(p.overlayDir); err != nil {
		return fmt.Errorf("reset overlay: remove overlay: %w", err)
	}
	overlay, err := Config{Options: cloneOptions(p.overlayDBOpts)}.Open(p.overlayDir)
	if err != nil {
		return fmt.Errorf("reset overlay: reopen overlay: %w", err)
	}
	overlay.SaveSettings(p.base.Settings())
	p.overlay = overlay
	return nil
}

// Close closes the overlay provider and optionally the base provider.
func (p *OverlayProvider) Close() error {
	var err error
	if p.overlay != nil {
		err = p.overlay.Close()
	}
	if p.opts.CloseBase && p.base != nil {
		if baseErr := p.base.Close(); err == nil {
			err = baseErr
		}
	}
	return err
}
