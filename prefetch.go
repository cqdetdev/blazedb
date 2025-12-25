package blazedb

import (
	"sync"
	"sync/atomic"

	"github.com/df-mc/dragonfly/server/world"
	"github.com/google/uuid"
)

// Prefetcher predicts and preloads chunks based on player movement.
type Prefetcher struct {
	db         *DB
	players    sync.Map // map[uuid.UUID]*playerTracker
	prefetchCh chan prefetchRequest
	workers    int
	enabled    atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// playerTracker tracks movement data for a single player.
type playerTracker struct {
	mu       sync.Mutex
	lastPos  world.ChunkPos
	velocity [2]int32 // Movement velocity (dx, dz)
}

type prefetchRequest struct {
	pos world.ChunkPos
	dim world.Dimension
}

// NewPrefetcher creates a new predictive prefetcher.
func NewPrefetcher(db *DB, workers int) *Prefetcher {
	if workers <= 0 {
		workers = 2 // Default 2 prefetch workers
	}
	p := &Prefetcher{
		db:         db,
		prefetchCh: make(chan prefetchRequest, 128), // Increased buffer size
		workers:    workers,
		stopCh:     make(chan struct{}),
	}
	p.enabled.Store(true)
	p.start()
	return p
}

// start launches prefetch workers.
func (p *Prefetcher) start() {
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// worker processes prefetch requests.
func (p *Prefetcher) worker() {
	defer p.wg.Done()
	for {
		select {
		case req := <-p.prefetchCh:
			if p.enabled.Load() {
				// Try to load chunk into cache (ignore errors for prefetch)
				_, _ = p.db.LoadColumn(req.pos, req.dim)
			}
		case <-p.stopCh:
			return
		}
	}
}

// UpdatePlayerPosition tracks player position and triggers prefetch.
func (p *Prefetcher) UpdatePlayerPosition(id uuid.UUID, pos world.ChunkPos, dim world.Dimension) {
	if !p.enabled.Load() {
		return
	}

	// Load or create tracker (lock-free read/insert)
	val, ok := p.players.Load(id)
	if !ok {
		val, _ = p.players.LoadOrStore(id, &playerTracker{
			lastPos: pos,
		})
	}
	tracker := val.(*playerTracker)

	// Fine-grained lock on just this player
	tracker.mu.Lock()

	// Calculate velocity
	dx := pos[0] - tracker.lastPos[0]
	dz := pos[1] - tracker.lastPos[1]

	// Smooth velocity with previous value
	oldVel := tracker.velocity
	tracker.velocity = [2]int32{
		(oldVel[0] + dx) / 2,
		(oldVel[1] + dz) / 2,
	}

	tracker.lastPos = pos
	vel := tracker.velocity
	tracker.mu.Unlock()

	// Only prefetch if player is moving efficiently
	if vel[0] == 0 && vel[1] == 0 {
		return
	}

	// Predict and prefetch next chunks
	// We use a fixed-size array to avoid allocations in predictNext
	predictions := p.predictNext(pos, vel)

	for _, pred := range predictions {
		// Zero value check (fixed size array may have empty slots if we changed impl,
		// but current predictNext fills all needed or returns slice of it)
		// actually predictNext returns slice backed by array potentially or just slice.
		// Let's optimize predictNext to act on a passed callback or return small stable slice.

		select {
		case p.prefetchCh <- prefetchRequest{pos: pred, dim: dim}:
			// Queued
		default:
			// Full, drop to avoid blocking tick
		}
	}
}

// predictNext predicts the next chunks to load based on position and velocity.
func (p *Prefetcher) predictNext(pos world.ChunkPos, vel [2]int32) []world.ChunkPos {
	// Pre-allocate slice with capacity to avoid resize, max 5 predictions
	predictions := make([]world.ChunkPos, 0, 5)

	// Normalize velocity to unit direction [-1, 0, 1]
	dx, dz := int32(0), int32(0)
	if vel[0] > 0 {
		dx = 1
	} else if vel[0] < 0 {
		dx = -1
	}
	if vel[1] > 0 {
		dz = 1
	} else if vel[1] < 0 {
		dz = -1
	}

	// If no directional movement after normalization (shouldn't happen if check passed), return empty
	if dx == 0 && dz == 0 {
		return predictions
	}

	// Predict 1-3 chunks ahead
	for i := int32(1); i <= 3; i++ {
		predictions = append(predictions, world.ChunkPos{
			pos[0] + dx*i,
			pos[1] + dz*i,
		})
	}

	// Add cone/diagonal predictions if moving diagonally
	if dx != 0 && dz != 0 {
		predictions = append(predictions,
			world.ChunkPos{pos[0] + dx*2, pos[1] + dz},
			world.ChunkPos{pos[0] + dx, pos[1] + dz*2},
		)
	}

	return predictions
}

// RemovePlayer removes a player from tracking.
func (p *Prefetcher) RemovePlayer(id uuid.UUID) {
	p.players.Delete(id)
}

// SetEnabled enables or disables prefetching.
func (p *Prefetcher) SetEnabled(enabled bool) {
	p.enabled.Store(enabled)
}

// Stop stops all prefetch workers.
func (p *Prefetcher) Stop() {
	p.enabled.Store(false)
	close(p.stopCh)
	p.wg.Wait()
}

// Stats returns prefetch statistics.
func (p *Prefetcher) Stats() (tracked int, queueLen int) {
	// Count items in sync.Map (O(N) but stats is rarely called)
	p.players.Range(func(_, _ interface{}) bool {
		tracked++
		return true
	})
	queueLen = len(p.prefetchCh)
	return
}
