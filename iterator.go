package blazedb

import (
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

// ColumnIterator iterates over a DB's position/column pairs.
//
// When an error is encountered, any call to Next will return false and will
// yield no position/chunk pairs. The error can be queried by calling the Error
// method. Calling Release is still necessary.
//
// An iterator must be released after use, but it is not necessary to read
// an iterator until exhaustion.
type ColumnIterator struct {
	db      *DB
	r       *IteratorRange
	err     error
	keys    []chunkKey
	current int
	col     *chunk.Column
	pos     world.ChunkPos
	dim     world.Dimension
}

// newColumnIterator creates a new column iterator.
func newColumnIterator(db *DB, r *IteratorRange) *ColumnIterator {
	iter := &ColumnIterator{
		db:      db,
		r:       r,
		keys:    make([]chunkKey, 0),
		current: -1,
	}

	// Collect all keys that match the range
	db.index.iterate(func(key chunkKey, offset, size int64) bool {
		if r.within(key.pos, key.dim) {
			iter.keys = append(iter.keys, key)
		}
		return true
	})

	return iter
}

// Next moves the iterator to the next key/value pair.
// It returns false if the iterator is exhausted.
func (iter *ColumnIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	iter.current++
	if iter.current >= len(iter.keys) {
		iter.col = nil
		iter.dim = nil
		return false
	}

	key := iter.keys[iter.current]
	iter.pos = key.pos
	iter.dim = key.dim

	var err error
	iter.col, err = iter.db.LoadColumn(iter.pos, iter.dim)
	if err != nil {
		iter.err = err
		return false
	}

	return true
}

// Column returns the current chunk column, or nil if none.
func (iter *ColumnIterator) Column() *chunk.Column {
	return iter.col
}

// Position returns the position of the current column.
func (iter *ColumnIterator) Position() world.ChunkPos {
	return iter.pos
}

// Dimension returns the dimension of the current column, or nil if none.
func (iter *ColumnIterator) Dimension() world.Dimension {
	return iter.dim
}

// Release releases resources associated with the iterator.
func (iter *ColumnIterator) Release() {
	iter.keys = nil
	iter.col = nil
}

// Error returns any accumulated error.
func (iter *ColumnIterator) Error() error {
	return iter.err
}

// IteratorRange limits what columns are returned by a ColumnIterator.
type IteratorRange struct {
	// Min and Max limit what chunk positions are returned.
	// A zero value for both causes all positions to be within range.
	Min, Max world.ChunkPos
	// Dimension specifies what dimension chunks should be from.
	// If nil, all dimensions are included.
	Dimension world.Dimension
}

// within checks if a position and dimension is within the IteratorRange.
func (r *IteratorRange) within(pos world.ChunkPos, dim world.Dimension) bool {
	if dim != r.Dimension && r.Dimension != nil {
		return false
	}
	return ((r.Min == world.ChunkPos{}) && (r.Max == world.ChunkPos{})) ||
		pos[0] >= r.Min[0] && pos[0] < r.Max[0] && pos[1] >= r.Min[1] && pos[1] < r.Max[1]
}
