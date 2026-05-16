package blazedb

import (
	"fmt"

	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/chunk"
)

func cloneColumn(col *chunk.Column, dim world.Dimension) (*chunk.Column, error) {
	if col == nil || col.Chunk == nil {
		return nil, fmt.Errorf("clone column: nil chunk column")
	}
	data := chunk.Encode(col.Chunk, chunk.DiskEncoding)
	c, err := chunk.DiskDecode(data, dim.Range())
	if err != nil {
		return nil, fmt.Errorf("clone column: %w", err)
	}
	clone := &chunk.Column{
		Chunk:           c,
		Entities:        cloneEntities(col.Entities),
		BlockEntities:   cloneBlockEntities(col.BlockEntities),
		Tick:            col.Tick,
		ScheduledBlocks: append([]chunk.ScheduledBlockUpdate(nil), col.ScheduledBlocks...),
	}
	return clone, nil
}

func cloneEntities(in []chunk.Entity) []chunk.Entity {
	if len(in) == 0 {
		return nil
	}
	out := make([]chunk.Entity, len(in))
	for i, entity := range in {
		out[i] = chunk.Entity{
			ID:   entity.ID,
			Data: cloneMap(entity.Data),
		}
	}
	return out
}

func cloneBlockEntities(in []chunk.BlockEntity) []chunk.BlockEntity {
	if len(in) == 0 {
		return nil
	}
	out := make([]chunk.BlockEntity, len(in))
	for i, blockEntity := range in {
		out[i] = chunk.BlockEntity{
			Pos:  blockEntity.Pos,
			Data: cloneMap(blockEntity.Data),
		}
	}
	return out
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
