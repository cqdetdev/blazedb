package blazedb

import (
	"hash/crc32"
	"sync"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

// Buffer pools for zero-allocation encoding/decoding
var (
	// Small buffer pool for chunks (64KB)
	smallBufPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 64*1024)
			return &buf
		},
	}

	// Large buffer pool for compression (256KB)
	largeBufPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 256*1024)
			return &buf
		},
	}
)

// getSmallBuf gets a buffer from the small pool.
func getSmallBuf() *[]byte {
	return smallBufPool.Get().(*[]byte)
}

// putSmallBuf returns a buffer to the small pool.
func putSmallBuf(buf *[]byte) {
	smallBufPool.Put(buf)
}

// getLargeBuf gets a buffer from the large pool.
func getLargeBuf() *[]byte {
	return largeBufPool.Get().(*[]byte)
}

// putLargeBuf returns a buffer to the large pool.
func putLargeBuf(buf *[]byte) {
	largeBufPool.Put(buf)
}

// compress compresses data using the specified algorithm.
func compress(data []byte, compression CompressionType) []byte {
	if data == nil || len(data) == 0 {
		return nil
	}

	switch compression {
	case CompressionNone:
		return data
	case CompressionLZ4:
		return compressLZ4(data)
	case CompressionSnappy:
		return snappy.Encode(nil, data)
	default:
		return data
	}
}

// decompress decompresses data using the specified algorithm.
func decompress(data []byte, compression CompressionType) []byte {
	if data == nil || len(data) == 0 {
		return nil
	}

	switch compression {
	case CompressionNone:
		return data
	case CompressionLZ4:
		return decompressLZ4(data)
	case CompressionSnappy:
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return data // Return original if decompression fails
		}
		return decoded
	default:
		return data
	}
}

// compressLZ4 compresses data using LZ4 (fastest algorithm).
// Format: [4 bytes uncompressed size][compressed data]
func compressLZ4(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}

	// LZ4 compressed size is at most slightly larger than source
	dst := make([]byte, 4+lz4.CompressBlockBound(len(data)))

	// Store uncompressed size in first 4 bytes (little-endian)
	dst[0] = byte(len(data))
	dst[1] = byte(len(data) >> 8)
	dst[2] = byte(len(data) >> 16)
	dst[3] = byte(len(data) >> 24)

	n, err := lz4.CompressBlock(data, dst[4:], nil)
	if err != nil || n == 0 {
		// Compression failed or data is incompressible, return original with size prefix
		result := make([]byte, 4+len(data))
		result[0] = byte(len(data))
		result[1] = byte(len(data) >> 8)
		result[2] = byte(len(data) >> 16)
		result[3] = byte(len(data) >> 24)
		copy(result[4:], data)
		return result
	}
	return dst[:4+n]
}

// decompressLZ4 decompresses LZ4 data.
// Format: [4 bytes uncompressed size][compressed data]
func decompressLZ4(data []byte) []byte {
	if len(data) < 4 {
		return data
	}

	// Read uncompressed size from first 4 bytes
	uncompressedSize := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24

	if uncompressedSize <= 0 || uncompressedSize > 64*1024*1024 {
		// Invalid size, return as-is
		return data
	}

	dst := make([]byte, uncompressedSize)
	n, err := lz4.UncompressBlock(data[4:], dst)
	if err != nil || n != uncompressedSize {
		// Decompression failed, data might be uncompressed with size prefix
		if len(data)-4 == uncompressedSize {
			return data[4:]
		}
		return data
	}
	return dst[:n]
}

// computeCRC32 computes a CRC32 checksum of the data.
func computeCRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// rleEncode performs run-length encoding on block data.
// This is especially effective for Minecraft chunks where large areas
// are filled with the same block (air, stone, dirt, etc.)
//
// Format: [blockID:2][count:2] repeated
// Returns nil if RLE would be larger than original.
func rleEncode(blocks []uint16) []byte {
	if len(blocks) == 0 {
		return nil
	}

	result := make([]byte, 0, len(blocks)) // Pre-allocate
	current := blocks[0]
	count := uint16(1)

	for i := 1; i < len(blocks); i++ {
		if blocks[i] == current && count < 65535 {
			count++
		} else {
			// Write run
			result = append(result, byte(current), byte(current>>8), byte(count), byte(count>>8))
			current = blocks[i]
			count = 1
		}
	}
	// Write final run
	result = append(result, byte(current), byte(current>>8), byte(count), byte(count>>8))

	// Only use RLE if it's smaller
	if len(result) >= len(blocks)*2 {
		return nil
	}

	return result
}

// rleDecode decodes run-length encoded block data.
func rleDecode(data []byte, expectedLen int) []uint16 {
	if len(data) == 0 {
		return nil
	}

	result := make([]uint16, 0, expectedLen)

	for i := 0; i+3 < len(data); i += 4 {
		blockID := uint16(data[i]) | uint16(data[i+1])<<8
		count := uint16(data[i+2]) | uint16(data[i+3])<<8
		for j := uint16(0); j < count; j++ {
			result = append(result, blockID)
		}
	}

	return result
}

// paletteCompress creates a palette-based compression of blocks.
// This is similar to Minecraft's native format but optimized for storage.
type blockPalette struct {
	palette []uint16          // Unique block IDs
	lookup  map[uint16]uint16 // blockID -> palette index
}

// newBlockPalette creates a new block palette.
func newBlockPalette() *blockPalette {
	return &blockPalette{
		palette: make([]uint16, 0, 16),
		lookup:  make(map[uint16]uint16, 16),
	}
}

// add adds a block ID to the palette and returns its index.
func (p *blockPalette) add(blockID uint16) uint16 {
	if idx, ok := p.lookup[blockID]; ok {
		return idx
	}
	idx := uint16(len(p.palette))
	p.palette = append(p.palette, blockID)
	p.lookup[blockID] = idx
	return idx
}

// get returns the block ID for a palette index.
func (p *blockPalette) get(idx uint16) uint16 {
	if int(idx) < len(p.palette) {
		return p.palette[idx]
	}
	return 0
}

// size returns the number of entries in the palette.
func (p *blockPalette) size() int {
	return len(p.palette)
}
