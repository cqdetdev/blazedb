package blazedb

import (
	"hash/crc32"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

const lz4RawBlockFlag = uint32(1 << 31)

func compressedBound(size int, compression CompressionType) int {
	if size <= 0 {
		return 0
	}
	switch compression {
	case CompressionLZ4:
		return 4 + lz4.CompressBlockBound(size)
	case CompressionSnappy:
		return snappy.MaxEncodedLen(size)
	default:
		return size
	}
}

func appendCompressed(dst, data []byte, compression CompressionType) []byte {
	lengthPos := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	start := len(dst)

	if len(data) > 0 {
		switch compression {
		case CompressionLZ4:
			dst = appendLZ4(dst, data)
		case CompressionSnappy:
			start := len(dst)
			dst = append(dst, make([]byte, snappy.MaxEncodedLen(len(data)))...)
			encoded := snappy.Encode(dst[start:], data)
			dst = dst[:start+len(encoded)]
		default:
			dst = append(dst, data...)
		}
	}

	writeLZ4Size(dst[lengthPos:], uint32(len(dst)-start))
	return dst
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

func appendLZ4(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	start := len(dst)
	dst = append(dst, make([]byte, 4+lz4.CompressBlockBound(len(data)))...)
	writeLZ4Size(dst[start:], uint32(len(data)))

	n, err := lz4.CompressBlock(data, dst[start+4:], nil)
	if err != nil || n == 0 {
		dst = dst[:start+4+len(data)]
		writeLZ4Size(dst[start:], uint32(len(data))|lz4RawBlockFlag)
		copy(dst[start+4:], data)
		return dst
	}
	return dst[:start+4+n]
}

func writeLZ4Size(dst []byte, size uint32) {
	dst[0] = byte(size)
	dst[1] = byte(size >> 8)
	dst[2] = byte(size >> 16)
	dst[3] = byte(size >> 24)
}

// decompressLZ4 decompresses LZ4 data.
// Format: [4 bytes uncompressed size][compressed data]
func decompressLZ4(data []byte) []byte {
	if len(data) < 4 {
		return data
	}

	// Read uncompressed size from first 4 bytes
	sizeField := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	if sizeField&lz4RawBlockFlag != 0 {
		uncompressedSize := int(sizeField &^ lz4RawBlockFlag)
		if len(data)-4 == uncompressedSize {
			return data[4:]
		}
		return data
	}

	uncompressedSize := int(sizeField)

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
