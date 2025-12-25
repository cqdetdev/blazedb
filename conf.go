package blazedb

import (
	"log/slog"
	"os"
	"path/filepath"
)

// CompressionType specifies the compression algorithm to use.
type CompressionType int

const (
	// CompressionNone disables compression.
	CompressionNone CompressionType = iota
	// CompressionSnappy uses Snappy compression (balanced speed/ratio).
	CompressionSnappy
	// CompressionLZ4 uses LZ4 compression (fastest).
	CompressionLZ4
)

// Options holds configuration options for a BlazeDB database.
type Options struct {
	// CacheSize is the maximum number of bytes to use for in-memory caching.
	// Defaults to 256MB. Higher values improve performance but use more RAM.
	CacheSize int64

	// Compression specifies the compression algorithm to use.
	// Defaults to CompressionLZ4 for maximum speed.
	Compression CompressionType

	// VerifyChecksums enables CRC32 checksum verification on reads.
	// Defaults to false for maximum speed. Enable for data integrity checks.
	VerifyChecksums bool

	// WriteBufferSize is the maximum size of the write buffer before flushing.
	// Defaults to 4MB. Larger buffers improve write throughput but delay persistence.
	WriteBufferSize int64

	// FlushInterval is how often the write buffer is flushed in milliseconds.
	// Defaults to 1000ms (1 second). Set to 0 for immediate writes.
	FlushInterval int

	// ParallelCompression enables parallel compression using goroutines.
	// Defaults to true.
	ParallelCompression bool

	// AutoTuneCache enables automatic cache size adjustment based on hit rate.
	// Defaults to true.
	AutoTuneCache bool

	// AutoTuneInterval is how often to check cache hit rate (in seconds).
	// Defaults to 30 seconds.
	AutoTuneInterval int

	// CacheMinSize is the minimum cache size for auto-tuning.
	// Defaults to 64MB.
	CacheMinSize int64

	// CacheMaxSize is the maximum cache size for auto-tuning.
	// Defaults to 1GB.
	CacheMaxSize int64

	// Log is the Logger to use for debug messages and errors.
	// If nil, defaults to slog.Default().
	Log *slog.Logger
}

// DefaultOptions returns the recommended default options for BlazeDB.
// Optimized for maximum write performance.
func DefaultOptions() *Options {
	return &Options{
		CacheSize:           256 * 1024 * 1024, // 256MB
		Compression:         CompressionLZ4,    // Fastest
		VerifyChecksums:     false,             // Skip for speed
		WriteBufferSize:     4 * 1024 * 1024,   // 4MB buffer
		FlushInterval:       1000,              // 1 second
		ParallelCompression: true,
		AutoTuneCache:       true,
		AutoTuneInterval:    30,                 // 30 seconds
		CacheMinSize:        64 * 1024 * 1024,   // 64MB min
		CacheMaxSize:        1024 * 1024 * 1024, // 1GB max
		Log:                 slog.Default(),
	}
}

// BalancedOptions returns options balanced between speed and safety.
func BalancedOptions() *Options {
	return &Options{
		CacheSize:           256 * 1024 * 1024,
		Compression:         CompressionSnappy,
		VerifyChecksums:     true,
		WriteBufferSize:     2 * 1024 * 1024,
		FlushInterval:       500,
		ParallelCompression: true,
		Log:                 slog.Default(),
	}
}

// SafeOptions returns options prioritizing data safety over speed.
func SafeOptions() *Options {
	return &Options{
		CacheSize:           128 * 1024 * 1024,
		Compression:         CompressionSnappy,
		VerifyChecksums:     true,
		WriteBufferSize:     0, // Immediate writes
		FlushInterval:       0,
		ParallelCompression: false,
		Log:                 slog.Default(),
	}
}

// TurboOptions returns options for maximum write speed.
// Use this for benchmarking or when you need extreme performance.
// WARNING: Data loss possible on crash due to aggressive buffering.
func TurboOptions() *Options {
	return &Options{
		CacheSize:           512 * 1024 * 1024, // 512MB cache
		Compression:         CompressionSnappy, // No compression overhead
		VerifyChecksums:     false,
		WriteBufferSize:     16 * 1024 * 1024, // 16MB buffer
		FlushInterval:       5000,             // 5 second flush
		ParallelCompression: true,
		Log:                 slog.Default(),
	}
}

// Config holds configuration for opening a BlazeDB database.
type Config struct {
	Options *Options
}

// Open creates a new DB reading and writing from/to files under the path
// passed. If a world is present at the path, Open will parse its data and
// initialise the world with it. If the data cannot be parsed, an error is
// returned.
func (conf Config) Open(dir string) (*DB, error) {
	if conf.Options == nil {
		conf.Options = DefaultOptions()
	}
	if conf.Options.Log == nil {
		conf.Options.Log = slog.Default()
	}
	conf.Options.Log = conf.Options.Log.With("provider", "blazedb")

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}

	return newDB(conf, dir)
}

// fileExists checks if a file exists at the given path.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// ensureDir creates a directory if it doesn't exist.
func ensureDir(dir string) error {
	if !fileExists(dir) {
		return os.MkdirAll(dir, 0777)
	}
	return nil
}

// dataPath returns the path to the chunks data file.
func dataPath(dir string) string {
	return filepath.Join(dir, "chunks.dat")
}

// indexPath returns the path to the spatial index file.
func indexPath(dir string) string {
	return filepath.Join(dir, "index.dat")
}

// metadataPath returns the path to the metadata file.
func metadataPath(dir string) string {
	return filepath.Join(dir, "metadata.json")
}
