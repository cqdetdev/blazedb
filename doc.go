// Package blazedb implements a high-performance world storage provider
// optimized for Minecraft Bedrock worlds.
//
// This implementation is a Dragonfly world.Provider and is intended as a
// specialized replacement for LevelDB/mcdb for chunk-heavy Bedrock workloads.
// It uses append-only chunk records, a persisted Morton/Z-order spatial index,
// an in-memory sharded LRU chunk cache, optional compression, and open-time
// index recovery from chunks.dat.
//
// Current local benchmarks use the committed synthetic Dragonfly chunk suite in
// tests/benchmark_test.go on Windows/amd64 with an AMD Ryzen 7 7730U and Go
// 1.26.3. In that suite, BlazeDB measured approximately:
//   - Hot cached chunk load: 0.85 us/op vs LevelDB 3.36 us/op (~4x faster).
//   - Default LZ4 StoreColumn: 5.8 us/op vs LevelDB 74 us/op (~13x faster).
//   - No-compression StoreColumn: 3.9 us/op vs LevelDB 74 us/op (~19x faster).
//   - Reopened tiny-cache chunk load: 27.1 us/op vs LevelDB 1.91 ms/op (~70x faster).
//   - Reopened 11x11 area load: 3.26 ms/op vs LevelDB 236 ms/op (~72x faster).
//   - Sparse repeated area misses: about 17.1 us/op after negative-miss caching,
//     compared with about 279 us/op cold.
//
// Durability is configurable. DurabilityFast is the default high-throughput
// mode and does not fsync on the StoreColumn hot path. DurabilityBalanced keeps
// buffered write throughput but fsyncs chunks.dat after flush batches.
// DurabilitySafe disables write buffering and fsyncs chunks.dat before
// StoreColumn returns; in the same benchmark run it measured about 556 us/op,
// which is slower than LevelDB's synthetic store benchmark but provides a
// stronger per-call chunk-byte persistence guarantee.
//
// Benchmark numbers are workload- and machine-dependent. The committed suite
// uses synthetic Dragonfly chunk columns, not a bundled real-world world corpus.
package blazedb
