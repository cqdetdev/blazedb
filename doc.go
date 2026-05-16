// Package blazedb implements a high-performance world storage provider
// optimized for Minecraft Bedrock worlds.
//
// This implementation is a Dragonfly world.Provider and is intended as a
// specialized replacement for LevelDB/mcdb for chunk-heavy Bedrock workloads.
// It uses append-only chunk records, a persisted Morton/Z-order spatial index,
// an in-memory sharded LRU chunk cache, optional compression, and open-time
// index recovery from chunks.dat.
//
// Current local benchmarks use the committed Dragonfly chunk suite in
// tests/benchmark_test.go on Windows/amd64 with an AMD Ryzen 7 7730U and Go
// 1.26.3. In that suite, BlazeDB measured approximately:
//   - Hot cached chunk load: 95 ns/op vs LevelDB 44.5 us/op (~468x faster).
//   - Default LZ4 StoreColumn: 4.1 us/op vs LevelDB 75.5 us/op (~18x faster).
//   - No-compression StoreColumn: 3.35 us/op vs LevelDB 75.5 us/op (~22x faster).
//   - Reopened tiny-cache chunk load: 26.8 us/op vs LevelDB 1.90 ms/op (~71x faster).
//   - Reopened 11x11 area load: 3.24 ms/op vs LevelDB 236 ms/op (~73x faster).
//   - Generated dense chunk store: 10.3 us/op vs LevelDB 167 us/op (~16x faster).
//   - Generated dense chunk reopened load: 83 us/op vs LevelDB 1.46 ms/op (~18x faster).
//   - Sparse repeated area misses: about 11.1 us/op after negative-miss caching,
//     compared with about 190 us/op cold.
//
// Durability is configurable. DurabilityFast is the default high-throughput
// mode and does not fsync on the StoreColumn hot path. DurabilityBalanced keeps
// buffered write throughput but fsyncs chunks.dat after flush batches.
// DurabilitySafe disables write buffering and fsyncs chunks.dat before
// StoreColumn returns; in the same benchmark run it measured about 558 us/op.
// DurabilitySafeBatch preserves the per-call synced-by-return guarantee while
// letting concurrent callers share a synced batch; parallel safe stores measured
// about 285 us/op in the same benchmark suite.
//
// Benchmark numbers are workload- and machine-dependent. The committed suite
// uses synthetic and generated dense Dragonfly chunk columns, not a bundled
// real-world world corpus.
package blazedb
