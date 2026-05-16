// Package blazedb implements a high-performance world storage provider
// optimized for Minecraft Bedrock worlds.
//
// This implementation is a Dragonfly world.Provider and is intended as a
// specialized replacement for LevelDB/mcdb for chunk-heavy Bedrock workloads.
// It uses append-only chunk records, a persisted Morton/Z-order spatial index,
// an in-memory sharded LRU chunk cache, optional compression, and open-time
// index recovery from chunks.dat. Workload-oriented opt-in APIs include
// overlay providers for disposable maps, explicit block-delta records, pinned
// cache areas, and segmented providers for region-level deletion/compaction.
//
// Current local benchmarks use the committed Dragonfly chunk suite in
// tests/benchmark_test.go on Windows/amd64 with an AMD Ryzen 7 7730U and Go
// 1.26.3. In that suite, BlazeDB measured approximately:
//   - Hot cached chunk load: 82.9 ns/op vs LevelDB 43.8 us/op (~529x faster).
//   - Default LZ4 StoreColumn: 4.2 us/op vs LevelDB 74.8 us/op (~18x faster).
//   - No-compression StoreColumn: 3.57 us/op vs LevelDB 74.8 us/op (~21x faster).
//   - Repeated unchanged safe StoreColumn: 5.47 us/op vs LevelDB unchanged
//     StoreColumn 55.8 us/op (~10x faster).
//   - Reopened tiny-cache chunk load: 26.9 us/op vs LevelDB 1.93 ms/op (~72x faster).
//   - Reopened 11x11 area load: 3.21 ms/op vs LevelDB 236.6 ms/op (~74x faster).
//   - Generated dense chunk store: 11.2 us/op vs LevelDB 166.4 us/op (~15x faster).
//   - Generated dense chunk reopened load: 84.0 us/op vs LevelDB 1.48 ms/op (~18x faster).
//   - Sparse repeated area misses: about 11.1 us/op after negative-miss caching,
//     compared with about 190 us/op cold.
//   - Explicit one-block delta records: about 14-19 us/op with one
//     allocation in tests/provider_workflow_benchmark_test.go.
//
// Durability is configurable. DurabilityFast is the default high-throughput
// mode and does not fsync on the StoreColumn hot path. DurabilityBalanced keeps
// buffered write throughput but fsyncs chunks.dat after flush batches.
// DurabilitySafe disables write buffering and fsyncs chunks.dat before
// StoreColumn returns; in the same benchmark run it measured about 558 us/op.
// DurabilitySafeBatch preserves the per-call synced-by-return guarantee while
// letting concurrent callers share a synced batch; parallel safe stores measured
// roughly 330 us/op in the same benchmark suite, with expected fsync noise.
// Repeated byte-identical hot
// chunk saves are elided after exact encoded-record comparison, so they skip
// append/fsync and measured about 5.47 us/op in DurabilitySafe.
//
// Benchmark numbers are workload- and machine-dependent. The committed suite
// uses synthetic and generated dense Dragonfly chunk columns, not a bundled
// real-world world corpus. The optional overlay, delta, pinning, and segmented
// APIs are additive; the default StoreColumn/LoadColumn path does not enable
// delta records unless Options.EnableDeltaRecords is set.
package blazedb
