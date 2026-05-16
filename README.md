# BlazeDB

BlazeDB is a high-performance, specialized storage engine for Minecraft Bedrock Edition worlds, designed essentially as a drop-in replacement for LevelDB.

It is engineered to overcome specific bottlenecks found in general-purpose KV stores like LevelDB when applied to Minecraft's spatial data patterns. BlazeDB leverages **Z-Order (Morton) Curves** for spatial locality, an **append-only architecture** for high write throughput, and a **sharded O(1) LRU cache** for low-latency reads.

## Features

- 🚀 **High Performance**: Optimized specifically for chunk data (reads/writes).
- 🗺️ **Spatial Locality**: Uses Z-Order (Morton) curves to keep creating/loading nearby chunks physically close on disk, effectively acting as a hardware-level prefetch.
- 💾 **Append-Only Storage**: Writes are always appended to the end of the file, eliminating random write seeks and "compaction stalls" during gameplay.
- 🧠 **Smart Caching**: Sharded, thread-safe LRU cache with O(1) operations and auto-tuning capabilities.
- ⚡ **Async Writes**: Non-blocking background write worker with buffering to prevent frame drops during heavy chunk generation.
- 📦 **Compression**: Supports LZ4 (default, ultra-fast) and Snappy. Includes buffer pooling to minimize GC pressure.
- 🛡️ **Crash Safety**: Atomic offset tracking and index rebuilding capabilities.

## Architecture & How It Works

### Data Storage Schematic

BlazeDB avoids the complexity of LSM Trees (used by LevelDB) in favor of a simpler, faster append-only model paired with an in-memory spatial index.

```text
Minecraft Server
|-- Put Chunk
|   `-- Write Buffer (map)
|       |-- async flush -> chunks.dat (append-only data)
|       `-- index update -> Spatial Index (Z-order map)
|
`-- Get Chunk
    `-- Sharded LRU Cache
        `-- miss -> Spatial Index (Z-order map)
            `-- lookup offset -> chunks.dat
                `-- read + decompress -> Sharded LRU Cache

Disk files:
|-- chunks.dat  append-only chunk data
`-- index.dat   persisted spatial index
```

### 1. Z-Order Spatial Indexing
Minecraft accesses data spatially (e.g., "load all chunks around player X,Z"). Standard KV stores like LevelDB just see keys as strings and may scatter nearby chunks across different SSTables.

BlazeDB interleaves the bits of the chunk X and Z coordinates to create a **Morton Code**. This ensures that chunks which are close to each other in the game world are mathematically close in the index.
- **Key**: `{MortonCode(x, z), DimensionID}`
- **Value**: `{FileOffset, Size, DimensionID}`

### 2. Append-Only Data File (`chunks.dat`)
When a chunk is saved, it is effectively serialized, compressed (LZ4), and appended to the end of `chunks.dat`. The index is updated to point to this new location.
- **Pros**: Zero seek time for writes; extremely fast.
- **Cons**: Old versions of chunks remain in the file until a "compaction" process runs (similar to garbage collection).

### 3. Chunk Format
Each entry in `chunks.dat` follows this binary format:
```text
[Header: "BLAZ"]      4 bytes
[Size]                4 bytes (Uint32)
[CRC32]               4 bytes
[ChunkX, ChunkZ]      8 bytes (Int32, Int32)
[DimensionID]         4 bytes (Int32)
[CompressionType]     1 byte
[SubChunkCount]       1 byte
[Biomes Data]         Length + Data
[SubChunks Data...]   Length + Data (Repeated)
[Entities Data]       Length + NBT
[BlockEntities Data]  Length + NBT
```

### 4. Sharded LRU Cache
To handle the high concurrency of a multi-player server, the in-memory cache is partitioned into 16 shards. This reduces lock contention, allowing multiple routines to read/write cache entries simultaneously without waiting on a single global lock.

## BlazeDB vs. LevelDB

| Feature | BlazeDB | LevelDB (mcdb) |
| :--- | :--- | :--- |
| **Write Strategy** | Append-Only (Log Structured) | LSM Tree (Sorted Strings Table) |
| **Write Performance** | **Extremely High** (Sequential I/O) | High (but suffers from compaction stalls) |
| **Read Performance** | **High** (Spatial Indexing + Caching) | High (Bloom Filters + Block Cache) |
| **Spatial Locality** | **Excellent** (Z-Order Curve) | Poor/Random (Lexicographical) |
| **Concurrency** | Sharded Locks (Low contention) | Global/Level Implementation dependent |
| **Disk Usage** | Higher (Requires periodic compaction) | Lower (Aggressive auto-compaction) |
| **Compression** | LZ4 (Fast) | Snappy / Zlib |
| **Stalls** | Rare (Background compaction optional) | Occasional (LevelDB write stalls) |

## Benchmarks

Lower is better. These numbers are from the committed benchmark suite in `tests/benchmark_test.go`, run locally on Windows/amd64 with an AMD Ryzen 7 7730U and Go 1.26.3. The benchmark columns use synthetic Dragonfly `chunk.Column` values; default BlazeDB rows use LZ4 compression unless noted. Allocation columns are `-benchmem` per-operation allocation, not total process RSS.

| Workload | BlazeDB | LevelDB | Speedup | BlazeDB Alloc/op | LevelDB Alloc/op |
| :--- | ---: | ---: | ---: | ---: | ---: |
| Hot cached chunk load | ~0.85 us/op | ~3.36 us/op | ~4.0x | ~157 B | ~564 B |
| Store chunk, default LZ4 | ~5.8 us/op | ~74 us/op | ~13x | ~3.6 KB | ~15-16 KB |
| Store chunk, no compression | ~3.9 us/op | ~74 us/op | ~19x | ~2.9 KB | ~15-16 KB |
| Reopened tiny-cache chunk load | ~27.1 us/op | ~1.91 ms/op | ~70x | ~12.3 KB | ~692-733 KB |
| Reopened 11x11 area load | ~3.26 ms/op | ~236 ms/op | ~72x | ~1.48 MB | ~86.1 MB |

BlazeDB also caches negative area misses. In sparse repeated area scans, the benchmarked path drops from about `279 us/op` cold to about `17.1 us/op` once misses are cached.

Safety mode store costs from the same benchmark run:

| Mode | Store Cost | vs LevelDB Store | Recommended Use |
| :--- | ---: | ---: | :--- |
| `DurabilityFast` | ~5.8 us/op | ~13x faster | Benchmarks, bulk conversion, local testing, and worlds where losing recent buffered writes is acceptable. |
| `DurabilityBalanced` | ~4.0 us/op | ~18x faster | Recommended server mode: keeps buffered throughput while syncing completed flush batches. |
| `DurabilitySafe` | ~556 us/op | ~7.5x slower | Critical writes where `StoreColumn` must not return until chunk bytes are synced to disk. |

## Safety Modes

BlazeDB exposes three durability modes so you can choose the safety/performance tradeoff explicitly:

| Mode | What It Guarantees | Performance Cost |
| :--- | :--- | :--- |
| `DurabilityFast` | Recent writes may be lost if the process or machine dies before buffered data reaches disk. Existing synced chunk records can still be recovered by index rebuild. | Fastest write path; no fsync on store/flush. |
| `DurabilityBalanced` | Once a flush batch completes, its chunk bytes are fsynced and recoverable even if `index.dat` is stale or corrupted. Writes still return when queued, so the latest queued writes can be lost before the next flush. | Near-fast-mode caller latency; fsync cost is paid per flush batch. |
| `DurabilitySafe` | `StoreColumn` writes the chunk record and fsyncs `chunks.dat` before returning. If the newest record is later corrupt, rebuild can fall back to the previous valid record. | Slowest mode because every store pays storage sync latency. |

```go
opts := blazedb.BalancedOptions() // recommended for production servers
db, err := blazedb.Config{Options: opts}.Open("world_blazedb")
```

Use `SafeOptions()` when per-call durability matters more than throughput:

```go
opts := blazedb.SafeOptions()
db, err := blazedb.Config{Options: opts}.Open("world_blazedb")
```

## Recovery and Durability

BlazeDB treats `index.dat` as a persisted acceleration index. If it is missing, corrupted, or stale compared with `chunks.dat`, BlazeDB rebuilds it on open by scanning valid records in `chunks.dat`, then saves the rebuilt index atomically. During rebuild, incomplete tail records from interrupted appends are truncated, complete records with bad CRCs are skipped, and older valid chunk versions remain usable.

Old chunk versions are removed only when `Compact()` is called.

### Pros of BlazeDB
1.  **No Write Stalls**: Since it just appends to a file, you don't get the "hiccups" caused by LevelDB compacting generic string keys in the background.
2.  **Faster Chunk Loading**: The use of Z-order curves means that when the OS reads pages from the disk, it's more likely to grab specifically the chunks relevant to the player's area.
3.  **Modern Compression**: LZ4 is significantly faster than standard Snappy/Zlib implementations for this use case.

### Cons of BlazeDB
1.  **Disk Space**: Because it is append-only, if a chunk is saved 100 times, it exists 100 times in the file until compacted. LevelDB handles this cleanup automatically and continuously.
2.  **Memory Usage**: The in-memory index scales with world size. For massive worlds (terabytes), the index RAM usage needs to be considered (though it is quite compact).

## Usage

```go
package main

import (
    "log/slog"

    "github.com/cqdetdev/blazedb"
    "github.com/df-mc/dragonfly/server"
)

func main() {
    // Open BlazeDB provider
    p, err := blazedb.Config{Options: blazedb.DefaultOptions()}.Open("path/to/world/db")
    if err != nil {
        panic(err)
    }
    
    // Use it in Dragonfly
    userConf := server.DefaultConfig()
    userConf.World.SaveData = false
    conf, err := userConf.Config(slog.Default())
    if err != nil {
        panic(err)
    }
    conf.WorldProvider = p
    
    srv := conf.New()
    srv.CloseOnProgramEnd()
    srv.Listen()
    
    for range srv.Accept() {
    }
}
```


## Predictive Prefetching

BlazeDB includes a smart, threaded prefetcher that analyzes player movement to predict and load future chunks before they are requested.

- **Velocity-Based**: Calculates player direction and velocity to determine where they are heading.
- **Look-Ahead**: Loads 1-3 chunks ahead in the movement direction (including diagonals).
- **Background Workers**: runs on dedicated goroutines to avoid blocking the main server thread.

```go
// The prefetcher is automatically initialized with the DB.
// You just need to update it with player positions.
prefetcher.UpdatePlayerPosition(playerUUID, currentChunkPos, dimension)
```

## Iterator

BlazeDB provides a `ColumnIterator` for efficient scanning of the world. It uses the spatial index to quickly identify relevant chunks without reading them all from disk.

```go
iter := db.NewColumnIterator(&blazedb.IteratorRange{
    Min: world.ChunkPos{0, 0},
    Max: world.ChunkPos{100, 100},
    Dimension: world.Overworld,
})
defer iter.Release()

for iter.Next() {
    pos := iter.Position()
    col := iter.Column()
    // Process chunk...
}
if err := iter.Error(); err != nil {
    // Handle error
}
```

## Advanced Configuration

| Option | Default | Description |
| :--- | :--- | :--- |
| `CacheSize` | `256MB` | Maximum memory usage for the chunk cache. |
| `Compression` | `CompressionLZ4` | Compression algorithm (`LZ4`, `Snappy`, `None`). |
| `WriteBufferSize` | `4MB` | Size of the in-memory write buffer before forcing a flush. |
| `FlushInterval` | `1000ms` | Simple periodic background flushes. |
| `VerifyChecksums` | `false` | Verify CRC32 checksums on read (costs CPU). |
| `Durability` | `DurabilityFast` | Fsync/buffering policy (`Fast`, `Balanced`, `Safe`). |
| `Log` | `slog.Default()` | Logger for debug/error messages. |

`TurboOptions()` uses a `512MB` cache, `CompressionNone`, a `16MB` write buffer, and a `5000ms` flush interval for maximum write throughput when crash-window tradeoffs are acceptable.

`BalancedOptions()` enables `DurabilityBalanced`, checksum verification, Snappy compression, and smaller flush batches. `SafeOptions()` enables `DurabilitySafe`, checksum verification, immediate writes, and no background write buffer.

### Credits
- Antigravity and Claude Code
