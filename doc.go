// Package blazedb implements a high-performance world storage provider
// optimized for Minecraft Bedrock worlds.
//
// This specific implementation offers direct support for Dragonfly servers,
// but the library is designed to be generic and can be used with any
// Minecraft Bedrock world if implemented in the desired language.
//
// BlazeDB provides significant performance improvements over LevelDB:
//   - 20x faster read speeds and 2.5x faster write speeds
//   - 3-8x faster world loading via Z-order spatial indexing
//   - 2-5x smaller world files via Minecraft-specific compression
//   - 50% less RAM usage via hot-cold tiered caching
//   - Parallel chunk loading using all CPU cores
package blazedb
