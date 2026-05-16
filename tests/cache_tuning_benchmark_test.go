package tests

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/world"
)

func BenchmarkCacheTuningDenseWorkingSet(b *testing.B) {
	cases := []struct {
		name      string
		cacheSize int64
		width     int32
		height    int32
	}{
		{name: "Radius5_16MB", cacheSize: 16 * 1024 * 1024, width: 11, height: 11},
		{name: "Radius5_32MB", cacheSize: 32 * 1024 * 1024, width: 11, height: 11},
		{name: "Radius5_64MB", cacheSize: 64 * 1024 * 1024, width: 11, height: 11},
		{name: "Radius16_32MB", cacheSize: 32 * 1024 * 1024, width: 33, height: 33},
		{name: "Radius16_64MB", cacheSize: 64 * 1024 * 1024, width: 33, height: 33},
		{name: "Radius16_128MB", cacheSize: 128 * 1024 * 1024, width: 33, height: 33},
		{name: "Radius16_256MB", cacheSize: 256 * 1024 * 1024, width: 33, height: 33},
		{name: "Radius32_64MB", cacheSize: 64 * 1024 * 1024, width: 65, height: 65},
		{name: "Radius32_128MB", cacheSize: 128 * 1024 * 1024, width: 65, height: 65},
		{name: "Radius32_256MB", cacheSize: 256 * 1024 * 1024, width: 65, height: 65},
		{name: "Radius32_512MB", cacheSize: 512 * 1024 * 1024, width: 65, height: 65},
		{name: "Radius32_1024MB", cacheSize: 1024 * 1024 * 1024, width: 65, height: 65},
	}

	col := createDenseTestColumn(world.Overworld.Range())
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			positions := benchmarkPositions(tc.width, tc.height)
			opts := blazeOptions(tc.cacheSize, 0)
			populateBlazeDBWithColumn(b, dir, positions, opts, col)

			db, err := blazedb.Config{Options: opts}.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			for _, pos := range positions {
				if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
					b.Fatal(err)
				}
			}

			before := db.GetStats()
			b.ReportMetric(float64(tc.cacheSize)/(1024*1024), "cache_MB")
			b.ReportMetric(float64(len(positions)), "chunks")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pos := positions[i%len(positions)]
				if _, err := db.LoadColumn(pos, world.Overworld); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			after := db.GetStats()
			reads := after.ChunkReads - before.ChunkReads
			hits := after.CacheHits - before.CacheHits
			if reads > 0 {
				b.ReportMetric(float64(hits)*100/float64(reads), "hit_%")
			}
			runtime.GC()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			b.ReportMetric(float64(mem.HeapAlloc)/(1024*1024), "heap_MB")
		})
	}
}

func BenchmarkCacheTuningStoreUnchangedSafe(b *testing.B) {
	for _, cacheMB := range []int64{16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("%dMB", cacheMB), func(b *testing.B) {
			dir := b.TempDir()
			opts := blazedb.SafeOptions()
			opts.CacheSize = cacheMB * 1024 * 1024
			opts.Log = discardLogger()
			db, err := blazedb.Config{Options: opts}.Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			pos := world.ChunkPos{42, -42}
			col := createDenseTestColumn(world.Overworld.Range())
			if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
				b.Fatal(err)
			}

			b.ReportMetric(float64(cacheMB), "cache_MB")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.StoreColumn(pos, world.Overworld, col); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
