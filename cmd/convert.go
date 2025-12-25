package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cqdetdev/blazedb"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/df-mc/dragonfly/server/world/mcdb"
)

func main() {
	source := flag.String("source", "", "Source world folder path")
	dest := flag.String("dest", "", "Destination world folder path")
	from := flag.String("from", "", "Source format: 'leveldb' or 'blazedb'")
	to := flag.String("to", "", "Destination format: 'leveldb' or 'blazedb'")
	flag.Parse()

	if *source == "" || *dest == "" || *from == "" || *to == "" {
		fmt.Println("World Converter - Convert between LevelDB and BlazeDB formats")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println("  convert -source <path> -dest <path> -from <format> -to <format>")
		fmt.Println()
		fmt.Println("Formats: leveldb, blazedb")
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  convert -source ./world_ldb -dest ./world_blaze -from leveldb -to blazedb")
		fmt.Println("  convert -source ./world_blaze -dest ./world_ldb -from blazedb -to leveldb")
		os.Exit(1)
	}

	if *from == *to {
		log.Fatal("Source and destination formats must be different")
	}

	start := time.Now()
	fmt.Printf("Converting from %s to %s...\n", *from, *to)
	fmt.Printf("Source: %s\n", *source)
	fmt.Printf("Destination: %s\n", *dest)

	// Create destination folder
	if err := os.MkdirAll(*dest, 0755); err != nil {
		log.Fatalf("Failed to create destination folder: %v", err)
	}

	totalChunks := 0
	dim := world.Overworld

	// Handle based on source/dest types
	switch {
	case *from == "leveldb" && *to == "blazedb":
		totalChunks = convertLevelDBToBlazeDB(*source, *dest, dim)
	case *from == "blazedb" && *to == "leveldb":
		totalChunks = convertBlazeDBToLevelDB(*source, *dest, dim)
	default:
		log.Fatalf("Unsupported conversion: %s to %s", *from, *to)
	}

	elapsed := time.Since(start)
	fmt.Printf("\n✓ Conversion complete!\n")
	fmt.Printf("  Total chunks: %d\n", totalChunks)
	fmt.Printf("  Time: %v\n", elapsed.Round(time.Millisecond))
	if elapsed.Seconds() > 0 {
		fmt.Printf("  Speed: %.0f chunks/second\n", float64(totalChunks)/elapsed.Seconds())
	}
}

func convertLevelDBToBlazeDB(srcPath, dstPath string, dim world.Dimension) int {
	// Open LevelDB source
	srcDB, err := mcdb.Config{}.Open(srcPath)
	if err != nil {
		log.Fatalf("Failed to open LevelDB source: %v", err)
	}
	defer srcDB.Close()

	// Open BlazeDB destination
	dstDB, err := blazedb.Config{Options: blazedb.DefaultOptions()}.Open(dstPath)
	if err != nil {
		log.Fatalf("Failed to open BlazeDB destination: %v", err)
	}
	defer dstDB.Close()

	// Copy settings
	dstDB.SaveSettings(srcDB.Settings())

	fmt.Println("Converting chunks from LevelDB to BlazeDB...")

	totalChunks := 0

	// Create iterator for LevelDB
	iter := srcDB.NewColumnIterator(nil)
	for iter.Next() {
		pos := iter.Position()

		// Load column from source
		col, err := srcDB.LoadColumn(pos, dim)
		if err != nil {
			continue
		}

		// Store to destination
		if err := dstDB.StoreColumn(pos, dim, col); err != nil {
			log.Printf("Warning: Failed to store chunk at %v: %v", pos, err)
			continue
		}

		totalChunks++
		if totalChunks%100 == 0 {
			fmt.Printf("\rConverted %d chunks...", totalChunks)
		}
	}
	iter.Release()

	return totalChunks
}

func convertBlazeDBToLevelDB(srcPath, dstPath string, dim world.Dimension) int {
	// Open BlazeDB source
	srcDB, err := blazedb.Config{Options: blazedb.DefaultOptions()}.Open(srcPath)
	if err != nil {
		log.Fatalf("Failed to open BlazeDB source: %v", err)
	}
	defer srcDB.Close()

	// Open LevelDB destination
	dstDB, err := mcdb.Config{}.Open(dstPath)
	if err != nil {
		log.Fatalf("Failed to open LevelDB destination: %v", err)
	}
	defer dstDB.Close()

	// Copy settings
	dstDB.SaveSettings(srcDB.Settings())

	fmt.Println("Converting chunks from BlazeDB to LevelDB...")

	totalChunks := 0

	// Create iterator for BlazeDB
	iter := srcDB.NewColumnIterator(nil)
	for iter.Next() {
		pos := iter.Position()

		// Load column from source
		col, err := srcDB.LoadColumn(pos, dim)
		if err != nil {
			continue
		}

		// Store to destination
		if err := dstDB.StoreColumn(pos, dim, col); err != nil {
			log.Printf("Warning: Failed to store chunk at %v: %v", pos, err)
			continue
		}

		totalChunks++
		if totalChunks%100 == 0 {
			fmt.Printf("\rConverted %d chunks...", totalChunks)
		}
	}
	iter.Release()

	return totalChunks
}
