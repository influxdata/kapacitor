package main

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

func cmdVerify(path string) {
	start := time.Now()
	dataPath := filepath.Join(path, "data")

	brokenBlocks := 0
	totalBlocks := 0

	// No need to do this in a loop
	ext := fmt.Sprintf(".%s", tsm1.TSMFileExtension)

	// Get all TSM files by walking through the data dir
	files := []string{}
	err := filepath.Walk(dataPath, func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(path) == ext {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	tw := tabwriter.NewWriter(os.Stdout, 16, 8, 0, '\t', 0)

	// Verify the checksums of every block in every file
	for _, f := range files {
		file, err := os.OpenFile(f, os.O_RDONLY, 0600)
		if err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		reader, err := tsm1.NewTSMReader(file)
		if err != nil {
			fmt.Printf("%v", err)
			os.Exit(1)
		}

		blockItr := reader.BlockIterator()
		brokenFileBlocks := 0
		count := 0
		for blockItr.Next() {
			totalBlocks++
			key, _, _, checksum, buf, err := blockItr.Read()
			if err != nil {
				brokenBlocks++
				fmt.Fprintf(tw, "%s: could not get checksum for key %v block %d due to error: %q\n", f, key, count, err)
			} else if expected := crc32.ChecksumIEEE(buf); checksum != expected {
				brokenBlocks++
				fmt.Fprintf(tw, "%s: got %d but expected %d for key %v, block %d\n", f, checksum, expected, key, count)
			}
			count++
		}
		if brokenFileBlocks == 0 {
			fmt.Fprintf(tw, "%s: healthy\n", f)
		}
		reader.Close()
	}

	fmt.Fprintf(tw, "Broken Blocks: %d / %d, in %vs\n", brokenBlocks, totalBlocks, time.Since(start).Seconds())
	tw.Flush()
}
