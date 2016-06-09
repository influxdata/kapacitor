package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/influxdata/influxdb/tsdb"
)

func cmdInfo(path string) {
	tstore := tsdb.NewStore(filepath.Join(path, "data"))
	tstore.Logger = log.New(ioutil.Discard, "", log.LstdFlags)
	tstore.EngineOptions.Config.Dir = filepath.Join(path, "data")
	tstore.EngineOptions.Config.WALLoggingEnabled = false
	tstore.EngineOptions.Config.WALDir = filepath.Join(path, "wal")
	if err := tstore.Open(); err != nil {
		fmt.Printf("Failed to open dir: %v\n", err)
		os.Exit(1)
	}

	size, err := tstore.DiskSize()
	if err != nil {
		fmt.Printf("Failed to determine disk usage: %v\n", err)
	}

	// Summary stats
	fmt.Printf("Shards: %d, Indexes: %d, Databases: %d, Disk Size: %d, Series: %d\n\n",
		tstore.ShardN(), tstore.DatabaseIndexN(), len(tstore.Databases()), size, countSeries(tstore))

	tw := tabwriter.NewWriter(os.Stdout, 16, 8, 0, '\t', 0)

	fmt.Fprintln(tw, strings.Join([]string{"Shard", "DB", "Measurement", "Tags [#K/#V]", "Fields [Name:Type]", "Series"}, "\t"))

	shardIDs := tstore.ShardIDs()

	databases := tstore.Databases()
	sort.Strings(databases)

	for _, db := range databases {
		index := tstore.DatabaseIndex(db)
		measurements := index.Measurements()
		sort.Sort(measurements)
		for _, m := range measurements {
			tags := m.TagKeys()
			tagValues := 0
			for _, tag := range tags {
				tagValues += len(m.TagValues(tag))
			}
			fields := m.FieldNames()
			sort.Strings(fields)
			series := m.SeriesKeys()
			sort.Strings(series)
			sort.Sort(ShardIDs(shardIDs))

			// Sample a point from each measurement to determine the field types
			for _, shardID := range shardIDs {
				shard := tstore.Shard(shardID)
				codec := shard.FieldCodec(m.Name)
				if codec == nil {
					continue
				}
				for _, field := range codec.Fields() {
					ft := fmt.Sprintf("%s:%s", field.Name, field.Type)
					fmt.Fprintf(tw, "%d\t%s\t%s\t%d/%d\t%d [%s]\t%d\n", shardID, db, m.Name, len(tags), tagValues,
						len(fields), ft, len(series))

				}

			}
		}
	}
	tw.Flush()
}

func countSeries(tstore *tsdb.Store) int {
	var count int
	for _, shardID := range tstore.ShardIDs() {
		shard := tstore.Shard(shardID)
		cnt, err := shard.SeriesCount()
		if err != nil {
			fmt.Printf("series count failed: %v\n", err)
			continue
		}
		count += cnt
	}
	return count
}

// ShardIDs is a collection of UINT 64 that represent shard ids.
type ShardIDs []uint64

func (a ShardIDs) Len() int           { return len(a) }
func (a ShardIDs) Less(i, j int) bool { return a[i] < a[j] }
func (a ShardIDs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
