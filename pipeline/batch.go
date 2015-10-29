package pipeline

import (
	"time"
)

// A BatchNode defines a source and a schedule for
// processing batch data. The data is queried from
// an InfluxDB database and then passed into the data pipeline.
//
// Example:
// batch
//     .query('''
//         SELECT mean("value")
//         FROM "telegraf"."default".cpu_usage_idle
//         WHERE "host" = 'serverA'
//     ''')
//     .period(1m)
//     .every(20s)
//     .groupBy(time(10s), 'cpu')
//     ...
//
// In the above example InfluxDB is queried every 20 seconds; the window of time returned
// spans 1 minute and is grouped into 10 second buckets.
type BatchNode struct {
	chainnode
	// The query to execute. Must not contain a time condition
	// in the `WHERE` clause or contain a `GROUP BY` clause.
	// The time conditions are added dynamically according to the period, offset and schedule.
	// The `GROUP BY` clause is added dynamically according to the dimensions
	// passed to the `groupBy` method.
	Query string

	// The period or length of time that will be queried from InfluxDB
	Period time.Duration

	// How often to query InfluxDB.
	//
	// The Every property is mutually exclusive with the Cron property.
	Every time.Duration

	// Define a schedule using a cron syntax.
	//
	// The specific cron implementation is documented here:
	// https://github.com/gorhill/cronexpr#implementation
	//
	// The Cron property is mutually exclusive with the Every property.
	Cron string

	// How far back in time to query from the current time
	//
	// For example an Offest of 2 hours and an Every of 5m,
	// Kapacitor will query InfluxDB every 5 minutes for the window of data 2 hours ago.
	//
	// This applies to Cron schedules as well. If the cron specifies to run every Sunday at
	// 1 AM and the Offset is 1 hour. Then at 1 AM on Sunday the data from 12 AM will be queried.
	Offset time.Duration

	// The list of dimensions for the group-by clause.
	//tick:ignore
	Dimensions []interface{}
}

func newBatchNode() *BatchNode {
	return &BatchNode{
		chainnode: newBasicChainNode("batch", BatchEdge, BatchEdge),
	}
}

// Group the data by a set of dimensions.
// At least one dimension should be a `time()`
// dimension, the rest are tag names.
// tick:property
func (b *BatchNode) GroupBy(d ...interface{}) *BatchNode {
	b.Dimensions = d
	return b
}
