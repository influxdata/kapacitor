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
//     .period(10s)
//     .groupBy(time(2s), 'cpu')
//     ...
//
// In the above example InfluxDB is queried every 10 seconds and the results
// are then processed by the rest of the pipeline.
type BatchNode struct {
	chainnode
	// The query to execute. Must not contain a time condition
	// in the `WHERE` clause or contain a `GROUP BY` clause.
	// The time conditions are added dynamically according to the period.
	// The `GROUP BY` clause is added dynamically according to the dimensions
	// passed to the `groupBy` method.
	Query string

	// The period at which Kapacitor should query InfluxDB.
	Period time.Duration

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
