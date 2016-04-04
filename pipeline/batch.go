package pipeline

import (
	"bytes"
	"time"

	"github.com/influxdata/kapacitor/tick"
)

// A node that handles creating several child BatchNodes.
// Each call to `query` creates a child batch node that
// can further be configured. See BatchNode
// The `batch` variable in batch tasks is an instance of
// a SourceBatchNode.
//
// Example:
//     var errors = batch
//                      |query('SELECT value from errors')
//                      ...
//     var views = batch
//                      |query('SELECT value from views')
//                      ...
//
type SourceBatchNode struct {
	node
}

func newSourceBatchNode() *SourceBatchNode {
	return &SourceBatchNode{
		node: node{
			desc:     "srcbatch",
			wants:    NoEdge,
			provides: BatchEdge,
		},
	}
}

// The query to execute. Must not contain a time condition
// in the `WHERE` clause or contain a `GROUP BY` clause.
// The time conditions are added dynamically according to the period, offset and schedule.
// The `GROUP BY` clause is added dynamically according to the dimensions
// passed to the `groupBy` method.
func (b *SourceBatchNode) Query(q string) *BatchNode {
	n := newBatchNode()
	n.QueryStr = q
	b.linkChild(n)
	return n
}

// Do not add the source batch node to the dot output
// since its not really an edge.
// tick:ignore
func (b *SourceBatchNode) dot(buf *bytes.Buffer) {
}

// A BatchNode defines a source and a schedule for
// processing batch data. The data is queried from
// an InfluxDB database and then passed into the data pipeline.
//
// Example:
// batch
//     |query('''
//         SELECT mean("value")
//         FROM "telegraf"."default".cpu_usage_idle
//         WHERE "host" = 'serverA'
//     ''')
//         .period(1m)
//         .every(20s)
//         .groupBy(time(10s), 'cpu')
//     ...
//
// In the above example InfluxDB is queried every 20 seconds; the window of time returned
// spans 1 minute and is grouped into 10 second buckets.
type BatchNode struct {
	chainnode

	// self describer
	describer *tick.ReflectionDescriber

	// The query text
	//tick:ignore
	QueryStr string

	// The period or length of time that will be queried from InfluxDB
	Period time.Duration

	// How often to query InfluxDB.
	//
	// The Every property is mutually exclusive with the Cron property.
	Every time.Duration

	// Align start and end times with the Every value
	// Does not apply if Cron is used.
	// tick:ignore
	AlignFlag bool `tick:"Align"`

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
	Dimensions []interface{} `tick:"GroupBy"`

	// Fill the data.
	// Options are:
	//
	//   - Any numerical value
	//   - null - exhibits the same behavior as the default
	//   - previous - reports the value of the previous window
	//   - none - suppresses timestamps and values where the value is null
	Fill interface{}

	// The name of a configured InfluxDB cluster.
	// If empty the default cluster will be used.
	Cluster string
}

func newBatchNode() *BatchNode {
	b := &BatchNode{
		chainnode: newBasicChainNode("batch", BatchEdge, BatchEdge),
	}
	b.describer, _ = tick.NewReflectionDescriber(b)
	return b
}

// Group the data by a set of dimensions.
// Can specify one time dimension.
//
// This property adds a `GROUP BY` clause to the query
// so all the normal behaviors when quering InfluxDB with a `GROUP BY` apply.
// More details: https://influxdb.com/docs/v0.9/query_language/data_exploration.html#the-group-by-clause
//
// Example:
//    batch
//        |query(...)
//            .groupBy(time(10s), 'tag1', 'tag2'))
//
// tick:property
func (b *BatchNode) GroupBy(d ...interface{}) *BatchNode {
	b.Dimensions = d
	return b
}

// Align start and stop times for quiries with even boundaries of the BatchNode.Every property.
// Does not apply if using the BatchNode.Cron property.
// tick:property
func (b *BatchNode) Align() *BatchNode {
	b.AlignFlag = true
	return b
}

// Tick Describer methods

//tick:ignore
func (b *BatchNode) Desc() string {
	return b.describer.Desc()
}

//tick:ignore
func (b *BatchNode) HasChainMethod(name string) bool {
	if name == "groupBy" {
		return true
	}
	return b.describer.HasChainMethod(name)
}

//tick:ignore
func (b *BatchNode) CallChainMethod(name string, args ...interface{}) (interface{}, error) {
	if name == "groupBy" {
		return b.chainnode.GroupBy(args...), nil
	}
	return b.describer.CallChainMethod(name, args...)
}

//tick:ignore
func (b *BatchNode) HasProperty(name string) bool {
	return b.describer.HasProperty(name)
}

//tick:ignore
func (b *BatchNode) Property(name string) interface{} {
	return b.describer.Property(name)
}

//tick:ignore
func (b *BatchNode) SetProperty(name string, args ...interface{}) (interface{}, error) {
	return b.describer.SetProperty(name, args...)
}
