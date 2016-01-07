package pipeline

import (
	"time"

	"github.com/influxdata/kapacitor/tick"
)

// A StreamNode represents the source of data being
// streamed to Kapacitor via any of its inputs.
// The stream node allows you to select which portion of the stream
// you want to process.
// The `stream` variable in stream tasks is an instance of
// a StreamNode.
//
// Example:
//    stream
//        .from()
//           .database('mydb')
//           .retentionPolicy('myrp')
//           .measurement('mymeasurement')
//           .where(lambda: "host" =~ /logger\d+/)
//        .window()
//        ...
//
// The above example selects only data points from the database `mydb`
// and retention policy `myrp` and measurement `mymeasurement` where
// the tag `host` matches the regex `logger\d+`
type StreamNode struct {
	chainnode
	// An expression to filter the data stream.
	// tick:ignore
	Expression tick.Node

	// The dimensions by which to group to the data.
	// tick:ignore
	Dimensions []interface{}

	// The database name.
	// If empty any database will be used.
	Database string

	// The retention policy name
	// If empty any retention policy will be used.
	RetentionPolicy string

	// The measurement name
	// If empty any measurement will be used.
	Measurement string

	// Optional duration for truncating timestamps.
	// Helpful to ensure data points land on specfic boundaries
	// Example:
	//    stream
	//       .from().measurement('mydata')
	//           .truncate(1s)
	//
	// All incoming data will be truncated to 1 second resolution.
	Truncate time.Duration
}

func newStreamNode() *StreamNode {
	return &StreamNode{
		chainnode: newBasicChainNode("stream", StreamEdge, StreamEdge),
	}
}

// Creates a new stream node that can be further
// filtered using the Database, RetentionPolicy, Measurement and Where properties.
// From can be called multiple times to create multiple
// independent forks of the data stream.
//
// Example:
//    // Select the 'cpu' measurement from just the database 'mydb'
//    // and retention policy 'myrp'.
//    var cpu = stream.from()
//                       .database('mydb')
//                       .retentionPolicy('myrp')
//                       .measurement('cpu')
//    // Select the 'load' measurement from any database and retention policy.
//    var load = stream.from()
//                        .measurement('load')
//    // Join cpu and load streams and do further processing.
//    cpu.join(load)
//            .as('cpu', 'load')
//        ...
//
func (s *StreamNode) From() *StreamNode {
	f := newStreamNode()
	s.linkChild(f)
	return f
}

// Filter the current stream using the given expression.
// This expression is a Kapacitor expression. Kapacitor
// expressions are a superset of InfluxQL WHERE expressions.
// See the `Expression` docs for more information.
//
// If empty then all data points are considered to match.
// tick:property
func (s *StreamNode) Where(expression tick.Node) *StreamNode {
	s.Expression = expression
	return s
}

// Group the data by a set of tags.
//
// Can pass literal * to group by all dimensions.
// Example:
//    .groupBy(*)
//
func (s *StreamNode) GroupBy(tag ...interface{}) *StreamNode {
	s.Dimensions = tag
	return s
}
