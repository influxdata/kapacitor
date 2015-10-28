package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// A StreamNode represents the source of data being
// streamed to Kapacitor via any of its inputs.
// The stream node allows you to select which portion of the stream
// you want to process.
//
// Example:
//    stream
//        .from('"mydb"."myrp"."mymeasurement"')
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

	// The db.rp.m from clause
	// tick:ignore
	FromSelector string
}

func newStreamNode() *StreamNode {
	return &StreamNode{
		chainnode: newBasicChainNode("stream", StreamEdge, StreamEdge),
	}
}

// Which database, retention policy and measurement to select.
// This is equivalent to the FROM statement in an InfluxQL
// query. As such shortened selectors can be supplied
// (i.e. "mymeasurement" is valid and selects all data points
// from the measurement "mymeasurement" independent
// of database or retention policy).
//
// Creates a new stream node that can be further
// filtered using the Where property.
// From can be called multiple times to create multiple
// independent forks of the data stream.
//
// Example:
//    var cpu = stream.from('cpu')
//    var load = stream.from('load')
//    // Join cpu and load streams and do further processing
//    cpu.join(load)
//            .as('cpu', 'load')
//        ...
func (s *StreamNode) From(from string) *StreamNode {
	f := newStreamNode()
	f.FromSelector = from
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
