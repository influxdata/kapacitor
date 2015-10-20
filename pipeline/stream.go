package pipeline

// A StreamNode represents the source of data being
// streamed to Kapacitor via any of its inputs.
// The stream node allows you to select which portion of the stream
// you want to process.
//
// If you want to process multiple independent streams see the `fork` chaining method.
//
// Example:
//    stream
//           .from('''"mydb"."myrp"."mymeasurement"''')
//           .where("host =~ /logger\d+/")
//        .window()
//        ...
//
// The above example selects only data points from the database `mydb`
// and retention policy `myrp` and measurement `mymeasurement` where
// the tag `host` matches the regex `logger\d+`
type StreamNode struct {
	node
	// Which database, retention policy and measurement to select.
	// This is equivalent to the FROM statement in an InfluxQL
	// query. As such shortened selectors can be supplied
	// (i.e. "mymeasurement" is valid and selects all data points
	// from the measurement "mymeasurement" independent
	// of database or retention policy).
	//
	// If empty then all data points are considered to match.
	From string
	// An expression to filter the data stream.
	// tick:ignore
	Predicate string
}

func newStreamNode() *StreamNode {
	return &StreamNode{
		node: node{
			desc:     "stream",
			wants:    StreamEdge,
			provides: StreamEdge,
		},
	}
}

// Fork the current stream. This is useful if you want to
// select multiple different data streams.
//
// Example:
//    var errors = stream.fork().from("errors")
//    var requests = stream.fork().from("requests")
//
// Example:
//    // This will not work since you are just changing
//    // the 'from' condition on the same stream node.
//    var errors = stream.from("errors")
//    var requests = stream.from("requests")
//    // 'errors' is now the same as 'requests'.
//
func (s *StreamNode) Fork() *StreamNode {
	c := newStreamNode()
	s.linkChild(c)
	return c
}

// Filter the current stream using the given expression.
// This expression is a Kapacitor expression. Kapacitor
// expressions are a superset of InfluxQL WHERE expressions.
// See the `Expression` docs for more information.
//
// If empty then all data points are considered to match.
// tick:property
func (s *StreamNode) Where(expression string) *StreamNode {
	s.Predicate = expression
	return s
}
