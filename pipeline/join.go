package pipeline

// Joins the data from two nodes in the order that data arrives.
// As each data point is received from a parent node it is paired
// with the next data point from the other parent node.
// Aliases are used to prefix all fields from the respective nodes.
//
// Example:
//    var errors = stream.fork().from("errors")
//    var requests = stream.fork().from("requests")
//    // Join the errors and requests stream
//    errors.join(requests)
//            .as("errors", "requests")
//            .rename("error_rate")
//        .apply(expr("rate", "errors.value / requests.value")
//        ...
//
// In the above example the `errors` and `requests` streams are joined
// and then transformed to calculate a combined field.
type JoinNode struct {
	node
	// The alias names of the two parents.
	// Note:
	//       Names[1] corresponds to the left  parent
	//       Names[0] corresponds to the right parent
	// tick:ignore
	Names []string
	// The name of this new joined data stream.
	// If empty the name of the left parent is used.
	Rename string
}

func newJoinNode(e EdgeType, n Node) *JoinNode {
	j := &JoinNode{
		node: node{
			desc:     "join",
			wants:    e,
			provides: e,
		},
	}
	n.linkChild(j)
	return j
}

// The alias names to be used as a dot prefix for all field names for each
// data point respectively.
// The name `nLeft` corresponds to the node on the left (i.e `leftNode.join(rightNode)`).
// The name `nRight` corresponds to the node on the right.
// tick:property
func (j *JoinNode) As(nLeft, nRight string) *JoinNode {
	//NOTE: the order is reversed because the second node is linked first
	j.Names = []string{nRight, nLeft}
	return j
}
