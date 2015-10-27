package pipeline

// Joins the data from two nodes in the order that data arrives.
// As each data point is received from a parent node it is paired
// with the next data point from the other parent node.
// Aliases are used to prefix all fields from the respective nodes.
//
// Example:
//    var errors = stream.fork().from('errors')
//    var requests = stream.fork().from('requests')
//    // Join the errors and requests streams
//    errors.join(requests)
//            // Provide prefix names for the fields of the data points.
//            .as('errors', 'requests')
//            .streamName('error_rate')
//        // Both the "value" fields from each parent have been prefixed
//        // with the respective names 'errors' and 'requests'.
//        .eval(lambda: "errors.value" / "requests.value"))
//           .as('rate')
//        ...
//
// In the above example the `errors` and `requests` streams are joined
// and then transformed to calculate a combined field.
type JoinNode struct {
	chainnode
	// The alias names of the two parents.
	// Note:
	//       Names[1] corresponds to the left  parent
	//       Names[0] corresponds to the right parent
	// tick:ignore
	Names []string
	// The name of this new joined data stream.
	// If empty the name of the left parent is used.
	StreamName string
}

func newJoinNode(e EdgeType, n Node) *JoinNode {
	j := &JoinNode{
		chainnode: newBasicChainNode("join", e, e),
	}
	n.linkChild(j)
	return j
}

// Prefix names for all fields from the respective nodes.
// Each field from the parent nodes will be prefixed with the provided name and a '.'.
// See the example above.
//
// The name `nLeft` corresponds to the node on the left (i.e `leftNode.join(rightNode)`).
// The name `nRight` corresponds to the node on the right.
// The names cannot have a dot '.' character.
//
// tick:property
func (j *JoinNode) As(nLeft, nRight string) *JoinNode {
	//NOTE: the order is reversed because the second node is linked first
	j.Names = []string{nRight, nLeft}
	return j
}
