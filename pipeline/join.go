package pipeline

import (
	"time"
)

// Joins the data from any number of nodes in the order that data arrives.
// The join can be an inner or outer join, see the JoinNode.Fill property.
// As each data point is received from a parent node it is paired
// with the next data point from the other parent node with a
// matching timestamp.
// A tolerance can be supplied in order to consider points within the give
// tolerance duration will be considered to be the same time.
//
// Aliases are used to prefix all fields from the respective nodes.
//
// Example:
//    var errors = stream
//                   .from('errors')
//    var requests = stream
//                   .from('requests')
//    // Join the errors and requests streams
//    errors.join(requests)
//            // Provide prefix names for the fields of the data points.
//            .as('errors', 'requests')
//            // points that are within 1 second are considered the same time.
//            .tolerance(1s)
//            // fill missing values with 0, implies outer join.
//            .fill(0.0)
//            // name the resulting stream
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

	// The maximum duration of time that two incoming points
	// can be apart and still be considered to be equal in time.
	// The joined data point's time will be rounded to the nearest
	// multiple of the tolerance duration.
	Tolerance time.Duration

	// Fill the data.
	// The fill option implies the type of join: inner or full outer
	// Options are:
	//
	//   - none - (default) skip rows where a point is missing, inner join.
	//   - null - fill missing points with null, full outer join.
	//   - Any numerical value - fill fields with given value, full outer join.
	Fill interface{}
}

func newJoinNode(e EdgeType, parents []Node) *JoinNode {
	j := &JoinNode{
		chainnode: newBasicChainNode("join", e, e),
	}
	for _, n := range parents {
		n.linkChild(j)
	}
	return j
}

// Prefix names for all fields from the respective nodes.
// Each field from the parent nodes will be prefixed with the provided name and a '.'.
// See the example above.
//
// The names cannot have a dot '.' character.
//
// tick:property
func (j *JoinNode) As(names ...string) *JoinNode {
	j.Names = names
	return j
}
