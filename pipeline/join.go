package pipeline

import (
	"time"
)

// Joins the data from any number of nodes.
// As each data point is received from a parent node it is paired
// with the next data points from the other parent nodes with a
// matching timestamp. Each parent node contributes at most one point
// to each joined point. A tolerance can be supplied to join points
// that do not have perfectly aligned timestamps.
// Any points that fall within the tolerance are joined on the timestamp.
// If multiple points fall within the same tolerance window than they are joined in the order
// they arrive.
//
// Aliases are used to prefix all fields from the respective nodes.
//
// The join can be an inner or outer join, see the JoinNode.Fill property.
//
// Example:
//    var errors = stream
//        |from()
//            .measurement('errors')
//    var requests = stream
//        |from()
//            .measurement('requests')
//    // Join the errors and requests streams
//    errors
//        |join(requests)
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
//        |eval(lambda: "errors.value" / "requests.value"))
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
	Names []string `tick:"As"`

	// The dimensions on which to join
	// tick:ignore
	Dimensions []string `tick:"On"`

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

// Join on specfic dimensions.
// For example given two measurements:
//
// 1. building_power -- tagged by building, value is the total power consumed by the building.
// 2. floor_power -- tagged by building and floor, values is the total power consumed by the floor.
//
// You want to calculate the percentage of the total building power consumed by each floor.
//
// Example:
//    var buidling = stream
//        |from()
//            .measurement('building_power')
//            .groupBy('building')
//    var floor = stream
//        |from()
//            .measurement('floor_power')
//            .groupBy('building', 'floor')
//    building
//        |join(floor)
//            .as('building', 'floor')
//            .on('building')
//        |eval(lambda: "floor.value" / "building.value")
//            ... // Values here are grouped by 'building' and 'floor'
//
// tick:property
func (j *JoinNode) On(dims ...string) *JoinNode {
	j.Dimensions = dims
	return j
}
