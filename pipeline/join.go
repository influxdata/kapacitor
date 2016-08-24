package pipeline

import (
	"fmt"
	"strings"
	"time"
)

const (
	defaultJoinDelimiter = "."
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
//        |eval(lambda: "errors.value" / "requests.value")
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

	// The delimiter for the field name prefixes.
	// Can be the empty string.
	Delimiter string

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
	//
	// When using a numerical or null fill, the fields names are determined by copying
	// the field names from another point.
	// This doesn't work well when different sources have different field names.
	// Use the DefaultNode and DeleteNode to finalize the fill operation if necessary.
	//
	// Example:
	//    var maintlock = stream
	//        |from()
	//            .measurement('maintlock')
	//            .groupBy('service')
	//    var requests = stream
	//        |from()
	//            .measurement('requests')
	//            .groupBy('service')
	//    // Join the maintlock and requests streams
	//    // The intent it to drop any points in maintenance mode.
	//    maintlock
	//        |join(requests)
	//            // Provide prefix names for the fields of the data points.
	//            .as('maintlock', 'requests')
	//            // points that are within 1 second are considered the same time.
	//            .tolerance(1s)
	//            // fill missing fields with null, implies outer join.
	//            // a better default per field will be set later.
	//            .fill('null')
	//            // name the resulting stream.
	//            .streamName('requests')
	//        |default()
	//            // default maintenance mode to false, overwriting the null value if present.
	//            .field('maintlock.mode', false)
	//            // default the requests to 0, again overwriting the null value if present.
	//            .field('requests.value', 0.0)
	//        // drop any points that are in maintenance mode.
	//        |where(lambda: "maintlock.mode")
	//        |...
	Fill interface{}
}

func newJoinNode(e EdgeType, parents []Node) *JoinNode {
	j := &JoinNode{
		chainnode: newBasicChainNode("join", e, e),
		Delimiter: defaultJoinDelimiter,
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

// Join on a subset of the group by dimensions.
// This is a special case where you want a single point from one parent to join with multiple
// points from a different parent.
//
// For example given two measurements:
//
// 1. building_power (a single value) -- tagged by building, value is the total power consumed by the building.
// 2. floor_power (multiple values) -- tagged by building and floor, values are the total power consumed by each floor.
//
// You want to calculate the percentage of the total building power consumed by each floor.
// Since you only have one point per building you need it to join multiple times with
// the points from each floor. By defining the `on` dimensions as `building` we are saying
// that we want points that only have the building tag to be joined with more specifc points that
// more tags, in this case the `floor` tag. In other words while we have points with tags building and floor
// we only want to join on the building tag.
//
// Example:
//    var building = stream
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

// Validate that the as() specification is consistent with the number of join arms.
func (j *JoinNode) validate() error {
	if len(j.Names) == 0 {
		return fmt.Errorf("a call to join.as() is required to specify the output stream prefixes.")
	}

	if len(j.Names) != len(j.Parents()) {
		return fmt.Errorf("number of prefixes specified by join.as() must match the number of joined streams")
	}

	for _, name := range j.Names {
		if len(name) == 0 {
			return fmt.Errorf("must provide a prefix name for the join node, see .as() property method")
		}
		if j.Delimiter != "" && strings.Contains(name, j.Delimiter) {
			return fmt.Errorf("cannot use name %s as field prefix, it contains the delimiter %q	", name, j.Delimiter)
		}
	}
	names := make(map[string]bool, len(j.Names))
	for _, name := range j.Names {
		if names[name] {
			return fmt.Errorf("cannot use the same prefix name see .as() property method")
		}
		names[name] = true
	}

	return nil
}
