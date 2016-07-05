package pipeline

import "time"

const (
	defaultFlattenDelimiter = "."
)

// Flatten a set of points on specific dimensions.
// For example given two points:
//
// m,host=A,port=80 bytes=3512
// m,host=A,port=443 bytes=6723
//
// Flattening the points on `port` would result in a single point:
//
// m,host=A 80.bytes=3512,443.bytes=6723
//
// Example:
//        |flatten()
//            .on('port')
//
// If flattening on multiple dimensions the order is preserved:
//
// m,host=A,port=80 bytes=3512
// m,host=A,port=443 bytes=6723
// m,host=B,port=443 bytes=7243
//
// Flattening the points on `host` and `port` would result in a single point:
//
// m A.80.bytes=3512,A.443.bytes=6723,B.443.bytes=7243
//
// Example:
//        |flatten()
//            .on('host', 'port')
//
//
// Since flattening points creates dynamically named fields in general it is expected
// that the resultant data is passed to a UDF or similar for custom processing.
type FlattenNode struct {
	chainnode

	// The dimensions on which to join
	// tick:ignore
	Dimensions []string `tick:"On"`

	// The delimiter between field name parts
	Delimiter string

	// The maximum duration of time that two incoming points
	// can be apart and still be considered to be equal in time.
	// The joined data point's time will be rounded to the nearest
	// multiple of the tolerance duration.
	Tolerance time.Duration
}

func newFlattenNode(e EdgeType) *FlattenNode {
	f := &FlattenNode{
		chainnode: newBasicChainNode("flatten", e, e),
		Delimiter: defaultFlattenDelimiter,
	}
	return f
}

// Specify the dimensions on which to flatten the points.
// tick:property
func (f *FlattenNode) On(dims ...string) *FlattenNode {
	f.Dimensions = dims
	return f
}
