package pipeline

// tick:ignore
type MapReduceInfo struct {
	Map    interface{}
	Reduce interface{}
	Edge   EdgeType
}

// DEPRECATION WARNING: As of v0.11 you can use the new InfluxQLNode to perform map reduce functions.
// This way of performing influxql functions will be removed in the 0.12 release.
//
// Performs a map operation on the data stream.
// In the map-reduce framework it is assumed that
// several different partitions of the data can be
// 'mapped' in parallel while only one 'reduce' operation
// will process all of the data stream.
//
// Example:
//    stream
//        .window()
//            .period(10s)
//            .every(10s)
//        // Sum the values for each 10s window of data.
//        .sum('value')
//        ...
type MapNode struct {
	chainnode
	// The map function
	// tick:ignore
	Map interface{}
}

func newMapNode(wants EdgeType, i interface{}) *MapNode {
	return &MapNode{
		chainnode: newBasicChainNode("map", wants, ReduceEdge),
		Map:       i,
	}
}

// DEPRECATION WARNING: As of v0.11 you can use the new InfluxQLNode to perform map reduce functions.
// This way of performing influxql functions will be removed in the 0.12 release.
//
// Performs a reduce operation on the data stream.
// In the map-reduce framework it is assumed that
// several different partitions of the data can be
// 'mapped' in parallel while only one 'reduce' operation
// will process all of the data stream.
//
// Example:
//    stream
//        .window()
//            .period(10s)
//            .every(10s)
//        // Sum the values for each 10s window of data.
//        .sum('value')
//        ...
type ReduceNode struct {
	chainnode
	//The reduce function
	// tick:ignore
	Reduce interface{}

	// Whether to use the max time or the
	// time of the selected point
	// tick:ignore
	PointTimes bool

	// The name of the field, defaults to the name of
	// MR function used (i.e. influxql.mean -> 'mean')
	As string
}

func newReduceNode(i interface{}, et EdgeType) *ReduceNode {
	return &ReduceNode{
		chainnode: newBasicChainNode("reduce", ReduceEdge, et),
		Reduce:    i,
	}
}

// Use the time of the selected point instead of the time of the batch.
//
// Only applies to selector MR functions like first, last, top, bottom, etc.
// Aggregation functions always use the batch time.
// tick:property
func (r *ReduceNode) UsePointTimes() *ReduceNode {
	r.PointTimes = true
	return r
}
