package pipeline

// tick:ignore
type MapReduceInfo struct {
	Map    interface{}
	Reduce interface{}
}

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
//        .mapReduce(influxql.sum('value'))
//        ...
type MapNode struct {
	chainnode
	// The map function
	// tick:ignore
	Map interface{}
}

func newMapNode(i interface{}) *MapNode {
	return &MapNode{
		chainnode: newBasicChainNode("map", BatchEdge, ReduceEdge),
		Map:       i,
	}
}

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
//        .mapReduce(influxql.sum('value'))
//        ...
type ReduceNode struct {
	chainnode
	//The reduce function
	// tick:ignore
	Reduce interface{}
}

func newReduceNode(i interface{}) *ReduceNode {
	return &ReduceNode{
		chainnode: newBasicChainNode("reduce", ReduceEdge, StreamEdge),
		Reduce:    i,
	}
}
