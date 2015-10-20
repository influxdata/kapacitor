package pipeline

// tick:ignore
type MapReduceInfo struct {
	MapI    interface{}
	ReduceI interface{}
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
//        .mapReduce(influxql.sum("value"))
//        ...
type MapNode struct {
	node
	// The map function
	// tick:ignore
	Map interface{}
}

func newMapNode(i interface{}) *MapNode {
	return &MapNode{
		node: node{
			desc:     "map",
			wants:    BatchEdge,
			provides: ReduceEdge,
		},
		Map: i,
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
//        .mapReduce(influxql.sum("value"))
//        ...
type ReduceNode struct {
	node
	//The reduce function
	// tick:ignore
	Reduce interface{}
}

func newReduceNode(i interface{}) *ReduceNode {
	return &ReduceNode{
		node: node{
			desc:     "reduce",
			wants:    ReduceEdge,
			provides: StreamEdge,
		},
		Reduce: i,
	}
}
