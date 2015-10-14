package pipeline

type MapReduceInfo struct {
	MapI    interface{}
	ReduceI interface{}
}

type MapNode struct {
	node
	Map interface{}
}

func NewMapNode(i interface{}) *MapNode {
	return &MapNode{
		node: node{
			desc:     "map",
			wants:    BatchEdge,
			provides: ReduceEdge,
		},
		Map: i,
	}
}

type ReduceNode struct {
	node
	Reduce interface{}
}

func NewReduceNode(i interface{}) *ReduceNode {
	return &ReduceNode{
		node: node{
			desc:     "reduce",
			wants:    ReduceEdge,
			provides: StreamEdge,
		},
		Reduce: i,
	}
}
