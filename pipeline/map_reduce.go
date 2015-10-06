package pipeline

type MapReduceFunc func() (mapF interface{}, reduceF interface{})

type MapNode struct {
	node
	Func  interface{}
	Field string
}

func NewMapNode(f interface{}, field string) *MapNode {
	return &MapNode{
		node: node{
			desc:     "map",
			wants:    BatchEdge,
			provides: ReduceEdge,
		},
		Func:  f,
		Field: field,
	}
}

type ReduceNode struct {
	node
	Func interface{}
}

func NewReduceNode(f interface{}) *ReduceNode {
	return &ReduceNode{
		node: node{
			desc:     "reduce",
			wants:    ReduceEdge,
			provides: StreamEdge,
		},
		Func: f,
	}
}
