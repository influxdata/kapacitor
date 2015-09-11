package pipeline

type MapReduceFunc func() (mapF interface{}, reduceF interface{})

type MapNode struct {
	node
	Func   interface{}
	Fields []string
}

func NewMapNode(f interface{}, fields ...string) *MapNode {
	return &MapNode{
		node: node{
			desc:     "map",
			wants:    BatchEdge,
			provides: ReduceEdge,
		},
		Func:   f,
		Fields: fields,
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
