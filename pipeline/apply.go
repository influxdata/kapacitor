package pipeline

// Takes the union of all of its parents
type ApplyNode struct {
	node
	Func interface{}
}

func newApplyNode(e EdgeType, f interface{}) *ApplyNode {
	a := &ApplyNode{
		node: node{
			desc:     "apply",
			wants:    e,
			provides: e,
		},
		Func: f,
	}
	return a
}
