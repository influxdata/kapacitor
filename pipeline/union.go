package pipeline

// Takes the union of all of its parents
type UnionNode struct {
	node
	NewName string
}

func newUnionNode(e EdgeType, nodes []Node) *UnionNode {
	u := &UnionNode{
		node: node{
			desc:     "union",
			wants:    e,
			provides: e,
		},
	}
	for _, n := range nodes {
		n.linkChild(u)
	}
	return u
}
