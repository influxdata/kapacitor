package pipeline

// Takes the union of all of its parents
type JoinNode struct {
	node
	Names  []string
	Rename string
}

func newJoinNode(e EdgeType, n Node) *JoinNode {
	j := &JoinNode{
		node: node{
			desc:     "join",
			wants:    e,
			provides: e,
		},
	}
	n.linkChild(j)
	return j
}

func (j *JoinNode) As(n1, n2 string) *JoinNode {
	//NOTE: the order is reversed because the second node is linked first
	j.Names = []string{n2, n1}
	return j
}
