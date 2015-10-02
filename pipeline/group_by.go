package pipeline

type GroupByNode struct {
	node
	Dimensions []string
}

func newGroupByNode(wants EdgeType, dims []string) *GroupByNode {
	return &GroupByNode{
		node: node{
			desc:     "groupby",
			wants:    wants,
			provides: wants,
		},
		Dimensions: dims,
	}
}
