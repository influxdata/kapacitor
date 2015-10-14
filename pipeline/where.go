package pipeline

type WhereNode struct {
	node
	Predicate string
}

func newWhereNode(wants EdgeType, predicate string) *WhereNode {
	return &WhereNode{
		node: node{
			desc:     "where",
			wants:    wants,
			provides: wants,
		},
		Predicate: predicate,
	}
}
