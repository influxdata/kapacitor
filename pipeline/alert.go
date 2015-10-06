package pipeline

type AlertNode struct {
	node
	Predicate string
	Post      string
}

func newAlertNode(wants EdgeType) *AlertNode {
	return &AlertNode{
		node: node{
			desc:     "alert",
			wants:    wants,
			provides: NoEdge,
		},
	}
}
