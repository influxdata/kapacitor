package pipeline

type AlertNode struct {
	node

	// HTTP POST
	Post string

	// Email settings
	From    string
	ToList  []string
	Subject string
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

func (a *AlertNode) Email(from, subject string, to ...string) Node {
	a.From = from
	a.Subject = subject
	a.ToList = to
	return a
}
