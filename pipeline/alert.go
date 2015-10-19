package pipeline

type AlertNode struct {
	node

	// Alert filters, each is an expression.
	// The assumption is that each sucessive level is a subset
	// of the previous level so the filter will only be applied if
	// a data point passed the previous level.
	// An empty value indicates the level is invalid and is skipped.
	Info string
	Warn string
	Crit string

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
