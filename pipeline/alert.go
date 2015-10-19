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

	//Flapping
	UseFlapping bool
	FlapLow     float64
	FlapHigh    float64
	History     int

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

// Perform flap detection on the alerts using a similar method to Nagios:
// https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/flapping.html
// Each different alerting level is considered a different state.
func (a *AlertNode) Flapping(low, high float64) Node {
	a.UseFlapping = true
	a.FlapLow = low
	a.FlapHigh = high
	return a
}
