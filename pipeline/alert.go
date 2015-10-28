package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// An AlertNode can trigger an alert of varying severity levels
// based on the data it receives.
//
// Example:
//   stream
//        .alert()
//            .info(lambda: "value" > 10)
//            .warn(lambda: "value" > 20)
//            .crit(lambda: "value" > 30)
//            .post("http://example.com/api/alert")
//
//
// It is assumed that each successive level filters a subset
// of the previous level. As a result the filter will only be applied if
// a data point passed the previous level.
// In the above example if value = 15 then the INFO and
// WARNING expressions would be evaluated but not the
// CRITICAL expression.
//
// Each expression maintains its own state.
type AlertNode struct {
	node

	// Filter expression for the INFO alert level.
	// An empty value indicates the level is invalid and is skipped.
	Info tick.Node
	// Filter expression for the WARNING alert level.
	// An empty value indicates the level is invalid and is skipped.
	Warn tick.Node
	// Filter expression for the CRITICAL alert level.
	// An empty value indicates the level is invalid and is skipped.
	Crit tick.Node

	//tick:ignore
	UseFlapping bool
	//tick:ignore
	FlapLow float64
	//tick:ignore
	FlapHigh float64
	// Number of previous states to remember when computing flapping levels.
	History int

	// Post the alert data to the specified URL.
	Post string

	// Email settings
	//tick:ignore
	From string
	//tick:ignore
	ToList []string
	//tick:ignore
	Subject string

	// A command to run when an alert triggers
	//tick:ignore
	Command []string

	// Log alert data to file
	Log string
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

// Execute a command whenever an alert is trigger and pass the alert data over STDIN.
// tick:property
func (a *AlertNode) Exec(executable string, args ...string) *AlertNode {
	a.Command = append([]string{executable}, args...)
	return a
}

// Email the alert data.
// tick:property
func (a *AlertNode) Email(from, subject string, to ...string) *AlertNode {
	a.From = from
	a.Subject = subject
	a.ToList = to
	return a
}

// Perform flap detection on the alerts.
// The method used is similar method to Nagios:
// https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/flapping.html
//
// Each different alerting level is considered a different state.
// tick:property
func (a *AlertNode) Flapping(low, high float64) Node {
	a.UseFlapping = true
	a.FlapLow = low
	a.FlapHigh = high
	return a
}
