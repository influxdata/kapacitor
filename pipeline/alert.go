package pipeline

import (
	"github.com/influxdb/kapacitor/tick"
)

// Number of previous states to remember when computing flapping percentage.
const defaultFlapHistory = 21

// Default template for constructing an ID
const defaultIDTmpl = "{{ .Name }}:{{ .Group }}"

// Default template for constructing a message.
const defaultMessageTmpl = "{{ .ID }} is {{ .Level }}"

// An AlertNode can trigger an event of varying severity levels,
// and pass the event to alert handlers. The criteria for triggering
// an alert is specified via a [lambda expression](https://influxdb.com/docs/kapacitor/v0.1/tick/expr.html).
// See AlertNode.Info, AlertNode.Warn, and AlertNode.Cirt below.
//
// Different event handlers can be configured for each AlertNode.
// Some handlers like Email, Slack, VictorOps and PagerDuty have a configuration
// option 'global' that indicates that all alerts implicitly use the handler.
//
// Available event handlers:
//
//    * log -- log alert data to file.
//    * post -- HTTP POST data to a specified URL.
//    * email -- Send and email with alert data.
//    * exec -- Execute a command passing alert data over STDIN.
//    * Slack -- Post alert message to Slack channel.
//    * VictorOps -- Send alert to VictorOps.
//    * PagerDuty -- Send alert to PagerDuty.
//
// See below for more details on configuring each handler.
//
// Each event that gets sent to a handler contains the following alert data:
//
//    * ID -- the ID of the alert, user defined.
//    * Message -- the alert message, user defined.
//    * Time -- the time the alert occurred.
//    * Level -- one of OK, INFO, WARNING or CRITICAL.
//    * Data -- influxql.Result containing the data that triggered the alert.
//
// Events are sent to handlers if the alert is in a state other than 'OK'
// or the alert just changed to the 'OK' state from a non 'OK' state (a.k.a. the alert recovered).
// Using the AlertNode.StateChangesOnly property events will only be sent to handlers
// if the alert changed state.
//
// Example:
//   stream
//        .groupBy('service')
//        .alert()
//            .id('kapacitor/{{ index .Tags "service" }}')
//            .message('{{ .ID }} is {{ .Level }} value:{{ index .Fields "value" }}')
//            .info(lambda: "value" > 10)
//            .warn(lambda: "value" > 20)
//            .crit(lambda: "value" > 30)
//            .post("http://example.com/api/alert")
//
//
// It is assumed that each successive level filters a subset
// of the previous level. As a result, the filter will only be applied if
// a data point passed the previous level.
// In the above example, if value = 15 then the INFO and
// WARNING expressions would be evaluated, but not the
// CRITICAL expression.
// Each expression maintains its own state.
type AlertNode struct {
	node

	// Template for constructing a unique ID for a given alert.
	//
	// Available template data:
	//
	//    * Name -- Measurement name.
	//    * Group -- Concatenation of all group-by tags of the form [key=value,]+.
	//        If no groupBy is performed equal to literal 'nil'.
	//    * Tags -- Map of tags. Use '{{ index .Tags "key" }}' to get a specific tag value.
	//
	// Example:
	//   stream.from().measurement('cpu')
	//   .groupBy('cpu')
	//   .alert()
	//      .id('kapacitor/{{ .Name }}/{{ .Group }}')
	//
	// ID: kapacitor/cpu/cpu=cpu0,
	//
	// Example:
	//   stream...
	//   .groupBy('service')
	//   .alert()
	//      .id('kapacitor/{{ index .Tags "service" }}')
	//
	// ID: kapacitor/authentication
	//
	// Example:
	//   stream...
	//   .groupBy('service', 'host')
	//   .alert()
	//      .id('kapacitor/{{ index .Tags "service" }}/{{ index .Tags "host" }}')
	//
	// ID: kapacitor/authentication/auth001.example.com
	//
	// Default: {{ .Name }}:{{ .Group }}
	Id string

	// Template for constructing a meaningful message for the alert.
	//
	// Available template data:
	//
	//    * ID -- The ID of the alert.
	//    * Name -- Measurement name.
	//    * Group -- Concatenation of all group-by tags of the form [key=value,]+.
	//        If no groupBy is performed equal to literal 'nil'.
	//    * Tags -- Map of tags. Use '{{ index .Tags "key" }}' to get a specific tag value.
	//    * Level -- Alert Level, one of: INFO, WARNING, CRITICAL.
	//    * Fields -- Map of fields. Use '{{ index .Fields "key" }}' to get a specific field value.
	//
	// Example:
	//   stream...
	//   .groupBy('service', 'host')
	//   .alert()
	//      .id('{{ index .Tags "service" }}/{{ index .Tags "host" }}')
	//      .message('{{ .ID }} is {{ .Level}} value: {{ index .Fields "value" }}')
	//
	// Message: authentication/auth001.example.com is CRITICAL value:42
	//
	// Default: {{ .ID }} is {{ .Level }}
	Message string

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

	// Number of previous states to remember when computing flapping levels and
	// checking for state changes.
	// Minimum value is 2 in order to keep track of current and previous states.
	//
	// Default: 21
	History int

	// Post the JSON alert data to the specified URL.
	Post string

	// Email settings
	//tick:ignore
	UseEmail bool
	//tick:ignore
	ToList []string

	// A command to run when an alert triggers
	//tick:ignore
	Command []string

	// Log JSON alert data to file. One event per line.
	Log string

	// Send alert to VictorOps.
	// tick:ignore
	UseVictorOps bool

	// VictorOps RoutingKey.
	// tick:ignore
	VictorOpsRoutingKey string

	// Send alert to PagerDuty.
	// tick:ignore
	UsePagerDuty bool

	// Send alert to Slack.
	// tick:ignore
	UseSlack bool

	// Slack channel
	// tick:ignore
	SlackChannel string

	// Send alerts only on state changes.
	// tick:ignore
	IsStateChangesOnly bool
}

func newAlertNode(wants EdgeType) *AlertNode {
	return &AlertNode{
		node: node{
			desc:     "alert",
			wants:    wants,
			provides: NoEdge,
		},
		History: defaultFlapHistory,
		Id:      defaultIDTmpl,
		Message: defaultMessageTmpl,
	}
}

// Execute a command whenever an alert is trigger and pass the alert data over STDIN in JSON format.
// tick:property
func (a *AlertNode) Exec(executable string, args ...string) *AlertNode {
	a.Command = append([]string{executable}, args...)
	return a
}

// Email the alert data.
//
// If the To list is empty, the To addresses from the configuration are used.
// The email subject is the AlertNode.Message property.
// The email body is the JSON alert data.
//
// If the 'smtp' section in the configuration has the option: global = true
// then all alerts are sent via email without the need to explicitly state it
// in the TICKscript.
//
// Example:
//     [smtp]
//       enabled = true
//       host = "localhost"
//       port = 25
//       username = ""
//       password = ""
//       from = "kapacitor@example.com"
//       to = ["oncall@example.com"]
//       # Set global to true so all alert trigger emails.
//       global = true
//
// Example:
//    stream...
//         .alert()
//
// Send email to 'oncall@example.com' from 'kapacitor@example.com'
//
// **NOTE**: The global option for email also implies stateChangesOnly is set on all alerts.
// tick:property
func (a *AlertNode) Email(to ...string) *AlertNode {
	a.UseEmail = true
	a.ToList = to
	return a
}

// Perform flap detection on the alerts.
// The method used is similar method to Nagios:
// https://assets.nagios.com/downloads/nagioscore/docs/nagioscore/3/en/flapping.html
//
// Each different alerting level is considered a different state.
// The low and high thresholds are inverted thresholds of a percentage of state changes.
// Meaning that if the percentage of state changes goes above the `high`
// threshold, the alert enters a flapping state. The alert remains in the flapping state
// until the percentage of state changes goes below the `low` threshold.
// Typical values are low: 0.25 and high: 0.5. The percentage values represent the number state changes
// over the total possible number of state changes. A percentage change of 0.5 means that the alert changed
// state in half of the recorded history, and remained the same in the other half of the history.
// tick:property
func (a *AlertNode) Flapping(low, high float64) Node {
	a.UseFlapping = true
	a.FlapLow = low
	a.FlapHigh = high
	return a
}

// Send alert to VictorOps.
// To use VictorOps alerting you must first enable the 'Alert Ingestion API'
// in the 'Integrations' section of VictorOps.
// Then place the API key from the URL into the 'victorops' section of the Kapacitor configuration.
//
// Example:
//    [victorops]
//      enabled = true
//      api-key = "xxxxx"
//      routing-key = "everyone"
//
// With the correct configuration you can now use VictorOps in TICKscripts.
//
// Example:
//    stream...
//         .alert()
//             .victorOps()
//
// Send alerts to VictorOps using the routing key in the configuration file.
//
// Example:
//    stream...
//         .alert()
//             .victorOps()
//             .routingKey('team_rocket')
//
// Send alerts to VictorOps with routing key 'team_rocket'
//
// If the 'victorops' section in the configuration has the option: global = true
// then all alerts are sent to VictorOps without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [victorops]
//      enabled = true
//      api-key = "xxxxx"
//      routing-key = "everyone"
//      global = true
//
// Example:
//    stream...
//         .alert()
//
// Send alert to VictorOps using the default routing key, found in the configuration.
// tick:property
func (a *AlertNode) VictorOps() *AlertNode {
	a.UseVictorOps = true
	return a
}

// The VictorOps routing key. If not set uses key specified in configuration.
// tick:property
func (a *AlertNode) RoutingKey(routingKey string) *AlertNode {
	a.VictorOpsRoutingKey = routingKey
	return a
}

// Send the alert to PagerDuty.
// To use PagerDuty alerting you must first follow the steps to enable a new 'Generic API' service.
//
// From https://developer.pagerduty.com/documentation/integration/events
//
//    1. In your account, under the Services tab, click "Add New Service".
//    2. Enter a name for the service and select an escalation policy. Then, select "Generic API" for the Service Type.
//    3. Click the "Add Service" button.
//    4. Once the service is created, you'll be taken to the service page. On this page, you'll see the "Service key", which is needed to access the API
//
// Place the 'service key' into the 'pagerduty' section of the Kapacitor configuration as the option 'service-key'.
//
// Example:
//    [pagerduty]
//      enabled = true
//      service-key = "xxxxxxxxx"
//
// With the correct configuration you can now use PagerDuty in TICKscripts.
//
// Example:
//    stream...
//         .alert()
//             .pagerDuty()
//
// If the 'pagerduty' section in the configuration has the option: global = true
// then all alerts are sent to PagerDuty without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [pagerduty]
//      enabled = true
//      service-key = "xxxxxxxxx"
//      global = true
//
// Example:
//    stream...
//         .alert()
//
// Send alert to PagerDuty.
// tick:property
func (a *AlertNode) PagerDuty() *AlertNode {
	a.UsePagerDuty = true
	return a
}

// Send the alert to Slack.
// To allow Kapacitor to post to Slack,
// go to the URL https://slack.com/services/new/incoming-webhook
// and create a new incoming webhook and place the generated URL
// in the 'slack' configuration section.
//
// Example:
//    [slack]
//      enabled = true
//      url = "https://hooks.slack.com/services/xxxxxxxxx/xxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxx"
//      channel = "#general"
//
// In order to not post a message every alert interval
// use AlertNode.StateChangesOnly so that only events
// where the alert changed state are posted to the channel.
//
// Example:
//    stream...
//         .alert()
//             .slack()
//
// Send alerts to Slack channel in the configuration file.
//
// Example:
//    stream...
//         .alert()
//             .slack()
//             .channel('#alerts')
//
// Send alerts to Slack channel '#alerts'
//
// Example:
//    stream...
//         .alert()
//             .slack()
//             .channel('@jsmith')
//
// Send alert to user '@jsmith'
//
// If the 'slack' section in the configuration has the option: global = true
// then all alerts are sent to Slack without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [slack]
//      enabled = true
//      url = "https://hooks.slack.com/services/xxxxxxxxx/xxxxxxxxx/xxxxxxxxxxxxxxxxxxxxxxxx"
//      channel = "#general"
//      global = true
//
// Example:
//    stream...
//         .alert()
//
// Send alert to Slack using default channel '#general'.
// **NOTE**: The global option for Slack also implies stateChangesOnly is set on all alerts.
// tick:property
func (a *AlertNode) Slack() *AlertNode {
	a.UseSlack = true
	return a
}

// Slack channel in which to post messages.
// If empty uses the channel from the configuration.
// tick:property
func (a *AlertNode) Channel(channel string) *AlertNode {
	a.SlackChannel = channel
	return a
}

// Only sends events where the state changed.
// Each different alert level OK, INFO, WARNING, and CRITICAL
// are considered different states.
//
// Example:
//    stream...
//        .window()
//             .period(10s)
//             .every(10s)
//        .alert()
//            .crit(lambda: "value" > 10)
//            .stateChangesOnly()
//            .slack()
//
// If the "value" is greater than 10 for a total of 60s, then
// only two events will be sent. First, when the value crosses
// the threshold, and second, when it falls back into an OK state.
// Without stateChangesOnly, the alert would have triggered 7 times:
// 6 times for each 10s period where the condition was met and once more
// for the recovery.
//
// tick:property
func (a *AlertNode) StateChangesOnly() *AlertNode {
	a.IsStateChangesOnly = true
	return a
}
