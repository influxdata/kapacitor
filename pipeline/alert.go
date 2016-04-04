package pipeline

import "github.com/influxdata/kapacitor/tick"

// Number of previous states to remember when computing flapping percentage.
const defaultFlapHistory = 21

// Default template for constructing an ID
const defaultIDTmpl = "{{ .Name }}:{{ .Group }}"

// Default template for constructing a message.
const defaultMessageTmpl = "{{ .ID }} is {{ .Level }}"

// Default template for constructing a details message.
const defaultDetailsTmpl = "{{ json . }}"

// Default log mode for file
const defaultLogFileMode = 0600

// An AlertNode can trigger an event of varying severity levels,
// and pass the event to alert handlers. The criteria for triggering
// an alert is specified via a [lambda expression](/kapacitor/latest/tick/expr/).
// See AlertNode.Info, AlertNode.Warn, and AlertNode.Crit below.
//
// Different event handlers can be configured for each AlertNode.
// Some handlers like Email, HipChat, Sensu, Slack, OpsGenie, VictorOps, PagerDuty and Talk have a configuration
// option 'global' that indicates that all alerts implicitly use the handler.
//
// Available event handlers:
//
//    * log -- log alert data to file.
//    * post -- HTTP POST data to a specified URL.
//    * email -- Send and email with alert data.
//    * exec -- Execute a command passing alert data over STDIN.
//    * HipChat -- Post alert message to HipChat room.
//    * Alerta -- Post alert message to Alerta.
//    * Sensu -- Post alert message to Sensu client.
//    * Slack -- Post alert message to Slack channel.
//    * OpsGenie -- Send alert to OpsGenie.
//    * VictorOps -- Send alert to VictorOps.
//    * PagerDuty -- Send alert to PagerDuty.
//    * Talk -- Post alert message to Talk client.
//
// See below for more details on configuring each handler.
//
// Each event that gets sent to a handler contains the following alert data:
//
//    * ID -- the ID of the alert, user defined.
//    * Message -- the alert message, user defined.
//    * Details -- the alert details, user defined HTML content.
//    * Time -- the time the alert occurred.
//    * Level -- one of OK, INFO, WARNING or CRITICAL.
//    * Data -- influxql.Result containing the data that triggered the alert.
//
// Events are sent to handlers if the alert is in a state other than 'OK'
// or the alert just changed to the 'OK' state from a non 'OK' state (a.k.a. the alert recovered).
// Using the AlertNode.StateChangesOnly property events will only be sent to handlers
// if the alert changed state.
//
// It is valid to configure multiple alert handlers, even with the same type.
//
// Example:
//   stream
//           .groupBy('service')
//       |alert()
//           .id('kapacitor/{{ index .Tags "service" }}')
//           .message('{{ .ID }} is {{ .Level }} value:{{ index .Fields "value" }}')
//           .info(lambda: "value" > 10)
//           .warn(lambda: "value" > 20)
//           .crit(lambda: "value" > 30)
//           .post("http://example.com/api/alert")
//           .post("http://another.example.com/api/alert")
//           .email().to('oncall@example.com')
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
	//    * TaskName -- The name of the task
	//    * Group -- Concatenation of all group-by tags of the form [key=value,]+.
	//        If no groupBy is performed equal to literal 'nil'.
	//    * Tags -- Map of tags. Use '{{ index .Tags "key" }}' to get a specific tag value.
	//
	// Example:
	//   stream
	//       |from()
	//           .measurement('cpu')
	//           .groupBy('cpu')
	//       |alert()
	//           .id('kapacitor/{{ .Name }}/{{ .Group }}')
	//
	// ID: kapacitor/cpu/cpu=cpu0,
	//
	// Example:
	//   stream
	//       |from()
	//           .measurement('cpu')
	//           .groupBy('service')
	//       |alert()
	//           .id('kapacitor/{{ index .Tags "service" }}')
	//
	// ID: kapacitor/authentication
	//
	// Example:
	//   stream
	//       |from()
	//           .measurement('cpu')
	//           .groupBy('service', 'host')
	//       |alert()
	//           .id('kapacitor/{{ index .Tags "service" }}/{{ index .Tags "host" }}')
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
	//    * TaskName -- The name of the task
	//    * Group -- Concatenation of all group-by tags of the form [key=value,]+.
	//        If no groupBy is performed equal to literal 'nil'.
	//    * Tags -- Map of tags. Use '{{ index .Tags "key" }}' to get a specific tag value.
	//    * Level -- Alert Level, one of: INFO, WARNING, CRITICAL.
	//    * Fields -- Map of fields. Use '{{ index .Fields "key" }}' to get a specific field value.
	//    * Time -- The time of the point that triggered the event.
	//
	// Example:
	//   stream
	//       |from()
	//           .measurement('cpu')
	//           .groupBy('service', 'host')
	//       |alert()
	//           .id('{{ index .Tags "service" }}/{{ index .Tags "host" }}')
	//           .message('{{ .ID }} is {{ .Level}} value: {{ index .Fields "value" }}')
	//
	// Message: authentication/auth001.example.com is CRITICAL value:42
	//
	// Default: {{ .ID }} is {{ .Level }}
	Message string

	// Template for constructing a detailed HTML message for the alert.
	// The same template data is available as the AlertNode.Message property,
	// in addition to a Message field that contains the rendered Message value.
	//
	// The intent is that the Message property be a single line summary while the
	// Details property is a more detailed message possibly spanning multiple lines,
	// and containing HTML formatting.
	//
	// This template is rendered using the html/template package in Go so that
	// safe and valid HTML can be generated.
	//
	// The `json` method is available within the template to convert any variable to a valid
	// JSON string.
	//
	// Example:
	//    |alert()
	//       .id('{{ .Name }}')
	//       .details('''
	//<h1>{{ .ID }}</h1>
	//<b>{{ .Message }}</b>
	//Value: {{ index .Fields "value" }}
	//''')
	//       .email()
	//
	// Default: {{ json . }}
	Details string

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
	UseFlapping bool `tick:"Flapping"`
	//tick:ignore
	FlapLow float64
	//tick:ignore
	FlapHigh float64

	// Number of previous states to remember when computing flapping levels and
	// checking for state changes.
	// Minimum value is 2 in order to keep track of current and previous states.
	//
	// Default: 21
	History int64

	// Send alerts only on state changes.
	// tick:ignore
	IsStateChangesOnly bool `tick:"StateChangesOnly"`

	// Post the JSON alert data to the specified URL.
	// tick:ignore
	PostHandlers []*PostHandler `tick:"Post"`

	// Email handlers
	// tick:ignore
	EmailHandlers []*EmailHandler `tick:"Email"`

	// A commands to run when an alert triggers
	// tick:ignore
	ExecHandlers []*ExecHandler `tick:"Exec"`

	// Log JSON alert data to file. One event per line.
	// tick:ignore
	LogHandlers []*LogHandler `tick:"Log"`

	// Send alert to VictorOps.
	// tick:ignore
	VictorOpsHandlers []*VictorOpsHandler `tick:"VictorOps"`

	// Send alert to PagerDuty.
	// tick:ignore
	PagerDutyHandlers []*PagerDutyHandler `tick:"PagerDuty"`

	// Send alert to Sensu.
	// tick:ignore
	SensuHandlers []*SensuHandler `tick:"Sensu"`

	// Send alert to Slack.
	// tick:ignore
	SlackHandlers []*SlackHandler `tick:"Slack"`

	// Send alert to HipChat.
	// tick:ignore
	HipChatHandlers []*HipChatHandler `tick:"HipChat"`

	// Send alert to Alerta.
	// tick:ignore
	AlertaHandlers []*AlertaHandler `tick:"Alerta"`

	// Send alert to OpsGenie
	// tick:ignore
	OpsGenieHandlers []*OpsGenieHandler `tick:"OpsGenie"`

	// Send alert to Talk.
	// tick:ignore
	TalkHandlers []*TalkHandler `tick:"Talk"`
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
		Details: defaultDetailsTmpl,
	}
}

// Only sends events where the state changed.
// Each different alert level OK, INFO, WARNING, and CRITICAL
// are considered different states.
//
// Example:
//   stream
//       |from()
//           .measurement('cpu')
//       |window()
//            .period(10s)
//            .every(10s)
//       |alert()
//           .crit(lambda: "value" > 10)
//           .stateChangesOnly()
//           .slack()
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
func (a *AlertNode) Flapping(low, high float64) *AlertNode {
	a.UseFlapping = true
	a.FlapLow = low
	a.FlapHigh = high
	return a
}

// HTTP POST JSON alert data to a specified URL.
// tick:property
func (a *AlertNode) Post(url string) *PostHandler {
	post := &PostHandler{
		AlertNode: a,
		URL:       url,
	}
	a.PostHandlers = append(a.PostHandlers, post)
	return post
}

// tick:embedded:AlertNode.Email
type PostHandler struct {
	*AlertNode

	// The POST URL.
	// tick:ignore
	URL string
}

// Email the alert data.
//
// If the To list is empty, the To addresses from the configuration are used.
// The email subject is the AlertNode.Message property.
// The email body is the AlertNode.Details property.
// The emails are sent as HTML emails and so the body can contain html markup.
//
// If the 'smtp' section in the configuration has the option: global = true
// then all alerts are sent via email without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    |alert()
//       .id('{{ .Name }}')
//       // Email subject
//       .meassage('{{ .ID }}:{{ .Level }}')
//       //Email body as HTML
//       .details('''
//<h1>{{ .ID }}</h1>
//<b>{{ .Message }}</b>
//Value: {{ index .Fields "value" }}
//''')
//       .email()
//
// Send an email with custom subject and body.
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
//       state-changes-only =  true
//
// Example:
//    stream
//         |alert()
//
// Send email to 'oncall@example.com' from 'kapacitor@example.com'
//
// tick:property
func (a *AlertNode) Email(to ...string) *EmailHandler {
	em := &EmailHandler{
		AlertNode: a,
		ToList:    to,
	}
	a.EmailHandlers = append(a.EmailHandlers, em)
	return em
}

// Email AlertHandler
// tick:embedded:AlertNode.Email
type EmailHandler struct {
	*AlertNode

	// List of email recipients.
	// tick:ignore
	ToList []string
}

// Execute a command whenever an alert is triggered and pass the alert data over STDIN in JSON format.
// tick:property
func (a *AlertNode) Exec(executable string, args ...string) *ExecHandler {
	exec := &ExecHandler{
		AlertNode: a,
		Command:   append([]string{executable}, args...),
	}
	a.ExecHandlers = append(a.ExecHandlers, exec)
	return exec
}

// tick:embedded:AlertNode.Exec
type ExecHandler struct {
	*AlertNode

	// The command to execute
	// tick:ignore
	Command []string
}

// Log JSON alert data to file. One event per line.
// Must specify the absolute path to the log file.
// It will be created if it does not exist.
// Example:
//    stream
//         |alert()
//             .log('/tmp/alert')
//
// Example:
//    stream
//         |alert()
//             .log('/tmp/alert')
//             .mode(0644)
// tick:property
func (a *AlertNode) Log(filepath string) *LogHandler {
	log := &LogHandler{
		AlertNode: a,
		FilePath:  filepath,
		Mode:      defaultLogFileMode,
	}
	a.LogHandlers = append(a.LogHandlers, log)
	return log
}

// tick:embedded:AlertNode.Log
type LogHandler struct {
	*AlertNode

	// Absolute path the the log file.
	// It will be created if it does not exist.
	// tick:ignore
	FilePath string

	// File's mode and permissions, default is 0600
	Mode int64
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
//    stream
//         |alert()
//             .victorOps()
//
// Send alerts to VictorOps using the routing key in the configuration file.
//
// Example:
//    stream
//         |alert()
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
//    stream
//         |alert()
//
// Send alert to VictorOps using the default routing key, found in the configuration.
// tick:property
func (a *AlertNode) VictorOps() *VictorOpsHandler {
	vo := &VictorOpsHandler{
		AlertNode: a,
	}
	a.VictorOpsHandlers = append(a.VictorOpsHandlers, vo)
	return vo
}

// tick:embedded:AlertNode.VictorOps
type VictorOpsHandler struct {
	*AlertNode

	// The routing key to use for the alert.
	// Defaults to the value in the configuration if empty.
	RoutingKey string
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
//    stream
//         |alert()
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
//    stream
//         |alert()
//
// Send alert to PagerDuty.
// tick:property
func (a *AlertNode) PagerDuty() *PagerDutyHandler {
	pd := &PagerDutyHandler{
		AlertNode: a,
	}
	a.PagerDutyHandlers = append(a.PagerDutyHandlers, pd)
	return pd
}

// tick:embedded:AlertNode.PagerDuty
type PagerDutyHandler struct {
	*AlertNode
}

// Send the alert to HipChat.
// To allow Kapacitor to post to HipChat,
// go to the URL https://www.hipchat.com/docs/apiv2 for
// information on how to get your room id and tokens.
//
// Example:
//    [hipchat]
//      enabled = true
//      url = "https://orgname.hipchat.com/v2/room"
//      room = "4189212"
//      token = "9hiWoDOZ9IbmHsOTeST123ABciWTIqXQVFDo63h9"
//
// In order to not post a message every alert interval
// use AlertNode.StateChangesOnly so that only events
// where the alert changed state are posted to the room.
//
// Example:
//    stream
//         |alert()
//             .hipChat()
//
// Send alerts to HipChat room in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .hipChat()
//             .room('Kapacitor')
//
// Send alerts to HipChat room 'Kapacitor'

//
// If the 'hipchat' section in the configuration has the option: global = true
// then all alerts are sent to HipChat without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [hipchat]
//      enabled = true
//      url = "https://orgname.hipchat.com/v2/room"
//      room = "Test Room"
//      token = "9hiWoDOZ9IbmHsOTeST123ABciWTIqXQVFDo63h9"
//      global = true
//      state-changes-only = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to HipChat using default room 'Test Room'.
// tick:property
func (a *AlertNode) HipChat() *HipChatHandler {
	hipchat := &HipChatHandler{
		AlertNode: a,
	}
	a.HipChatHandlers = append(a.HipChatHandlers, hipchat)
	return hipchat
}

// tick:embedded:AlertNode.HipChat
type HipChatHandler struct {
	*AlertNode

	// HipChat room in which to post messages.
	// If empty uses the channel from the configuration.
	Room string

	// HipChat authentication token.
	// If empty uses the token from the configuration.
	Token string
}

// Send the alert to Alerta.
//
// Example:
//    [alerta]
//      enabled = true
//      url = "https://alerta.yourdomain"
//      token = "9hiWoDOZ9IbmHsOTeST123ABciWTIqXQVFDo63h9"
//      environment = "Production"
//      origin = "Kapacitor"
//
// In order to not post a message every alert interval
// use AlertNode.StateChangesOnly so that only events
// where the alert changed state are sent to Alerta.
//
// Send alerts to Alerta. The resource and event properties are required.
//
// Example:
//    stream
//         |alert()
//             .alerta()
//                 .resource('Hostname or service')
//                 .event('Something went wrong')
//
// Alerta also accepts optional alert information.
//
// Example:
//    stream
//         |alert()
//             .alerta()
//                 .resource('Hostname or service')
//                 .event('Something went wrong')
//                 .environment('Development')
//                 .group('Dev. Servers')
//
// NOTE: Alerta cannot be configured globally because of its required properties.
// tick:property
func (a *AlertNode) Alerta() *AlertaHandler {
	alerta := &AlertaHandler{
		AlertNode: a,
		Resource:  defaultAlertaResource,
		Group:     defaultAlertaGroup,
	}
	a.AlertaHandlers = append(a.AlertaHandlers, alerta)
	return alerta
}

const defaultAlertaResource = "{{ .Name }}"
const defaultAlertaGroup = "{{ .Group }}"

// tick:embedded:AlertNode.Alerta
type AlertaHandler struct {
	*AlertNode

	// Alerta authentication token.
	// If empty uses the token from the configuration.
	Token string

	// Alerta resource.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Name }}
	Resource string

	// Alerta environment.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Defaut is set from the configuration.
	Environment string

	// Alerta group.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Group }}
	Group string

	// Alerta value.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default is an empty string.
	Value string

	// Alerta origin.
	// If empty uses the origin from the configuration.
	Origin string

	// List of effected Services
	// tick:ignore
	Service []string `tick:"Services"`
}

// List of effected services.
// If not specified defaults to the Name of the stream.
func (a *AlertaHandler) Services(service ...string) *AlertaHandler {
	a.Service = service
	return a
}

// Send the alert to Sensu.
//
// Example:
//    [sensu]
//      enabled = true
//      url = "http://sensu:3030"
//      source = "Kapacitor"
//
// Example:
//    stream
//         |alert()
//             .sensu()
//
// Send alerts to Sensu client.
//
// tick:property
func (a *AlertNode) Sensu() *SensuHandler {
	sensu := &SensuHandler{
		AlertNode: a,
	}
	a.SensuHandlers = append(a.SensuHandlers, sensu)
	return sensu
}

// tick:embedded:AlertNode.Sensu
type SensuHandler struct {
	*AlertNode
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
//    stream
//         |alert()
//             .slack()
//
// Send alerts to Slack channel in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .slack()
//             .channel('#alerts')
//
// Send alerts to Slack channel '#alerts'
//
// Example:
//    stream
//         |alert()
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
//      state-changes-only = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to Slack using default channel '#general'.
// tick:property
func (a *AlertNode) Slack() *SlackHandler {
	slack := &SlackHandler{
		AlertNode: a,
	}
	a.SlackHandlers = append(a.SlackHandlers, slack)
	return slack
}

// tick:embedded:AlertNode.Slack
type SlackHandler struct {
	*AlertNode

	// Slack channel in which to post messages.
	// If empty uses the channel from the configuration.
	Channel string
}

// Send alert to OpsGenie.
// To use OpsGenie alerting you must first enable the 'Alert Ingestion API'
// in the 'Integrations' section of OpsGenie.
// Then place the API key from the URL into the 'opsgenie' section of the Kapacitor configuration.
//
// Example:
//    [opsgenie]
//      enabled = true
//      api-key = "xxxxx"
//      teams = ["everyone"]
//      recipients = ["jim", "bob"]
//
// With the correct configuration you can now use OpsGenie in TICKscripts.
//
// Example:
//    stream
//         |alert()
//             .opsGenie()
//
// Send alerts to OpsGenie using the teams and recipients in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .opsGenie()
//             .teams('team_rocket','team_test')
//
// Send alerts to OpsGenie with team set to 'team_rocket' and 'team_test'
//
// If the 'opsgenie' section in the configuration has the option: global = true
// then all alerts are sent to OpsGenie without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [opsgenie]
//      enabled = true
//      api-key = "xxxxx"
//      recipients = ["johndoe"]
//      global = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to OpsGenie using the default recipients, found in the configuration.
// tick:property
func (a *AlertNode) OpsGenie() *OpsGenieHandler {
	og := &OpsGenieHandler{
		AlertNode: a,
	}
	a.OpsGenieHandlers = append(a.OpsGenieHandlers, og)
	return og
}

// tick:embedded:AlertNode.OpsGenie
type OpsGenieHandler struct {
	*AlertNode

	// OpsGenie Teams.
	// tick:ignore
	TeamsList []string `tick:"Teams"`

	// OpsGenie Recipients.
	// tick:ignore
	RecipientsList []string `tick:"Recipients"`
}

// The list of teams to be alerted. If empty defaults to the teams from the configuration.
// tick:property
func (og *OpsGenieHandler) Teams(teams ...string) *OpsGenieHandler {
	og.TeamsList = teams
	return og
}

// The list of recipients to be alerted. If empty defaults to the recipients from the configuration.
// tick:property
func (og *OpsGenieHandler) Recipients(recipients ...string) *OpsGenieHandler {
	og.RecipientsList = recipients
	return og
}

// Send the alert to Talk.
// To use Talk alerting you must first follow the steps to create a new incoming webhook.
//
//    1. Go to the URL https:/account.jianliao.com/signin.
//    2. Sign in with you account. under the Team tab, click "Integrations".
//    3. Select "Customize service", click incoming Webhook "Add" button.
//    4. After choose the topic to connect with "xxx", click "Confirm Add" button.
//    5. Once the service is created, you'll see the "Generate Webhook url".
//
// Place the 'Generate Webhook url' into the 'Talk' section of the Kapacitor configuration as the option 'url'.
//
// Example:
//    [talk]
//      enabled = true
//      url = "https://jianliao.com/v2/services/webhook/uuid"
//      author_name = "Kapacitor"
//
// Example:
//    stream
//         |alert()
//             .talk()
//
// Send alerts to Talk client.
//
// tick:property
func (a *AlertNode) Talk() *TalkHandler {
	talk := &TalkHandler{
		AlertNode: a,
	}
	a.TalkHandlers = append(a.TalkHandlers, talk)
	return talk
}

// tick:embedded:AlertNode.Talk
type TalkHandler struct {
	*AlertNode
}
