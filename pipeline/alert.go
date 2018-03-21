package pipeline

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/pkg/errors"
)

// Number of previous states to remember when computing flapping percentage.
const defaultFlapHistory = 21

// Default template for constructing an ID
const defaultIDTmpl = "{{ .Name }}:{{ .Group }}"

// Default template for constructing a message.
const defaultMessageTmpl = "{{ .ID }} is {{ .Level }}"

// Default template for constructing a details message.
const defaultDetailsTmpl = "{{ json . }}"

// AlertNode struct wraps the default AlertNodeData
// tick:wraps:AlertNodeData
type AlertNode struct{ *AlertNodeData }

// An AlertNode can trigger an event of varying severity levels,
// and pass the event to alert handlers. The criteria for triggering
// an alert is specified via a [lambda expression](/kapacitor/latest/tick/expr/).
// See AlertNode.Info, AlertNode.Warn, and AlertNode.Crit below.
//
// Different event handlers can be configured for each AlertNode.
// Some handlers like Email, HipChat, Sensu, Slack, OpsGenie, VictorOps, PagerDuty, Telegram and Talk have a configuration
// option 'global' that indicates that all alerts implicitly use the handler.
//
// Available event handlers:
//
//    * log -- log alert data to file.
//    * post -- HTTP POST data to a specified URL.
//    * tcp -- Send data to a specified address via raw TCP.
//    * email -- Send and email with alert data.
//    * exec -- Execute a command passing alert data over STDIN.
//    * HipChat -- Post alert message to HipChat room.
//    * Alerta -- Post alert message to Alerta.
//    * Sensu -- Post alert message to Sensu client.
//    * Slack -- Post alert message to Slack channel.
//    * SNMPTraps -- Trigger SNMP traps.
//    * OpsGenie -- Send alert to OpsGenie.
//    * VictorOps -- Send alert to VictorOps.
//    * PagerDuty -- Send alert to PagerDuty.
//    * Pushover -- Send alert to Pushover.
//    * Talk -- Post alert message to Talk client.
//    * Telegram -- Post alert message to Telegram client.
//    * MQTT -- Post alert message to MQTT.
//
// See below for more details on configuring each handler.
//
// Each event that gets sent to a handler contains the following alert data:
//
//    * ID -- the ID of the alert, user defined.
//    * Message -- the alert message, user defined.
//    * Details -- the alert details, user defined HTML content.
//    * Time -- the time the alert occurred.
//    * Duration -- the duration of the alert in nanoseconds.
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
//           .tcp("exampleendpoint.com:5678")
//           .email('oncall@example.com')
//
//
// Each expression maintains its own state.
// The order of execution for the expressions is not considered to be deterministic.
// For each point an expression may or may not be evaluated.
// If no expression is true then the alert is considered to be in the OK state.
//
// Kapacitor supports alert reset expressions.
// This way when an alert enters a state, it can only be lowered in severity if its reset expression evaluates to true.
//
// Example:
//   stream
//       |from()
//           .measurement('cpu')
//           .where(lambda: "host" == 'serverA')
//           .groupBy('host')
//       |alert()
//           .info(lambda: "value" > 60)
//           .infoReset(lambda: "value" < 50)
//           .warn(lambda: "value" > 70)
//           .warnReset(lambda: "value" < 60)
//           .crit(lambda: "value" > 80)
//           .critReset(lambda: "value" < 70)
//
// For example given the following values:
//     61 73 64 85 62 56 47
// The corresponding alert states are:
//     INFO WARNING WARNING CRITICAL INFO INFO OK
//
// Available Statistics:
//
//    * alerts_triggered -- Total number of alerts triggered
//    * oks_triggered -- Number of OK alerts triggered
//    * infos_triggered -- Number of Info alerts triggered
//    * warns_triggered -- Number of Warn alerts triggered
//    * crits_triggered -- Number of Crit alerts triggered
//
type AlertNodeData struct {
	chainnode

	// Category places this alert in a named category.
	// Categories are used to inhibit alerts.
	Category string `json:"category"`

	// Topic specifies the name of an alert topic to which,
	// alerts will be published.
	// Alert handlers can be configured per topic, see the API documentation.
	Topic string `json:"topic"`

	// Template for constructing a unique ID for a given alert.
	//
	// Available template data:
	//
	//    * Name -- Measurement name.
	//    * TaskName -- The name of the task
	//    * Group -- Concatenation of all group-by tags of the form [key=value,]+.
	//        If no groupBy is performed equal to literal 'nil'.
	//    * Tags -- Map of tags. Use '{{ index .Tags "key" }}' to get a specific tag value.
	//    * ServerInfo -- Information about the running server. Available nested fields are:
	//        Hostname, ClusterID and ServerID.
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
	Id string `json:"alertId"`

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
	//    * Duration -- The duration of the alert.
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
	Message string `json:"message"`

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
	Details string `json:"details"`

	// Filter expression for the INFO alert level.
	// An empty value indicates the level is invalid and is skipped.
	Info *ast.LambdaNode `json:"info"`
	// Filter expression for the WARNING alert level.
	// An empty value indicates the level is invalid and is skipped.
	Warn *ast.LambdaNode `json:"warn"`
	// Filter expression for the CRITICAL alert level.
	// An empty value indicates the level is invalid and is skipped.
	Crit *ast.LambdaNode `json:"crit"`

	// Filter expression for reseting the INFO alert level to lower level.
	InfoReset *ast.LambdaNode `json:"infoReset"`
	// Filter expression for reseting the WARNING alert level to lower level.
	WarnReset *ast.LambdaNode `json:"warnReset"`
	// Filter expression for reseting the CRITICAL alert level to lower level.
	CritReset *ast.LambdaNode `json:"critReset"`

	//tick:ignore
	UseFlapping bool `tick:"Flapping" json:"useFlapping"`
	//tick:ignore
	FlapLow float64 `json:"flapLow"`
	//tick:ignore
	FlapHigh float64 `json:"flapHigh"`

	// Number of previous states to remember when computing flapping levels and
	// checking for state changes.
	// Minimum value is 2 in order to keep track of current and previous states.
	//
	// Default: 21
	History int64 `json:"history"`

	// Optional tag key to use when tagging the data with the alert level.
	LevelTag string `json:"levelTag"`
	// Optional field key to add to the data, containing the alert level as a string.
	LevelField string `json:"levelField"`

	// Optional field key to add to the data, containing the alert message.
	MessageField string `json:"messageField"`

	// Optional field key to add the alert duration to the data.
	// The duration is always in units of nanoseconds.
	DurationField string `json:"durationField"`

	// Optional tag key to use when tagging the data with the alert ID.
	IdTag string `json:"idTag"`
	// Optional field key to add to the data, containing the alert ID as a string.
	IdField string `json:"idField"`

	// Indicates an alert should trigger only if all points in a batch match the criteria
	// tick:ignore
	AllFlag bool `tick:"All" json:"all"`

	// Do not send recovery events.
	// tick:ignore
	NoRecoveriesFlag bool `tick:"NoRecoveries" json:"noRecoveries"`

	// Send alerts only on state changes.
	// tick:ignore
	IsStateChangesOnly bool `tick:"StateChangesOnly" json:"stateChangesOnly"`

	// Maximum interval to ignore non state changed events
	// tick:ignore
	StateChangesOnlyDuration time.Duration `json:"stateChangesOnlyDuration"`

	// Inhibitors
	// tick:ignore
	Inhibitors []Inhibitor `tick:"Inhibit" json:"inhibitors"`

	// Post the JSON alert data to the specified URL.
	// tick:ignore
	HTTPPostHandlers []*AlertHTTPPostHandler `tick:"Post" json:"post"`

	// Send the JSON alert data to the specified endpoint via TCP.
	// tick:ignore
	TcpHandlers []*TcpHandler `tick:"Tcp" json:"tcp"`

	// Email handlers
	// tick:ignore
	EmailHandlers []*EmailHandler `tick:"Email" json:"email"`

	// A commands to run when an alert triggers
	// tick:ignore
	ExecHandlers []*ExecHandler `tick:"Exec" json:"exec"`

	// Log JSON alert data to file. One event per line.
	// tick:ignore
	LogHandlers []*LogHandler `tick:"Log" json:"log"`

	// Send alert to VictorOps.
	// tick:ignore
	VictorOpsHandlers []*VictorOpsHandler `tick:"VictorOps" json:"victorOps"`

	// Send alert to PagerDuty.
	// tick:ignore
	PagerDutyHandlers []*PagerDutyHandler `tick:"PagerDuty" json:"pagerDuty"`

	// Send alert to PagerDuty API v2.
	// tick:ignore
	PagerDuty2Handlers []*PagerDuty2Handler `tick:"PagerDuty2" json:"pagerDuty2"`

	// Send alert to Pushover.
	// tick:ignore
	PushoverHandlers []*PushoverHandler `tick:"Pushover" json:"pushover"`

	// Send alert to Sensu.
	// tick:ignore
	SensuHandlers []*SensuHandler `tick:"Sensu" json:"sensu"`

	// Send alert to Slack.
	// tick:ignore
	SlackHandlers []*SlackHandler `tick:"Slack" json:"slack"`

	// Send alert to Telegram.
	// tick:ignore
	TelegramHandlers []*TelegramHandler `tick:"Telegram" json:"telegram"`

	// Send alert to HipChat.
	// tick:ignore
	HipChatHandlers []*HipChatHandler `tick:"HipChat" json:"hipChat"`

	// Send alert to Alerta.
	// tick:ignore
	AlertaHandlers []*AlertaHandler `tick:"Alerta" json:"alerta"`

	// Send alert to OpsGenie
	// tick:ignore
	OpsGenieHandlers []*OpsGenieHandler `tick:"OpsGenie" json:"opsGenie"`

	// Send alert to OpsGenie using v2 API
	// tick:ignore
	OpsGenie2Handlers []*OpsGenie2Handler `tick:"OpsGenie2" json:"opsGenie2"`

	// Send alert to Talk.
	// tick:ignore
	TalkHandlers []*TalkHandler `tick:"Talk" json:"talk"`

	// Send alert to MQTT
	// tick:ignore
	MQTTHandlers []*MQTTHandler `tick:"Mqtt" json:"mqtt"`

	// Send alert using SNMPtraps.
	// tick:ignore
	SNMPTrapHandlers []*SNMPTrapHandler `tick:"SnmpTrap" json:"snmpTrap"`

	// Send alert to Kafka topic
	// tick:ignore
	KafkaHandlers []*KafkaHandler `tick:"Kafka" json:"kafka"`
}

func newAlertNode(wants EdgeType) *AlertNode {
	a := &AlertNode{
		AlertNodeData: &AlertNodeData{
			chainnode: newBasicChainNode("alert", wants, wants),
			History:   defaultFlapHistory,
			Id:        defaultIDTmpl,
			Message:   defaultMessageTmpl,
			Details:   defaultDetailsTmpl,
		},
	}
	return a
}

// MarshalJSON converts AlertNode to JSON
// tick:ignore
func (n *AlertNode) MarshalJSON() ([]byte, error) {
	type Alias AlertNodeData
	var raw = &struct {
		TypeOf
		*Alias
	}{
		TypeOf: TypeOf{
			Type: "alert",
			ID:   n.ID(),
		},
		Alias: (*Alias)(n.AlertNodeData),
	}

	return json.Marshal(raw)
}

// UnmarshalJSON converts JSON to an AlertNode
// tick:ignore
func (n *AlertNode) UnmarshalJSON(data []byte) error {
	type Alias AlertNode
	var raw = &struct {
		TypeOf
		*Alias
	}{
		Alias: (*Alias)(n),
	}
	err := json.Unmarshal(data, raw)
	if err != nil {
		return err
	}
	if raw.Type != "alert" {
		return fmt.Errorf("error unmarshaling node %d of type %s as AlertNode", raw.ID, raw.Type)
	}
	n.setID(raw.ID)
	return nil
}

//tick:ignore
func (n *AlertNodeData) ChainMethods() map[string]reflect.Value {
	return map[string]reflect.Value{
		"Log": reflect.ValueOf(n.chainnode.Log),
	}
}

func (n *AlertNodeData) validate() error {
	for _, snmp := range n.SNMPTrapHandlers {
		if err := snmp.validate(); err != nil {
			return errors.Wrapf(err, "invalid SNMP trap %q", snmp.TrapOid)
		}
	}

	for _, post := range n.HTTPPostHandlers {
		if err := post.validate(); err != nil {
			return errors.Wrap(err, "invalid post")
		}
	}
	return nil
}

// Indicates an alert should trigger only if all points in a batch match the criteria.
// Does not apply to stream alerts.
// tick:property
func (n *AlertNodeData) All() *AlertNodeData {
	n.AllFlag = true
	return n
}

// Do not send recovery alerts.
// tick:property
func (n *AlertNodeData) NoRecoveries() *AlertNodeData {
	n.NoRecoveriesFlag = true
	return n
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
// An optional maximum interval duration can be provided.
// An event will not be ignore (aka trigger an alert) if more than the maximum interval has elapsed
// since the last alert.
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
//           .stateChangesOnly(10m)
//           .slack()
//
// The above usage will only trigger alerts to slack on state changes or at least every 10 minutes.
//
// tick:property
func (n *AlertNodeData) StateChangesOnly(maxInterval ...time.Duration) *AlertNodeData {
	n.IsStateChangesOnly = true
	if len(maxInterval) == 1 {
		n.StateChangesOnlyDuration = maxInterval[0]
	}
	return n
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
func (n *AlertNodeData) Flapping(low, high float64) *AlertNodeData {
	n.UseFlapping = true
	n.FlapLow = low
	n.FlapHigh = high
	return n
}

// Inhibit other alerts in a category.
// The equal tags provides a list of tags that must be equal in order for an alert event to be inhibited.
//
// The following two TICKscripts demonstrate how to use the inhibit feature.
//
// Example:
//    //cpu_alert.tick
//    stream
//        |from()
//            .measurement('cpu')
//            .groupBy('host')
//        |alert()
//            .category('system_alerts')
//            .crit(lambda: "usage_idle" < 10.0)
//
//    //host_alert.tick
//    stream
//        |from()
//            .measurement('uptime')
//            .groupBy('host')
//        |deadman(0.0, 1m)
//            .inhibit('system_alerts', 'host')
//
// The deadman is a kind of alert node and so can be used to inhibit all alerts in the `system_alerts` category when it triggers.
// The 'host` argument to the inhibit function says that the host tag must be equal between the cpu alert and the host alert in order for it to be inhibited.
// This has the effect of the deadman alerts only inhibits cpu alerts for hosts are are currently dead.
//
// tick:property
func (n *AlertNodeData) Inhibit(category string, equalTags ...string) *AlertNodeData {
	n.Inhibitors = append(n.Inhibitors, Inhibitor{
		Category:  category,
		EqualTags: equalTags,
	})
	return n
}

// Inhibitor represents a single alert inhibitor
// tick:ignore
type Inhibitor struct {
	Category  string   `json:"category"`
	EqualTags []string `json:"equalTags"`
}

// HTTP POST JSON alert data to a specified URL.
//
// Example:
//    stream
//         |alert()
//             .post()
//                 .endpoint('example')
//
// Example:
//    stream
//         |alert()
//             .post('http://example.com')
//
// tick:property
func (n *AlertNodeData) Post(urls ...string) *AlertHTTPPostHandler {
	post := &AlertHTTPPostHandler{
		AlertNodeData: n,
	}
	n.HTTPPostHandlers = append(n.HTTPPostHandlers, post)

	if len(urls) == 0 {
		return post
	}

	post.URL = urls[0]
	return post
}

// tick:embedded:AlertNode.Post
type AlertHTTPPostHandler struct {
	*AlertNodeData `json:"-"`

	// The POST URL.
	// tick:ignore
	URL string `json:"url"`

	// Name of the endpoint to be used, as is defined in the configuration file
	Endpoint string `json:"endpoint"`

	// tick:ignore
	Headers map[string]string `tick:"Header" json:"headers"`

	// tick:ignore
	CaptureResponseFlag bool `tick:"CaptureResponse" json:"captureResponse"`

	// Timeout for HTTP Post
	Timeout time.Duration `json:"timeout"`
}

// Set a header key and value on the post request.
// Setting the Authenticate header is not allowed from within TICKscript,
// please use the configuration file to specify sensitive headers.
//
// Example:
//    stream
//         |alert()
//             .post()
//                 .endpoint('example')
//                 .header('a','b')
// tick:property
func (a *AlertHTTPPostHandler) Header(k, v string) *AlertHTTPPostHandler {
	if a.Headers == nil {
		a.Headers = map[string]string{}
	}

	a.Headers[k] = v
	return a
}

// CaptureResponse indicates that the HTTP response should be read and logged if
// the status code was not an 2xx code.
// tick:property
func (a *AlertHTTPPostHandler) CaptureResponse() *AlertHTTPPostHandler {
	a.CaptureResponseFlag = true
	return a
}

func (a *AlertHTTPPostHandler) validate() error {
	for k := range a.Headers {
		if strings.ToUpper(k) == "AUTHENTICATE" {
			return errors.New("cannot set 'authenticate' header")
		}
	}
	return nil
}

// Send JSON alert data to a specified address over TCP.
// tick:property
func (n *AlertNodeData) Tcp(address string) *TcpHandler {
	tcp := &TcpHandler{
		AlertNodeData: n,
		Address:       address,
	}
	n.TcpHandlers = append(n.TcpHandlers, tcp)
	return tcp
}

// tick:embedded:AlertNode.Tcp
type TcpHandler struct {
	*AlertNodeData `json:"-"`

	// The endpoint address.
	Address string `json:"address"`
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
//       .message('{{ .ID }}:{{ .Level }}')
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
func (n *AlertNodeData) Email(to ...string) *EmailHandler {
	em := &EmailHandler{
		AlertNodeData: n,
		ToList:        to,
	}
	n.EmailHandlers = append(n.EmailHandlers, em)
	return em
}

// Email AlertHandler
// tick:embedded:AlertNode.Email
type EmailHandler struct {
	*AlertNodeData `json:"-"`

	// List of email recipients.
	// tick:ignore
	ToList []string `tick:"To" json:"to"`
}

// Define the To addresses for the email alert.
// Multiple calls append to the existing list of addresses.
// If empty uses the addresses from the configuration.
//
// Example:
//    |alert()
//       .id('{{ .Name }}')
//       // Email subject
//       .message('{{ .ID }}:{{ .Level }}')
//       //Email body as HTML
//       .details('''
//<h1>{{ .ID }}</h1>
//<b>{{ .Message }}</b>
//Value: {{ index .Fields "value" }}
//''')
//       .email('admin@example.com')
//         .to('oncall@example.com')
//         .to('support@example.com')
//
// All three email addresses will receive the alert message.
//
// Passing addresses to the `email` property directly or using the `email.to` property is the same.
// tick:property
func (h *EmailHandler) To(to ...string) *EmailHandler {
	h.ToList = append(h.ToList, to...)
	return h
}

// Execute a command whenever an alert is triggered and pass the alert data over STDIN in JSON format.
// tick:property
func (n *AlertNodeData) Exec(executable string, args ...string) *ExecHandler {
	exec := &ExecHandler{
		AlertNodeData: n,
		Command:       append([]string{executable}, args...),
	}
	n.ExecHandlers = append(n.ExecHandlers, exec)
	return exec
}

// tick:embedded:AlertNode.Exec
type ExecHandler struct {
	*AlertNodeData `json:"-"`

	// The command to execute
	// tick:ignore
	Command []string `json:"command"`
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
func (n *AlertNodeData) Log(filepath string) *LogHandler {
	log := &LogHandler{
		AlertNodeData: n,
		FilePath:      filepath,
	}
	n.LogHandlers = append(n.LogHandlers, log)
	return log
}

// tick:embedded:AlertNode.Log
type LogHandler struct {
	*AlertNodeData `json:"-"`

	// Absolute path the the log file.
	// It will be created if it does not exist.
	// tick:ignore
	FilePath string `json:"filePath"`

	// File's mode and permissions, default is 0600
	// NOTE: The leading 0 is required to interpret the value as an octal integer.
	Mode int64 `json:"mode"`
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
func (n *AlertNodeData) VictorOps() *VictorOpsHandler {
	vo := &VictorOpsHandler{
		AlertNodeData: n,
	}
	n.VictorOpsHandlers = append(n.VictorOpsHandlers, vo)
	return vo
}

// tick:embedded:AlertNode.VictorOps
type VictorOpsHandler struct {
	*AlertNodeData `json:"-"`

	// The routing key to use for the alert.
	// Defaults to the value in the configuration if empty.
	RoutingKey string `json:"routingKey"`
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
func (n *AlertNodeData) PagerDuty() *PagerDutyHandler {
	pd := &PagerDutyHandler{
		AlertNodeData: n,
	}
	n.PagerDutyHandlers = append(n.PagerDutyHandlers, pd)
	return pd
}

// tick:embedded:AlertNode.PagerDuty
type PagerDutyHandler struct {
	*AlertNodeData `json:"-"`

	// The service key to use for the alert.
	// Defaults to the value in the configuration if empty.
	ServiceKey string `json:"serviceKey"`
}

// Send the alert to PagerDuty API v2.
// To use PagerDuty alerting you must first follow the steps to enable a new 'Generic API' service.
// NOTE: the API v2 endpoint is different and requires a new configuration in order to process/handle alerts
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
//    [pagerduty2]
//      enabled = true
//      service-key = "xxxxxxxxx"
//
// With the correct configuration you can now use PagerDuty in TICKscripts.
//
// Example:
//    stream
//         |alert()
//             .pagerDuty2()
//
// If the 'pagerduty' section in the configuration has the option: global = true
// then all alerts are sent to PagerDuty without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [pagerduty2]
//      enabled = true
//      service-key = "xxxxxxxxx"
//      global = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to PagerDuty API v2.
// tick:property
func (n *AlertNodeData) PagerDuty2() *PagerDuty2Handler {
	pd2 := &PagerDuty2Handler{
		AlertNodeData: n,
	}
	n.PagerDuty2Handlers = append(n.PagerDuty2Handlers, pd2)
	return pd2
}

// tick:embedded:AlertNode.PagerDuty
type PagerDuty2Handler struct {
	*AlertNodeData `json:"-"`

	// The service key to use for the alert.
	// Defaults to the value in the configuration if empty.
	ServiceKey string `json:"serviceKey"`
}

// Send the alert to HipChat.
// For step-by-step instructions on setting up Kapacitor with HipChat, see the [Event Handler Setup Guide](https://docs.influxdata.com//kapacitor/latest/guides/event-handler-setup/#hipchat-setup).
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
func (n *AlertNodeData) HipChat() *HipChatHandler {
	hipchat := &HipChatHandler{
		AlertNodeData: n,
	}
	n.HipChatHandlers = append(n.HipChatHandlers, hipchat)
	return hipchat
}

// tick:embedded:AlertNode.HipChat
type HipChatHandler struct {
	*AlertNodeData `json:"-"`

	// HipChat room in which to post messages.
	// If empty uses the channel from the configuration.
	Room string `json:"room"`

	// HipChat authentication token.
	// If empty uses the token from the configuration.
	Token string `json:"token"`
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
//                 .timeout(5m)
//
// NOTE: Alerta cannot be configured globally because of its required properties.
// tick:property
func (n *AlertNodeData) Alerta() *AlertaHandler {
	alerta := &AlertaHandler{
		AlertNodeData: n,
	}
	n.AlertaHandlers = append(n.AlertaHandlers, alerta)
	return alerta
}

// tick:embedded:AlertNode.Alerta
type AlertaHandler struct {
	*AlertNodeData `json:"-"`

	// Alerta authentication token.
	// If empty uses the token from the configuration.
	Token string `json:"token"`

	// Alerta resource.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Name }}
	Resource string `json:"resource"`

	// Alerta event.
	// Can be a template and has access to the same data as the idInfo property.
	// Default: {{ .ID }}
	Event string `json:"event"`

	// Alerta environment.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Defaut is set from the configuration.
	Environment string `json:"environment"`

	// Alerta group.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default: {{ .Group }}
	Group string `json:"group"`

	// Alerta value.
	// Can be a template and has access to the same data as the AlertNode.Details property.
	// Default is an empty string.
	Value string `json:"value"`

	// Alerta origin.
	// If empty uses the origin from the configuration.
	Origin string `json:"origin"`

	// List of effected Services
	// tick:ignore
	Service []string `tick:"Services" json:"service"`

	// Alerta timeout.
	// Default: 24h
	Timeout time.Duration `json:"timeout"`
}

// List of effected services.
// If not specified defaults to the Name of the stream.
// tick:property
func (a *AlertaHandler) Services(service ...string) *AlertaHandler {
	a.Service = service
	return a
}

// Send alert to an MQTT broker
// tick:property
func (n *AlertNodeData) Mqtt(topic string) *MQTTHandler {
	m := &MQTTHandler{
		AlertNodeData: n,
		Topic:         topic,
	}
	n.MQTTHandlers = append(n.MQTTHandlers, m)
	return m
}

// tick:embedded:AlertNode.Mqtt
type MQTTHandler struct {
	*AlertNodeData `json:"-"`

	// BrokerName is the name of the configured MQTT broker to use when publishing the alert.
	// If empty defaults to the configured default broker.
	BrokerName string `json:"brokerName"`

	// The topic where alerts will be dispatched to
	Topic string `json:"topic"`

	// The Qos that will be used to deliver the alerts
	//
	// Valid values are:
	//
	//    * 0 - At most once delivery
	//    * 1 - At least once delivery
	//    * 2 - Exactly once delivery
	//
	Qos int64 `json:"qos"`

	// Retained indicates whether this alert should be delivered to
	// clients that were not connected to the broker at the time of the alert.
	Retained bool `json:"retained"`
}

// Send the alert to Sensu.
//
// Example:
//    [sensu]
//      enabled = true
//      addr = "sensu:3030"
//      source = "Kapacitor"
//      handlers = ["sns","slack"]
//
// Example:
//    stream
//         |alert()
//             .sensu()
//
// Send alerts to Sensu client.
//
// Example:
//    stream
//         |alert()
//             .sensu()
//             .handlers('sns','slack')
//
// Send alerts to Sensu specifying the handlers
//
// tick:property
func (n *AlertNodeData) Sensu() *SensuHandler {
	sensu := &SensuHandler{
		AlertNodeData: n,
	}
	n.SensuHandlers = append(n.SensuHandlers, sensu)
	return sensu
}

// tick:embedded:AlertNode.Sensu
type SensuHandler struct {
	*AlertNodeData `json:"-"`

	// Sensu source in which to post messages.
	// If empty uses the Source from the configuration.
	Source string `json:"source"`

	// Sensu handler list
	// If empty uses the handler list from the configuration
	// tick:ignore
	HandlersList []string `tick:"Handlers" json:"handlers"`
}

// List of effected services.
// If not specified defaults to the Name of the stream.
// tick:property
func (s *SensuHandler) Handlers(handlers ...string) *SensuHandler {
	s.HandlersList = handlers
	return s
}

// Send the alert to Pushover.
// Register your application with Pushover at
// https://pushover.net/apps/build to get a
// Pushover token.
//
// Alert Level Mapping:
// OK - Sends a -2 priority level.
// Info - Sends a -1 priority level.
// Warning - Sends a 0 priority level.
// Critical - Sends a 1 priority level.
//
// Example:
//    [pushover]
//      enabled = true
//      token = "9hiWoDOZ9IbmHsOTeST123ABciWTIqXQVFDo63h9"
//      user_key = "Pushover"
//
// Example:
//    stream
//         |alert()
//             .pushover()
//              .sound('siren')
//              .user_key('other user')
//              .device('mydev')
//              .title('mytitle')
//              .URL('myurl')
//              .URLTitle('mytitle')
//
// Send alerts to Pushover.
//
// tick:property
func (n *AlertNodeData) Pushover() *PushoverHandler {
	pushover := &PushoverHandler{
		AlertNodeData: n,
	}
	n.PushoverHandlers = append(n.PushoverHandlers, pushover)
	return pushover
}

// tick:embedded:AlertNode.Pushover
type PushoverHandler struct {
	*AlertNodeData `json:"-"`

	// User/Group key of your user (or you), viewable when logged
	// into the Pushover dashboard. Often referred to as USER_KEY
	// in the Pushover documentation.
	// If empty uses the user from the configuration.
	UserKey string `json:"userKey"`

	// Users device name to send message directly to that device,
	// rather than all of a user's devices (multiple device names may
	// be separated by a comma)
	Device string `json:"device"`

	// Your message's title, otherwise your apps name is used
	Title string `json:"title"`

	// A supplementary URL to show with your message
	URL string `json:"url"`

	// A title for your supplementary URL, otherwise just URL is shown
	URLTitle string `json:"urlTitle"`

	// The name of one of the sounds supported by the device clients to override
	// the user's default sound choice
	Sound string `json:"sound"`
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
// Send alerts to the default worskace Slack channel in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .slack()
//             .channel('#alerts')
//
// Send alerts to the default workspace with Slack channel '#alerts'
//
// Example:
//    stream
//         |alert()
//             .slack()
//             .channel('@jsmith')
//
// Send alert to user '@jsmith'
//
// Example:
// stream
//      |alert()
//          .slack()
//          .workspace('opencommunity')
//          .channel('#support')
//
// send alerts to the opencommunity workspace on the channel '#support'
//
// If the 'slack' section in the configuration has the option: global = true
// then all alerts are sent to Slack without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [[slack]]
//      enabled = true
//      default = true
//      workspace = examplecorp
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
func (n *AlertNodeData) Slack() *SlackHandler {
	slack := &SlackHandler{
		AlertNodeData: n,
	}
	n.SlackHandlers = append(n.SlackHandlers, slack)
	return slack
}

// tick:embedded:AlertNode.Slack
type SlackHandler struct {
	*AlertNodeData `json:"-"`

	// The workspace to publish the alert to.  If empty defaults to the configured
	// default broker.
	Workspace string `json:"workspace"`

	// Slack channel in which to post messages.
	// If empty uses the channel from the configuration.
	Channel string `json:"channel"`

	// Username of the Slack bot.
	// If empty uses the username from the configuration.
	Username string `json:"username"`

	// IconEmoji is an emoji name surrounded in ':' characters.
	// The emoji image will replace the normal user icon for the slack bot.
	IconEmoji string `json:"iconEmoji"`
}

// Send the alert to Telegram.
// For step-by-step instructions on setting up Kapacitor with Telegram, see the [Event Handler Setup Guide](https://docs.influxdata.com//kapacitor/latest/guides/event-handler-setup/#telegram-setup).
// To allow Kapacitor to post to Telegram,
//
// Example:
//    [telegram]
//      enabled = true
//      token = "123456789:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
//      chat-id = "xxxxxxxxx"
//      parse-mode = "Markdown"
//	disable-web-page-preview = true
//	disable-notification = false
//
// In order to not post a message every alert interval
// use AlertNode.StateChangesOnly so that only events
// where the alert changed state are posted to the chat-id.
//
// Example:
//    stream
//         |alert()
//             .telegram()
//
// Send alerts to Telegram chat-id in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .telegram()
//             .chatId('xxxxxxx')
//
// Send alerts to Telegram user/group 'xxxxxx'
//
// If the 'telegram' section in the configuration has the option: global = true
// then all alerts are sent to Telegram without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [telegram]
//      enabled = true
//      token = "123456789:xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
//      chat-id = "xxxxxxxxx"
//      global = true
//      state-changes-only = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to Telegram using default chat-id 'xxxxxxxx'.
// tick:property
func (n *AlertNodeData) Telegram() *TelegramHandler {
	telegram := &TelegramHandler{
		AlertNodeData: n,
	}
	n.TelegramHandlers = append(n.TelegramHandlers, telegram)
	return telegram
}

// tick:embedded:AlertNode.Telegram
type TelegramHandler struct {
	*AlertNodeData `json:"-"`

	// Telegram user/group ID to post messages to.
	// If empty uses the chati-d from the configuration.
	ChatId string `json:"chatId"`
	// Parse node, defaults to Mardown
	// If empty uses the parse-mode from the configuration.
	ParseMode string `json:"parseMode"`
	// Web Page preview
	// If empty uses the disable-web-page-preview from the configuration.
	// tick:ignore
	IsDisableWebPagePreview bool `tick:"DisableWebPagePreview" json:"disableWebPagePreview"`
	// Disables Notification
	// If empty uses the disable-notification from the configuration.
	// tick:ignore
	IsDisableNotification bool `tick:"DisableNotification" json:"disableNotification"`
}

// Disables the Notification. If empty defaults to the configuration.
// tick:property
func (tel *TelegramHandler) DisableNotification() *TelegramHandler {
	tel.IsDisableNotification = true
	return tel
}

// Disables the WebPagePreview. If empty defaults to the configuration.
// tick:property
func (tel *TelegramHandler) DisableWebPagePreview() *TelegramHandler {
	tel.IsDisableWebPagePreview = true
	return tel
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
func (n *AlertNodeData) OpsGenie() *OpsGenieHandler {
	og := &OpsGenieHandler{
		AlertNodeData: n,
	}
	n.OpsGenieHandlers = append(n.OpsGenieHandlers, og)
	return og
}

// tick:embedded:AlertNode.OpsGenie
type OpsGenieHandler struct {
	*AlertNodeData `json:"-"`

	// OpsGenie Teams.
	// tick:ignore
	TeamsList []string `tick:"Teams" json:"teams"`

	// OpsGenie Recipients.
	// tick:ignore
	RecipientsList []string `tick:"Recipients" json:"recipients"`
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

// Send alert to OpsGenie using the v2 API.
// To use OpsGenie2 alerting you must first enable the 'Alert Ingestion API'
// in the 'Integrations' section of OpsGenie2.
// Then place the API key from the URL into the 'opsgenie2' section of the Kapacitor configuration.
//
// Example:
//    [opsgenie2]
//      enabled = true
//      api-key = "xxxxx"
//      teams = ["everyone"]
//      recipients = ["jim", "bob"]
//
// With the correct configuration you can now use OpsGenie2 in TICKscripts.
//
// Example:
//    stream
//         |alert()
//             .opsGenie()
//
// Send alerts to OpsGenie2 using the teams and recipients in the configuration file.
//
// Example:
//    stream
//         |alert()
//             .opsGenie()
//             .teams('team_rocket','team_test')
//
// Send alerts to OpsGenie2 with team set to 'team_rocket' and 'team_test'
//
// If the 'opsgenie2' section in the configuration has the option: global = true
// then all alerts are sent to OpsGenie2 without the need to explicitly state it
// in the TICKscript.
//
// Example:
//    [opsgenie2]
//      enabled = true
//      api-key = "xxxxx"
//      recipients = ["johndoe"]
//      global = true
//
// Example:
//    stream
//         |alert()
//
// Send alert to OpsGenie2 using the default recipients, found in the configuration.
// tick:property
func (n *AlertNodeData) OpsGenie2() *OpsGenie2Handler {
	og := &OpsGenie2Handler{
		AlertNodeData: n,
	}
	n.OpsGenie2Handlers = append(n.OpsGenie2Handlers, og)
	return og
}

// tick:embedded:AlertNode.OpsGenie2
type OpsGenie2Handler struct {
	*AlertNodeData `json:"-"`

	// OpsGenie2 Teams.
	// tick:ignore
	TeamsList []string `tick:"Teams" json:"teams"`

	// OpsGenie2 Recipients.
	// tick:ignore
	RecipientsList []string `tick:"Recipients" json:"recipients"`
}

// The list of teams to be alerted. If empty defaults to the teams from the configuration.
// tick:property
func (og *OpsGenie2Handler) Teams(teams ...string) *OpsGenie2Handler {
	og.TeamsList = teams
	return og
}

// The list of recipients to be alerted. If empty defaults to the recipients from the configuration.
// tick:property
func (og *OpsGenie2Handler) Recipients(recipients ...string) *OpsGenie2Handler {
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
func (n *AlertNodeData) Talk() *TalkHandler {
	talk := &TalkHandler{
		AlertNodeData: n,
	}
	n.TalkHandlers = append(n.TalkHandlers, talk)
	return talk
}

// tick:embedded:AlertNode.Talk
type TalkHandler struct {
	*AlertNodeData `json:"-"`
}

// Send the alert using SNMP traps.
// To allow Kapacitor to post SNMP traps,
//
// Example:
//    [snmptrap]
//      enabled = true
//      addr = "127.0.0.1:9162"
//      community = "public"
//
// Example:
//    stream
//         |alert()
//             .snmpTrap('1.1.1.1')
//                 .data('1.3.6.1.2.1.1.7', 'i', '{{ index .Field "value" }}')
//
// Send alerts to `target-ip:target-port` on OID '1.3.6.1.2.1.1.7'
//
// tick:property
func (n *AlertNodeData) SnmpTrap(trapOid string) *SNMPTrapHandler {
	snmpTrap := &SNMPTrapHandler{
		AlertNodeData: n,
		TrapOid:       trapOid,
	}
	n.SNMPTrapHandlers = append(n.SNMPTrapHandlers, snmpTrap)
	return snmpTrap
}

// SNMPTrap AlertHandler
// tick:embedded:AlertNode.SnmpTrap
type SNMPTrapHandler struct {
	*AlertNodeData `json:"-"`

	// TrapOid
	// tick:ignore
	TrapOid string `json:"trapOid"`

	// List of trap data.
	// tick:ignore
	DataList []SNMPData `tick:"Data" json:"data"`
}

// tick:ignore
type SNMPData struct {
	Oid   string `json:"oid"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// Define Data for SNMP Trap alert.
// Multiple calls append to the existing list of data.
//
// Available types:
//
// | Abbreviation | Datatype   |
// | ------------ | --------   |
// | c            | Counter    |
// | i            | Integer    |
// | n            | Null       |
// | s            | String     |
// | t            | Time ticks |
//
// Example:
//    |alert()
//       .message('{{ .ID }}:{{ .Level }}')
//       .snmpTrap('1.3.6.1.4.1.1')
//          .data('1.3.6.1.4.1.1.5', 's', '{{ .Level }}' )
//          .data('1.3.6.1.4.1.1.6', 'i', '50' )
//          .data('1.3.6.1.4.1.1.7', 'c', '{{ index .Fields "num_requests" }}' )
//          .data('1.3.6.1.4.1.1.8', 's', '{{ .Message }}' )
//
// tick:property
func (h *SNMPTrapHandler) Data(oid, typ, value string) *SNMPTrapHandler {
	data := SNMPData{
		Oid:   oid,
		Type:  typ,
		Value: value,
	}
	h.DataList = append(h.DataList, data)
	return h
}

func (h *SNMPTrapHandler) validate() error {
	if h.TrapOid == "" {
		return fmt.Errorf("you must supply a trap Oid")
	}
	for _, d := range h.DataList {
		switch d.Type {
		case "c", "i", "n", "s", "t":
			// OK
		default:
			return fmt.Errorf("unsupported data type %q for data entry %q", d.Type, d.Oid)
		}
	}
	return nil
}

// Send the alert to a Kafka topic.
//
// Example:
//    [[kafka]]
//      enabled = true
//      id = "default"
//      brokers = ["localhost:9092"]
//
// Example:
//    stream
//         |alert()
//             .kafka()
//                 .cluster('default')
//                 .kafkaTopic('alerts')
//
//
// tick:property
func (n *AlertNodeData) Kafka() *KafkaHandler {
	k := &KafkaHandler{
		AlertNodeData: n,
	}
	n.KafkaHandlers = append(n.KafkaHandlers, k)
	return k
}

// Kafka alert Handler
// tick:embedded:AlertNode.Kafka
type KafkaHandler struct {
	*AlertNodeData `json:"-"`

	// Cluster is the id of the configure kafka cluster
	Cluster string `json:"cluster"`

	// Kafka Topic
	KafkaTopic string `json:"kafka-topic"`

	// Template used to construct the message body
	// If empty the alert data in JSON is sent as the message body.
	Template string `json:"template"`
}
