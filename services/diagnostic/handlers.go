package diagnostic

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	alertservice "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/ec2"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/sideload"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/uuid"
	plog "github.com/prometheus/common/log"
)

func Err(l Logger, msg string, err error, ctx []keyvalue.T) {
	if len(ctx) == 0 {
		l.Error(msg, Error(err))
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		l.Error(msg, Error(err), String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		l.Error(msg, Error(err), String(x.Key, x.Value), String(y.Key, y.Value))
		return
	}

	// Use the allocation version for any length
	fields := make([]Field, len(ctx)+1) // +1 for error
	fields[0] = Error(err)
	for i := 1; i < len(fields); i++ {
		kv := ctx[i-1]
		fields[i] = String(kv.Key, kv.Value)
	}

	l.Error(msg, fields...)
}

func Info(l Logger, msg string, ctx []keyvalue.T) {
	if len(ctx) == 0 {
		l.Info(msg)
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		l.Info(msg, String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		l.Info(msg, String(x.Key, x.Value), String(y.Key, y.Value))
		return
	}

	// Use the allocation version for any length
	fields := make([]Field, len(ctx))
	for i, kv := range ctx {
		fields[i] = String(kv.Key, kv.Value)
	}

	l.Info(msg, fields...)
}

func Debug(l Logger, msg string, ctx []keyvalue.T) {
	if len(ctx) == 0 {
		l.Debug(msg)
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		l.Debug(msg, String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		l.Debug(msg, String(x.Key, x.Value), String(y.Key, y.Value))
		return
	}

	// Use the allocation version for any length
	fields := make([]Field, len(ctx))
	for i, kv := range ctx {
		fields[i] = String(kv.Key, kv.Value)
	}

	l.Debug(msg, fields...)
}

// Alert Service Handler

type AlertServiceHandler struct {
	L Logger
}

func logFieldsFromContext(ctx []keyvalue.T) []Field {
	fields := make([]Field, len(ctx))
	for i, kv := range ctx {
		fields[i] = String(kv.Key, kv.Value)
	}

	return fields
}

func (h *AlertServiceHandler) WithHandlerContext(ctx ...keyvalue.T) alertservice.HandlerDiagnostic {
	fields := logFieldsFromContext(ctx)

	return &AlertServiceHandler{
		L: h.L.With(fields...),
	}
}

func (h *AlertServiceHandler) MigratingHandlerSpecs() {
	h.L.Debug("migrating old v1.2 handler specs")
}

func (h *AlertServiceHandler) MigratingOldHandlerSpec(spec string) {
	h.L.Debug("migrating old handler spec", String("handler", spec))
}

func (h *AlertServiceHandler) FoundHandlerRows(length int) {
	h.L.Debug("found handler rows", Int("handler_row_count", length))
}

func (h *AlertServiceHandler) CreatingNewHandlers(length int) {
	h.L.Debug("creating new handlers in place of old handlers", Int("handler_row_count", length))
}

func (h *AlertServiceHandler) FoundNewHandler(key string) {
	h.L.Debug("found new handler skipping", String("handler", key))
}

func (h *AlertServiceHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.L, msg, err, ctx)
}

// Kapcitor Handler

type KapacitorHandler struct {
	l Logger
}

func (h *KapacitorHandler) WithTaskContext(task string) kapacitor.TaskDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(String("task", task)),
	}
}

func (h *KapacitorHandler) WithTaskMasterContext(tm string) kapacitor.Diagnostic {
	return &KapacitorHandler{
		l: h.l.With(String("task_master", tm)),
	}
}

func (h *KapacitorHandler) WithNodeContext(node string) kapacitor.NodeDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(String("node", node)),
	}
}

func (h *KapacitorHandler) WithEdgeContext(task, parent, child string) kapacitor.EdgeDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(String("task", task), String("parent", parent), String("child", child)),
	}
}

func (h *KapacitorHandler) TaskMasterOpened() {
	h.l.Info("opened task master")
}

func (h *KapacitorHandler) TaskMasterClosed() {
	h.l.Info("closed task master")
}

func (h *KapacitorHandler) StartingTask(task string) {
	h.l.Debug("starting task", String("task", task))
}

func (h *KapacitorHandler) StartedTask(task string) {
	h.l.Info("started task", String("task", task))
}

func (h *KapacitorHandler) StoppedTask(task string) {
	h.l.Info("stopped task", String("task", task))
}

func (h *KapacitorHandler) StoppedTaskWithError(task string, err error) {
	h.l.Error("failed to stop task with out error", String("task", task), Error(err))
}

func (h *KapacitorHandler) TaskMasterDot(d string) {
	h.l.Debug("listing dot", String("dot", d))
}

func (h *KapacitorHandler) ClosingEdge(collected int64, emitted int64) {
	h.l.Debug("closing edge", Int64("collected", collected), Int64("emitted", emitted))
}

func (h *KapacitorHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *KapacitorHandler) AlertTriggered(level alert.Level, id string, message string, rows *models.Row) {
	h.l.Debug("alert triggered",
		Stringer("level", level),
		String("id", id),
		String("event_message", message),
		String("data", fmt.Sprintf("%v", rows)),
	)
}

func (h *KapacitorHandler) SettingReplicas(new int, old int, id string) {
	h.l.Debug("setting replicas",
		Int("new", new),
		Int("old", old),
		String("event_id", id),
	)
}

func (h *KapacitorHandler) StartingBatchQuery(q string) {
	h.l.Debug("starting next batch query", String("query", q))
}

func TagPairs(tags models.Tags) []Field {
	ts := []Field{}
	for k, v := range tags {
		ts = append(ts, String(k, v))
	}

	return ts
}

func FieldPairs(tags models.Fields) []Field {
	ts := []Field{}
	for k, v := range tags {
		var el Field
		switch t := v.(type) {
		case int64:
			el = Int64(k, t)
		case string:
			el = String(k, t)
		case float64:
			el = Float64(k, t)
		case bool:
			el = Bool(k, t)
		default:
			el = String(k, fmt.Sprintf("%v", t))
		}
		ts = append(ts, el)
	}

	return ts
}

func (h *KapacitorHandler) LogPointData(level, prefix string, point edge.PointMessage) {
	fields := []Field{
		String("prefix", prefix),
		String("name", point.Name()),
		String("db", point.Database()),
		String("rp", point.RetentionPolicy()),
		String("group", string(point.GroupID())),
		Strings("dimension", point.Dimensions().TagNames),
		GroupedFields("tag", TagPairs(point.Tags())),
		GroupedFields("field", FieldPairs(point.Fields())),
		Time("time", point.Time()),
	}

	var log func(string, ...Field)

	switch level {
	case "INFO":
		log = h.l.Info
	case "ERROR":
		log = h.l.Error
	case "DEBUG":
		log = h.l.Debug
	default:
		log = h.l.Info
	}

	log("point", fields...)
}

func (h *KapacitorHandler) LogBatchData(level, prefix string, batch edge.BufferedBatchMessage) {
	var log func(string, ...Field)

	switch level {
	case "INFO":
		log = h.l.Info
	case "ERROR":
		log = h.l.Error
	case "DEBUG":
		log = h.l.Debug
	default:
		log = h.l.Info
	}

	begin := batch.Begin()
	log("begin batch",
		String("prefix", prefix),
		String("name", begin.Name()),
		String("group", string(begin.GroupID())),
		GroupedFields("tag", TagPairs(begin.Tags())),
		Time("time", begin.Time()),
	)

	for _, p := range batch.Points() {
		log("batch point",
			String("prefix", prefix),
			String("name", begin.Name()),
			String("group", string(begin.GroupID())),
			GroupedFields("tag", TagPairs(p.Tags())),
			GroupedFields("field", FieldPairs(p.Fields())),
			Time("time", p.Time()),
		)
	}

	log("end batch",
		String("prefix", prefix),
		String("name", begin.Name()),
		String("group", string(begin.GroupID())),
		GroupedFields("tag", TagPairs(begin.Tags())),
		Time("time", begin.Time()),
	)
}

func (h *KapacitorHandler) UDFLog(s string) {
	h.l.Info("UDF log", String("text", s))
}

// Alerta handler

type AlertaHandler struct {
	l Logger
}

func (h *AlertaHandler) WithContext(ctx ...keyvalue.T) alerta.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &AlertaHandler{
		l: h.l.With(fields...),
	}
}

func (h *AlertaHandler) TemplateError(err error, kv keyvalue.T) {
	h.l.Error("failed to evaluate Alerta template", Error(err), String(kv.Key, kv.Value))
}

func (h *AlertaHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// HipChat handler
type HipChatHandler struct {
	l Logger
}

func (h *HipChatHandler) WithContext(ctx ...keyvalue.T) hipchat.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &HipChatHandler{
		l: h.l.With(fields...),
	}
}

func (h *HipChatHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// Kafka handler
type KafkaHandler struct {
	l Logger
}

func (h *KafkaHandler) WithContext(ctx ...keyvalue.T) kafka.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &KafkaHandler{
		l: h.l.With(fields...),
	}
}

func (h *KafkaHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}
func (h *KafkaHandler) InsecureSkipVerify() {
	h.l.Info("service is configured to skip ssl verification")
}

// HTTPD handler

type HTTPDHandler struct {
	l Logger
}

func (h *HTTPDHandler) NewHTTPServerErrorLogger() *log.Logger {
	s := &StaticLevelHandler{
		l:     h.l.With(String("service", "httpd_server_errors")),
		level: llError,
	}

	return log.New(s, "", log.LstdFlags)
}

func (h *HTTPDHandler) StartingService() {
	h.l.Info("starting HTTP service")
}

func (h *HTTPDHandler) StoppedService() {
	h.l.Info("closed HTTP service")
}

func (h *HTTPDHandler) ShutdownTimeout() {
	h.l.Error("shutdown timedout, forcefully closing all remaining connections")
}

func (h *HTTPDHandler) AuthenticationEnabled(enabled bool) {
	h.l.Info("authentication", Bool("enabled", enabled))
}

func (h *HTTPDHandler) ListeningOn(addr string, proto string) {
	h.l.Info("listening on", String("addr", addr), String("protocol", proto))
}

func (h *HTTPDHandler) WriteBodyReceived(body string) {
	h.l.Debug("write body received by handler: %s", String("body", body))
}

func (h *HTTPDHandler) HTTP(
	host string,
	username string,
	start time.Time,
	method string,
	uri string,
	proto string,
	status int,
	referer string,
	userAgent string,
	reqID string,
	duration time.Duration,
) {
	h.l.Info("http request",
		String("host", host),
		String("username", username),
		Time("start", start),
		String("method", method),
		String("uri", uri),
		String("protocol", proto),
		Int("status", status),
		String("referer", referer),
		String("user-agent", userAgent),
		String("request-id", reqID),
		Duration("duration", duration),
	)
}

func (h *HTTPDHandler) RecoveryError(
	msg string,
	err string,
	host string,
	username string,
	start time.Time,
	method string,
	uri string,
	proto string,
	status int,
	referer string,
	userAgent string,
	reqID string,
	duration time.Duration,
) {
	h.l.Error(
		msg,
		String("err", err),
		String("host", host),
		String("username", username),
		Time("start", start),
		String("method", method),
		String("uri", uri),
		String("protocol", proto),
		Int("status", status),
		String("referer", referer),
		String("user-agent", userAgent),
		String("request-id", reqID),
		Duration("duration", duration),
	)
}

func (h *HTTPDHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// Reporting handler
type ReportingHandler struct {
	l Logger
}

func (h *ReportingHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// PagerDuty handler
type PagerDutyHandler struct {
	l Logger
}

func (h *PagerDutyHandler) WithContext(ctx ...keyvalue.T) pagerduty.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &PagerDutyHandler{
		l: h.l.With(fields...),
	}
}

func (h *PagerDutyHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// PagerDuty2 handler
type PagerDuty2Handler struct {
	l Logger
}

func (h *PagerDuty2Handler) WithContext(ctx ...keyvalue.T) pagerduty2.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &PagerDuty2Handler{
		l: h.l.With(fields...),
	}
}

func (h *PagerDuty2Handler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// Slack Handler

type SlackHandler struct {
	l Logger
}

func (h *SlackHandler) InsecureSkipVerify() {
	h.l.Info("service is configured to skip ssl verification")
}

func (h *SlackHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *SlackHandler) WithContext(ctx ...keyvalue.T) slack.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SlackHandler{
		l: h.l.With(fields...),
	}
}

// Storage Handler

type StorageHandler struct {
	l Logger
}

func (h *StorageHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// TaskStore Handler

type TaskStoreHandler struct {
	l Logger
}

func (h *TaskStoreHandler) StartingTask(taskID string) {
	h.l.Debug("starting enabled task on startup", String("task", taskID))
}

func (h *TaskStoreHandler) StartedTask(taskID string) {
	h.l.Debug("started task during startup", String("task", taskID))
}

func (h *TaskStoreHandler) FinishedTask(taskID string) {
	h.l.Debug("task finished", String("task", taskID))
}

func (h *TaskStoreHandler) Debug(msg string) {
	h.l.Debug(msg)
}

func (h *TaskStoreHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *TaskStoreHandler) AlreadyMigrated(entity, id string) {
	h.l.Debug("entity has already been migrated skipping", String(entity, id))
}

func (h *TaskStoreHandler) Migrated(entity, id string) {
	h.l.Debug("entity was migrated to new storage service", String(entity, id))
}

// VictorOps Handler

type VictorOpsHandler struct {
	l Logger
}

func (h *VictorOpsHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *VictorOpsHandler) WithContext(ctx ...keyvalue.T) victorops.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &VictorOpsHandler{
		l: h.l.With(fields...),
	}
}

type SMTPHandler struct {
	l Logger
}

func (h *SMTPHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *SMTPHandler) WithContext(ctx ...keyvalue.T) smtp.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SMTPHandler{
		l: h.l.With(fields...),
	}
}

type OpsGenieHandler struct {
	l Logger
}

func (h *OpsGenieHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *OpsGenieHandler) WithContext(ctx ...keyvalue.T) opsgenie.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &OpsGenieHandler{
		l: h.l.With(fields...),
	}
}

type OpsGenie2Handler struct {
	l Logger
}

func (h *OpsGenie2Handler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *OpsGenie2Handler) WithContext(ctx ...keyvalue.T) opsgenie2.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &OpsGenie2Handler{
		l: h.l.With(fields...),
	}
}

// UDF service handler

type UDFServiceHandler struct {
	l Logger
}

func (h *UDFServiceHandler) LoadedUDFInfo(udf string) {
	h.l.Debug("loaded UDF info", String("udf", udf))
}

func (h *UDFServiceHandler) WithUDFContext() udf.Diagnostic {
	return &UDFServiceHandler{
		l: h.l,
	}
}

func (h *UDFServiceHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *UDFServiceHandler) UDFLog(msg string) {
	h.l.Info("UDF log", String("text", msg))
}

// Pushover handler

type PushoverHandler struct {
	l Logger
}

func (h *PushoverHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *PushoverHandler) WithContext(ctx ...keyvalue.T) pushover.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &PushoverHandler{
		l: h.l.With(fields...),
	}
}

// Template handler

type HTTPPostHandler struct {
	l Logger
}

func (h *HTTPPostHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	fields := make([]Field, len(ctx)+1)
	fields[0] = Error(err)
	for i, kv := range ctx {
		fields[i+1] = String(kv.Key, kv.Value)
	}
	h.l.Error(msg, fields...)
}

func (h *HTTPPostHandler) WithContext(ctx ...keyvalue.T) httppost.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &HTTPPostHandler{
		l: h.l.With(fields...),
	}
}

// Sensu handler

type SensuHandler struct {
	l Logger
}

func (h *SensuHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *SensuHandler) WithContext(ctx ...keyvalue.T) sensu.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SensuHandler{
		l: h.l.With(fields...),
	}
}

// SNMPTrap handler

type SNMPTrapHandler struct {
	l Logger
}

func (h *SNMPTrapHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *SNMPTrapHandler) WithContext(ctx ...keyvalue.T) snmptrap.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SNMPTrapHandler{
		l: h.l.With(fields...),
	}
}

// Telegram handler

type TelegramHandler struct {
	l Logger
}

func (h *TelegramHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *TelegramHandler) WithContext(ctx ...keyvalue.T) telegram.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &TelegramHandler{
		l: h.l.With(fields...),
	}
}

// MQTT handler

type MQTTHandler struct {
	l Logger
}

func (h *MQTTHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *MQTTHandler) CreatingAlertHandler(c mqtt.HandlerConfig) {
	qos, _ := c.QoS.MarshalText()
	h.l.Debug("creating mqtt handler",
		String("broker_name", c.BrokerName),
		String("topic", c.Topic),
		Bool("retained", c.Retained),
		String("qos", string(qos)),
	)
}

func (h *MQTTHandler) HandlingEvent() {
	h.l.Debug("handling event")
}

func (h *MQTTHandler) WithContext(ctx ...keyvalue.T) mqtt.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &MQTTHandler{
		l: h.l.With(fields...),
	}
}

// Talk handler

type TalkHandler struct {
	l Logger
}

func (h *TalkHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *TalkHandler) WithContext(ctx ...keyvalue.T) talk.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &TalkHandler{
		l: h.l.With(fields...),
	}
}

// Config handler

type ConfigOverrideHandler struct {
	l Logger
}

func (h *ConfigOverrideHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

type ServerHandler struct {
	l Logger
}

func (h *ServerHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *ServerHandler) Info(msg string, ctx ...keyvalue.T) {
	Info(h.l, msg, ctx)
}

func (h *ServerHandler) Debug(msg string, ctx ...keyvalue.T) {
	Debug(h.l, msg, ctx)
}

type ReplayHandler struct {
	l Logger
}

func (h *ReplayHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *ReplayHandler) Debug(msg string, ctx ...keyvalue.T) {
	Debug(h.l, msg, ctx)
}

// K8s handler

type K8sHandler struct {
	l Logger
}

func (h *K8sHandler) WithClusterContext(cluster string) k8s.Diagnostic {
	return &K8sHandler{
		l: h.l.With(String("cluster_id", cluster)),
	}
}

// Swarm handler

type SwarmHandler struct {
	l Logger
}

func (h *SwarmHandler) WithClusterContext(cluster string) swarm.Diagnostic {
	return &SwarmHandler{
		l: h.l.With(String("cluster_id", cluster)),
	}
}

// EC2 handler

type EC2Handler struct {
	*ScraperHandler
}

func (h *EC2Handler) WithClusterContext(cluster string) ec2.Diagnostic {
	return &EC2Handler{
		ScraperHandler: &ScraperHandler{
			l: h.ScraperHandler.l.With(String("cluster_id", cluster)),
		},
	}
}

// Deadman handler

type DeadmanHandler struct {
	l Logger
}

func (h *DeadmanHandler) ConfiguredGlobally() {
	h.l.Info("Deadman's switch is configured globally")
}

// NoAuth handler

type NoAuthHandler struct {
	l Logger
}

func (h *NoAuthHandler) FakedUserAuthentication(username string) {
	h.l.Info("using noauth auth backend. Faked Authentication for user", String("user", username))
}

func (h *NoAuthHandler) FakedSubscriptionUserToken() {
	h.l.Info("using noauth auth backend. Faked authentication for subscription user token")
}

// Stats handler

type StatsHandler struct {
	l Logger
}

func (h *StatsHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

// UDP handler

type UDPHandler struct {
	l Logger
}

func (h *UDPHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *UDPHandler) StartedListening(addr string) {
	h.l.Info("started listening on UDP", String("address", addr))
}

func (h *UDPHandler) ClosedService() {
	h.l.Info("closed service")
}

// InfluxDB handler

type InfluxDBHandler struct {
	l Logger
}

func (h *InfluxDBHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Err(h.l, msg, err, ctx)
}

func (h *InfluxDBHandler) WithClusterContext(id string) influxdb.Diagnostic {
	return &InfluxDBHandler{
		l: h.l.With(String("cluster", id)),
	}
}

func (h *InfluxDBHandler) WithUDPContext(db string, rp string) udp.Diagnostic {
	return &UDPHandler{
		l: h.l.With(String("listener_id", fmt.Sprintf("udp:%s.%s", db, rp))),
	}
}

func (h *InfluxDBHandler) InsecureSkipVerify(urls []string) {
	h.l.Info("using InsecureSkipVerify when connecting to InfluxDB; this is insecure", Strings("urls", urls))
}

func (h *InfluxDBHandler) UnlinkingSubscriptions(cluster string) {
	h.l.Debug("unlinking subscription for cluster", String("cluster", cluster))
}

func (h *InfluxDBHandler) LinkingSubscriptions(cluster string) {
	h.l.Debug("linking subscription for cluster", String("cluster", cluster))
}

func (h *InfluxDBHandler) StartedUDPListener(db string, rp string) {
	h.l.Info("started UDP listener", String("dbrp", fmt.Sprintf("%s.%s", db, rp)))
}

// Scraper handler

type ScraperHandler struct {
	mu  sync.Mutex
	buf *bytes.Buffer
	l   Logger
}

func (h *ScraperHandler) Debug(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprint(h.buf, ctx...)

	h.l.Debug(h.buf.String())
}

func (h *ScraperHandler) Debugln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Debug(strconv.Quote(h.buf.String()))
}

func (h *ScraperHandler) Debugf(s string, ctx ...interface{}) {
	h.l.Debug(fmt.Sprintf(s, ctx...))
}

func (h *ScraperHandler) Info(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprint(h.buf, ctx...)

	h.l.Info(h.buf.String())
}

func (h *ScraperHandler) Infoln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Info(strconv.Quote(h.buf.String()))
}

func (h *ScraperHandler) Infof(s string, ctx ...interface{}) {
	h.l.Debug(fmt.Sprintf(s, ctx...))
}

func (h *ScraperHandler) Warn(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprint(h.buf, ctx...)

	h.l.Info(h.buf.String())
}

func (h *ScraperHandler) Warnln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Info(strconv.Quote(h.buf.String()))
}

func (h *ScraperHandler) Warnf(s string, ctx ...interface{}) {
	h.l.Info(fmt.Sprintf(s, ctx...))
}

func (h *ScraperHandler) Error(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprint(h.buf, ctx...)

	h.l.Error(h.buf.String())
}

func (h *ScraperHandler) Errorln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Error(strconv.Quote(h.buf.String()))
}

func (h *ScraperHandler) Errorf(s string, ctx ...interface{}) {
	h.l.Error(fmt.Sprintf(s, ctx...))
}

func (h *ScraperHandler) Fatal(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprint(h.buf, ctx...)

	h.l.Error(h.buf.String())
}

func (h *ScraperHandler) Fatalln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Error(h.buf.String())
}

func (h *ScraperHandler) Fatalf(s string, ctx ...interface{}) {
	h.l.Error(fmt.Sprintf(s, ctx...))
}

func (h *ScraperHandler) With(key string, value interface{}) plog.Logger {
	var field Field

	switch value.(type) {
	case int:
		field = Int(key, value.(int))
	case float64:
		field = Float64(key, value.(float64))
	case string:
		field = String(key, value.(string))
	case time.Duration:
		field = Duration(key, value.(time.Duration))
	default:
		field = String(key, fmt.Sprintf("%v", value))
	}

	return &ScraperHandler{
		l: h.l.With(field),
	}
}

func (h *ScraperHandler) SetFormat(string) error {
	return nil
}

func (h *ScraperHandler) SetLevel(string) error {
	return nil
}

// Edge Handler

type EdgeHandler struct {
	l Logger
}

func (h *EdgeHandler) Collect(mtype edge.MessageType) {
	h.l.Debug("collected message", Stringer("message_type", mtype))
}
func (h *EdgeHandler) Emit(mtype edge.MessageType) {
	h.l.Debug("emitted message", Stringer("message_type", mtype))
}

type logLevel int

const (
	llInvalid logLevel = iota
	llDebug
	llError
	llInfo
)

type StaticLevelHandler struct {
	l     Logger
	level logLevel
}

func (h *StaticLevelHandler) Write(buf []byte) (int, error) {
	switch h.level {
	case llDebug:
		h.l.Debug(string(buf))
	case llError:
		h.l.Error(string(buf))
	case llInfo:
		h.l.Info(string(buf))
	default:
		return 0, errors.New("invalid log level")
	}

	return len(buf), nil
}

// Cmd handler

type CmdHandler struct {
	l Logger
}

func (h *CmdHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *CmdHandler) KapacitorStarting(version, branch, commit string) {
	h.l.Info("kapacitor starting", String("version", version), String("branch", branch), String("commit", commit))
}

func (h *CmdHandler) GoVersion() {
	h.l.Info("go version", String("version", runtime.Version()))
}

func (h *CmdHandler) Info(msg string) {
	h.l.Info(msg)
}

// Load handler

type LoadHandler struct {
	l Logger
}

func (h *LoadHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *LoadHandler) Debug(msg string) {
	h.l.Debug(msg)
}

func (h *LoadHandler) Loading(el string, file string) {
	h.l.Debug("loading object from file", String("object", el), String("file", file))
}

// Session handler

type SessionHandler struct {
	l Logger
}

func (h *SessionHandler) CreatedLogSession(id uuid.UUID, contentType string, tags []tag) {
	ts := make([]string, len(tags))
	for i, t := range tags {
		ts[i] = t.key + "=" + t.value
	}

	h.l.Info("created log session", Stringer("id", id), String("content-type", contentType), Strings("tags", ts))
}

func (h *SessionHandler) DeletedLogSession(id uuid.UUID, contentType string, tags []tag) {
	ts := make([]string, len(tags))
	for i, t := range tags {
		ts[i] = t.key + "=" + t.value
	}

	h.l.Info("deleted log session", Stringer("id", id), String("content-type", contentType), Strings("tags", ts))
}

type SideloadHandler struct {
	l Logger
}

func (h *SideloadHandler) Error(msg string, err error) {
	h.l.Error(msg, Error(err))
}

func (h *SideloadHandler) WithContext(ctx ...keyvalue.T) sideload.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SideloadHandler{
		l: h.l.With(fields...),
	}
}
