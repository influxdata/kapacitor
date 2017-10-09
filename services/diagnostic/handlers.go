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
	klog "github.com/influxdata/kapacitor/services/diagnostic/internal/log"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/udf"
	plog "github.com/prometheus/common/log"
)

func Error(l *klog.Logger, msg string, err error, ctx []keyvalue.T) {
	if len(ctx) == 0 {
		l.Error(msg, klog.Error(err))
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		l.Error(msg, klog.Error(err), klog.String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		l.Error(msg, klog.Error(err), klog.String(x.Key, x.Value), klog.String(y.Key, y.Value))
		return
	}

	// This isn't great wrt to allocation, but should be rare. Currently
	// no calls to Error use more than 2 ctx values. If a new call to
	// Error uses more than 2, update this function
	fields := make([]klog.Field, len(ctx)+1) // +1 for error
	fields[0] = klog.Error(err)
	for i := 1; i < len(fields); i++ {
		kv := ctx[i-1]
		fields[i] = klog.String(kv.Key, kv.Value)
	}

	l.Error(msg, fields...)
}

// Alert Service Handler

type AlertServiceHandler struct {
	l *klog.Logger
}

func logFieldsFromContext(ctx []keyvalue.T) []klog.Field {
	fields := make([]klog.Field, len(ctx))
	for i, kv := range ctx {
		fields[i] = klog.String(kv.Key, kv.Value)
	}

	return fields
}

func (h *AlertServiceHandler) WithHandlerContext(ctx ...keyvalue.T) alertservice.HandlerDiagnostic {
	fields := logFieldsFromContext(ctx)

	return &AlertServiceHandler{
		l: h.l.With(fields...),
	}
}

func (h *AlertServiceHandler) MigratingHandlerSpecs() {
	h.l.Debug("migrating old v1.2 handler specs")
}

func (h *AlertServiceHandler) MigratingOldHandlerSpec(spec string) {
	h.l.Debug("migrating old handler spec", klog.String("handler", spec))
}

func (h *AlertServiceHandler) FoundHandlerRows(length int) {
	h.l.Debug("found handler rows", klog.Int("handler_row_count", length))
}

func (h *AlertServiceHandler) CreatingNewHandlers(length int) {
	h.l.Debug("creating new handlers in place of old handlers", klog.Int("handler_row_count", length))
}

func (h *AlertServiceHandler) FoundNewHandler(key string) {
	h.l.Debug("found new handler skipping", klog.String("handler", key))
}

func (h *AlertServiceHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

// Kapcitor Handler

type KapacitorHandler struct {
	l *klog.Logger
}

func (h *KapacitorHandler) WithTaskContext(task string) kapacitor.TaskDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(klog.String("task", task)),
	}
}

func (h *KapacitorHandler) WithTaskMasterContext(tm string) kapacitor.Diagnostic {
	return &KapacitorHandler{
		l: h.l.With(klog.String("task_master", tm)),
	}
}

func (h *KapacitorHandler) WithNodeContext(node string) kapacitor.NodeDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(klog.String("node", node)),
	}
}

func (h *KapacitorHandler) WithEdgeContext(task, parent, child string) kapacitor.EdgeDiagnostic {
	return &KapacitorHandler{
		l: h.l.With(klog.String("task", task), klog.String("parent", parent), klog.String("child", child)),
	}
}

func (h *KapacitorHandler) TaskMasterOpened() {
	h.l.Info("opened task master")
}

func (h *KapacitorHandler) TaskMasterClosed() {
	h.l.Info("closed task master")
}

func (h *KapacitorHandler) StartingTask(task string) {
	h.l.Debug("starting task", klog.String("task", task))
}

func (h *KapacitorHandler) StartedTask(task string) {
	h.l.Info("started task", klog.String("task", task))
}

func (h *KapacitorHandler) StoppedTask(task string) {
	h.l.Info("stopped task", klog.String("task", task))
}

func (h *KapacitorHandler) StoppedTaskWithError(task string, err error) {
	h.l.Error("failed to stop task with out error", klog.String("task", task), klog.Error(err))
}

func (h *KapacitorHandler) TaskMasterDot(d string) {
	h.l.Debug("listing dot", klog.String("dot", d))
}

func (h *KapacitorHandler) ClosingEdge(collected int64, emitted int64) {
	h.l.Debug("closing edge", klog.Int64("collected", collected), klog.Int64("emitted", emitted))
}

func (h *KapacitorHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *KapacitorHandler) AlertTriggered(level alert.Level, id string, message string, rows *models.Row) {
	h.l.Debug("alert triggered",
		klog.Stringer("level", level),
		klog.String("id", id),
		klog.String("event_message", message),
		klog.String("data", fmt.Sprintf("%v", rows)),
	)
}

func (h *KapacitorHandler) SettingReplicas(new int, old int, id string) {
	h.l.Debug("setting replicas",
		klog.Int("new", new),
		klog.Int("old", old),
		klog.String("event_id", id),
	)
}

func (h *KapacitorHandler) StartingBatchQuery(q string) {
	h.l.Debug("starting next batch query", klog.String("query", q))
}

func TagPairs(tags models.Tags) []klog.Field {
	ts := []klog.Field{}
	for k, v := range tags {
		ts = append(ts, klog.String(k, v))
	}

	return ts
}

func FieldPairs(tags models.Fields) []klog.Field {
	ts := []klog.Field{}
	for k, v := range tags {
		var el klog.Field
		switch t := v.(type) {
		case int64:
			el = klog.Int64(k, t)
		case string:
			el = klog.String(k, t)
		case float64:
			el = klog.Float64(k, t)
		case bool:
			el = klog.Bool(k, t)
		default:
			el = klog.String(k, fmt.Sprintf("%v", t))
		}
		ts = append(ts, el)
	}

	return ts
}

func (h *KapacitorHandler) LogPointData(level, prefix string, point edge.PointMessage) {
	fields := []klog.Field{
		klog.String("prefix", prefix),
		klog.String("name", point.Name()),
		klog.String("db", point.Database()),
		klog.String("rp", point.RetentionPolicy()),
		klog.String("group", string(point.GroupID())),
		klog.Strings("dimension", point.Dimensions().TagNames),
		klog.GroupedFields("tag", TagPairs(point.Tags())),
		klog.GroupedFields("field", FieldPairs(point.Fields())),
		klog.Time("time", point.Time()),
	}

	var log func(string, ...klog.Field)

	switch level {
	case "INFO":
		log = h.l.Info
	case "ERROR":
		log = h.l.Error
	case "DEBUG":
		log = h.l.Debug
	case "WARN":
		log = h.l.Warn
	default:
		log = h.l.Info
	}

	log("point", fields...)
}

func (h *KapacitorHandler) LogBatchData(level, prefix string, batch edge.BufferedBatchMessage) {
	var log func(string, ...klog.Field)

	switch level {
	case "INFO":
		log = h.l.Info
	case "ERROR":
		log = h.l.Error
	case "DEBUG":
		log = h.l.Debug
	case "WARN":
		log = h.l.Warn
	default:
		log = h.l.Info
	}

	begin := batch.Begin()
	log("begin batch",
		klog.String("prefix", prefix),
		klog.String("name", begin.Name()),
		klog.String("group", string(begin.GroupID())),
		klog.GroupedFields("tag", TagPairs(begin.Tags())),
		klog.Time("time", begin.Time()),
	)

	for _, p := range batch.Points() {
		log("batch point",
			klog.String("prefix", prefix),
			klog.String("name", begin.Name()),
			klog.String("group", string(begin.GroupID())),
			klog.GroupedFields("tag", TagPairs(p.Tags())),
			klog.GroupedFields("field", FieldPairs(p.Fields())),
			klog.Time("time", p.Time()),
		)
	}

	log("end batch",
		klog.String("prefix", prefix),
		klog.String("name", begin.Name()),
		klog.String("group", string(begin.GroupID())),
		klog.GroupedFields("tag", TagPairs(begin.Tags())),
		klog.Time("time", begin.Time()),
	)
}

func (h *KapacitorHandler) UDFLog(s string) {
	h.l.Info("UDF log", klog.String("text", s))
}

// Alerta handler

type AlertaHandler struct {
	l *klog.Logger
}

func (h *AlertaHandler) WithContext(ctx ...keyvalue.T) alerta.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &AlertaHandler{
		l: h.l.With(fields...),
	}
}

func (h *AlertaHandler) TemplateError(err error, kv keyvalue.T) {
	h.l.Error("failed to evaluate Alerta template", klog.Error(err), klog.String(kv.Key, kv.Value))
}

func (h *AlertaHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// HipChat handler
type HipChatHandler struct {
	l *klog.Logger
}

func (h *HipChatHandler) WithContext(ctx ...keyvalue.T) hipchat.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &HipChatHandler{
		l: h.l.With(fields...),
	}
}

func (h *HipChatHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// HTTPD handler

type HTTPDHandler struct {
	l *klog.Logger
}

func (h *HTTPDHandler) NewHTTPServerErrorLogger() *log.Logger {
	s := &StaticLevelHandler{
		l:     h.l.With(klog.String("service", "httpd_server_errors")),
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
	h.l.Info("authentication", klog.Bool("enabled", enabled))
}

func (h *HTTPDHandler) ListeningOn(addr string, proto string) {
	h.l.Info("listening on", klog.String("addr", addr), klog.String("protocol", proto))
}

func (h *HTTPDHandler) WriteBodyReceived(body string) {
	h.l.Debug("write body received by handler: %s", klog.String("body", body))
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
		klog.String("host", host),
		klog.String("username", username),
		klog.Time("start", start),
		klog.String("method", method),
		klog.String("uri", uri),
		klog.String("protocol", proto),
		klog.Int("status", status),
		klog.String("referer", referer),
		klog.String("user-agent", userAgent),
		klog.String("request-id", reqID),
		klog.Duration("duration", duration),
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
		klog.String("err", err),
		klog.String("host", host),
		klog.String("username", username),
		klog.Time("start", start),
		klog.String("method", method),
		klog.String("uri", uri),
		klog.String("protocol", proto),
		klog.Int("status", status),
		klog.String("referer", referer),
		klog.String("user-agent", userAgent),
		klog.String("request-id", reqID),
		klog.Duration("duration", duration),
	)
}

func (h *HTTPDHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// Reporting handler
type ReportingHandler struct {
	l *klog.Logger
}

func (h *ReportingHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// PagerDuty handler
type PagerDutyHandler struct {
	l *klog.Logger
}

func (h *PagerDutyHandler) WithContext(ctx ...keyvalue.T) pagerduty.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &PagerDutyHandler{
		l: h.l.With(fields...),
	}
}

func (h *PagerDutyHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// Slack Handler

type SlackHandler struct {
	l *klog.Logger
}

func (h *SlackHandler) InsecureSkipVerify() {
	h.l.Warn("service is configured to skip ssl verification")
}

func (h *SlackHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *SlackHandler) WithContext(ctx ...keyvalue.T) slack.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SlackHandler{
		l: h.l.With(fields...),
	}
}

// Storage Handler

type StorageHandler struct {
	l *klog.Logger
}

func (h *StorageHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// TaskStore Handler

type TaskStoreHandler struct {
	l *klog.Logger
}

func (h *TaskStoreHandler) StartingTask(taskID string) {
	h.l.Debug("starting enabled task on startup", klog.String("task", taskID))
}

func (h *TaskStoreHandler) StartedTask(taskID string) {
	h.l.Debug("started task during startup", klog.String("task", taskID))
}

func (h *TaskStoreHandler) FinishedTask(taskID string) {
	h.l.Debug("task finished", klog.String("task", taskID))
}

func (h *TaskStoreHandler) Debug(msg string) {
	h.l.Debug(msg)
}

func (h *TaskStoreHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *TaskStoreHandler) AlreadyMigrated(entity, id string) {
	h.l.Debug("entity has already been migrated skipping", klog.String(entity, id))
}

func (h *TaskStoreHandler) Migrated(entity, id string) {
	h.l.Debug("entity was migrated to new storage service", klog.String(entity, id))
}

// VictorOps Handler

type VictorOpsHandler struct {
	l *klog.Logger
}

func (h *VictorOpsHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *VictorOpsHandler) WithContext(ctx ...keyvalue.T) victorops.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &VictorOpsHandler{
		l: h.l.With(fields...),
	}
}

type SMTPHandler struct {
	l *klog.Logger
}

func (h *SMTPHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *SMTPHandler) WithContext(ctx ...keyvalue.T) smtp.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SMTPHandler{
		l: h.l.With(fields...),
	}
}

type OpsGenieHandler struct {
	l *klog.Logger
}

func (h *OpsGenieHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *OpsGenieHandler) WithContext(ctx ...keyvalue.T) opsgenie.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &OpsGenieHandler{
		l: h.l.With(fields...),
	}
}

// UDF service handler

type UDFServiceHandler struct {
	l *klog.Logger
}

func (h *UDFServiceHandler) LoadedUDFInfo(udf string) {
	h.l.Debug("loaded UDF info", klog.String("udf", udf))
}

func (h *UDFServiceHandler) WithUDFContext() udf.Diagnostic {
	return &UDFServiceHandler{
		l: h.l,
	}
}

func (h *UDFServiceHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *UDFServiceHandler) UDFLog(msg string) {
	h.l.Info("UDF log", klog.String("text", msg))
}

// Pushover handler

type PushoverHandler struct {
	l *klog.Logger
}

func (h *PushoverHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *PushoverHandler) WithContext(ctx ...keyvalue.T) pushover.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &PushoverHandler{
		l: h.l.With(fields...),
	}
}

// Template handler

type HTTPPostHandler struct {
	l *klog.Logger
}

func (h *HTTPPostHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	fields := make([]klog.Field, len(ctx)+1)
	fields[0] = klog.Error(err)
	for i, kv := range ctx {
		fields[i+1] = klog.String(kv.Key, kv.Value)
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
	l *klog.Logger
}

func (h *SensuHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *SensuHandler) WithContext(ctx ...keyvalue.T) sensu.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SensuHandler{
		l: h.l.With(fields...),
	}
}

// SNMPTrap handler

type SNMPTrapHandler struct {
	l *klog.Logger
}

func (h *SNMPTrapHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *SNMPTrapHandler) WithContext(ctx ...keyvalue.T) snmptrap.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &SNMPTrapHandler{
		l: h.l.With(fields...),
	}
}

// Telegram handler

type TelegramHandler struct {
	l *klog.Logger
}

func (h *TelegramHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *TelegramHandler) WithContext(ctx ...keyvalue.T) telegram.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &TelegramHandler{
		l: h.l.With(fields...),
	}
}

// MQTT handler

type MQTTHandler struct {
	l *klog.Logger
}

func (h *MQTTHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *MQTTHandler) CreatingAlertHandler(c mqtt.HandlerConfig) {
	qos, _ := c.QoS.MarshalText()
	h.l.Debug("creating mqtt handler",
		klog.String("broker_name", c.BrokerName),
		klog.String("topic", c.Topic),
		klog.Bool("retained", c.Retained),
		klog.String("qos", string(qos)),
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
	l *klog.Logger
}

func (h *TalkHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *TalkHandler) WithContext(ctx ...keyvalue.T) talk.Diagnostic {
	fields := logFieldsFromContext(ctx)

	return &TalkHandler{
		l: h.l.With(fields...),
	}
}

// Config handler

type ConfigOverrideHandler struct {
	l *klog.Logger
}

func (h *ConfigOverrideHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

type ServerHandler struct {
	l *klog.Logger
}

func (h *ServerHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *ServerHandler) Info(msg string, ctx ...keyvalue.T) {
	if len(ctx) == 0 {
		h.l.Info(msg)
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		h.l.Info(msg, klog.String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		h.l.Info(msg, klog.String(x.Key, x.Value), klog.String(y.Key, y.Value))
		return
	}

	fields := make([]klog.Field, len(ctx))
	for i := 0; i < len(fields); i++ {
		kv := ctx[i]
		fields[i] = klog.String(kv.Key, kv.Value)
	}

	h.l.Info(msg, fields...)
}

func (h *ServerHandler) Debug(msg string, ctx ...keyvalue.T) {
	if len(ctx) == 0 {
		h.l.Debug(msg)
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		h.l.Debug(msg, klog.String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		h.l.Debug(msg, klog.String(x.Key, x.Value), klog.String(y.Key, y.Value))
		return
	}

	fields := make([]klog.Field, len(ctx))
	for i := 0; i < len(fields); i++ {
		kv := ctx[i]
		fields[i] = klog.String(kv.Key, kv.Value)
	}

	h.l.Debug(msg, fields...)
}

type ReplayHandler struct {
	l *klog.Logger
}

func (h *ReplayHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *ReplayHandler) Debug(msg string, ctx ...keyvalue.T) {
	if len(ctx) == 0 {
		h.l.Debug(msg)
		return
	}

	if len(ctx) == 1 {
		el := ctx[0]
		h.l.Debug(msg, klog.String(el.Key, el.Value))
		return
	}

	if len(ctx) == 2 {
		x := ctx[0]
		y := ctx[1]
		h.l.Debug(msg, klog.String(x.Key, x.Value), klog.String(y.Key, y.Value))
		return
	}

	fields := make([]klog.Field, len(ctx))
	for i := 0; i < len(fields); i++ {
		kv := ctx[i]
		fields[i] = klog.String(kv.Key, kv.Value)
	}

	h.l.Debug(msg, fields...)
}

// K8s handler

type K8sHandler struct {
	l *klog.Logger
}

func (h *K8sHandler) WithClusterContext(cluster string) k8s.Diagnostic {
	return &K8sHandler{
		l: h.l.With(klog.String("cluster_id", cluster)),
	}
}

// Swarm handler

type SwarmHandler struct {
	l *klog.Logger
}

func (h *SwarmHandler) WithClusterContext(cluster string) swarm.Diagnostic {
	return &SwarmHandler{
		l: h.l.With(klog.String("cluster_id", cluster)),
	}
}

// Deadman handler

type DeadmanHandler struct {
	l *klog.Logger
}

func (h *DeadmanHandler) ConfiguredGlobally() {
	h.l.Info("Deadman's switch is configured globally")
}

// NoAuth handler

type NoAuthHandler struct {
	l *klog.Logger
}

func (h *NoAuthHandler) FakedUserAuthentication(username string) {
	h.l.Warn("using noauth auth backend. Faked Authentication for user", klog.String("user", username))
}

func (h *NoAuthHandler) FakedSubscriptionUserToken() {
	h.l.Warn("using noauth auth backend. Faked authentication for subscription user token")
}

// Stats handler

type StatsHandler struct {
	l *klog.Logger
}

func (h *StatsHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

// UDP handler

type UDPHandler struct {
	l *klog.Logger
}

func (h *UDPHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *UDPHandler) StartedListening(addr string) {
	h.l.Info("started listening on UDP", klog.String("address", addr))
}

func (h *UDPHandler) ClosedService() {
	h.l.Info("closed service")
}

// InfluxDB handler

type InfluxDBHandler struct {
	l *klog.Logger
}

func (h *InfluxDBHandler) Error(msg string, err error, ctx ...keyvalue.T) {
	Error(h.l, msg, err, ctx)
}

func (h *InfluxDBHandler) WithClusterContext(id string) influxdb.Diagnostic {
	return &InfluxDBHandler{
		l: h.l.With(klog.String("cluster", id)),
	}
}

func (h *InfluxDBHandler) WithUDPContext(db string, rp string) udp.Diagnostic {
	return &UDPHandler{
		l: h.l.With(klog.String("listener_id", fmt.Sprintf("udp:%s.%s", db, rp))),
	}
}

func (h *InfluxDBHandler) InsecureSkipVerify(urls []string) {
	h.l.Warn("using InsecureSkipVerify when connecting to InfluxDB; this is insecure", klog.Strings("urls", urls))
}

func (h *InfluxDBHandler) UnlinkingSubscriptions(cluster string) {
	h.l.Debug("unlinking subscription for cluster", klog.String("cluster", cluster))
}

func (h *InfluxDBHandler) LinkingSubscriptions(cluster string) {
	h.l.Debug("linking subscription for cluster", klog.String("cluster", cluster))
}

func (h *InfluxDBHandler) StartedUDPListener(db string, rp string) {
	h.l.Info("started UDP listener", klog.String("dbrp", fmt.Sprintf("%s.%s", db, rp)))
}

// Scraper handler

type ScraperHandler struct {
	mu  sync.Mutex
	buf *bytes.Buffer
	l   *klog.Logger
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

	h.l.Warn(h.buf.String())
}

func (h *ScraperHandler) Warnln(ctx ...interface{}) {
	h.mu.Lock()
	defer h.mu.Unlock()
	defer h.buf.Reset()
	fmt.Fprintln(h.buf, ctx...)

	h.l.Warn(strconv.Quote(h.buf.String()))
}

func (h *ScraperHandler) Warnf(s string, ctx ...interface{}) {
	h.l.Warn(fmt.Sprintf(s, ctx...))
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
	var field klog.Field

	switch value.(type) {
	case int:
		field = klog.Int(key, value.(int))
	case float64:
		field = klog.Float64(key, value.(float64))
	case string:
		field = klog.String(key, value.(string))
	case time.Duration:
		field = klog.Duration(key, value.(time.Duration))
	default:
		field = klog.String(key, fmt.Sprintf("%v", value))
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
	l *klog.Logger
}

func (h *EdgeHandler) Collect(mtype edge.MessageType) {
	h.l.Debug("collected message", klog.Stringer("message_type", mtype))
}
func (h *EdgeHandler) Emit(mtype edge.MessageType) {
	h.l.Debug("emitted message", klog.Stringer("message_type", mtype))
}

type logLevel int

const (
	llInvalid logLevel = iota
	llDebug
	llError
	llInfo
	llWarn
)

type StaticLevelHandler struct {
	l     *klog.Logger
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
	case llWarn:
		h.l.Warn(string(buf))
	default:
		return 0, errors.New("invalid log level")
	}

	return len(buf), nil
}

// Cmd handler

type CmdHandler struct {
	l *klog.Logger
}

func (h *CmdHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *CmdHandler) KapacitorStarting(version, branch, commit string) {
	h.l.Info("kapacitor starting", klog.String("version", version), klog.String("branch", branch), klog.String("commit", commit))
}

func (h *CmdHandler) GoVersion() {
	h.l.Info("go version", klog.String("version", runtime.Version()))
}

func (h *CmdHandler) Info(msg string) {
	h.l.Info(msg)
}

// Load handler

type LoadHandler struct {
	l *klog.Logger
}

func (h *LoadHandler) Error(msg string, err error) {
	h.l.Error(msg, klog.Error(err))
}

func (h *LoadHandler) Debug(msg string) {
	h.l.Debug(msg)
}

func (h *LoadHandler) Loading(el string, file string) {
	h.l.Debug("loading object from file", klog.String("object", el), klog.String("file", file))
}
