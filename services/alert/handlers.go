package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	text "text/template"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

type HandlerDiagnostic interface {
	Error(msg string, err error, ctx ...keyvalue.T)
}

// Default log mode for file
const defaultLogFileMode = 0600

type LogHandlerConfig struct {
	Path string      `mapstructure:"path"`
	Mode os.FileMode `mapstructure:"mode"`
}

func (c LogHandlerConfig) Validate() error {
	if c.Mode.Perm()&0200 == 0 {
		return fmt.Errorf("invalid file mode %v, must be user writable", c.Mode)
	}
	if !filepath.IsAbs(c.Path) {
		return fmt.Errorf("log path must be absolute: %s is not absolute", c.Path)
	}
	return nil
}

type logHandler struct {
	logpath string
	mode    os.FileMode
	diag    HandlerDiagnostic
}

func DefaultLogHandlerConfig() LogHandlerConfig {
	return LogHandlerConfig{
		Mode: defaultLogFileMode,
	}
}

func NewLogHandler(c LogHandlerConfig, d HandlerDiagnostic) (alert.Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &logHandler{
		logpath: c.Path,
		mode:    c.Mode,
		diag:    d,
	}, nil
}

func (h *logHandler) Handle(event alert.Event) {
	ad := event.AlertData()

	f, err := os.OpenFile(h.logpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, h.mode)
	if err != nil {
		h.diag.Error("failed to open file for alert logging", err, keyvalue.KV("file", h.logpath))
		return
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(ad)
	if err != nil {
		h.diag.Error("failed to marshal alert data json", err)
	}
}

type mlogHandler struct {
	logpath string
	mode    os.FileMode
	diag    HandlerDiagnostic
}

func NewMlogHandler(c LogHandlerConfig, d HandlerDiagnostic) (alert.Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &mlogHandler{
		logpath: c.Path,
		mode:    c.Mode,
		diag:    d,
	}, nil
}

func (h *mlogHandler) Handle(event alert.Event) {
	ad := event.AlertData()
	file := fmt.Sprintf("%s/%d_%s.log", h.logpath, ad.Time.Unix(), ad.ID)
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, h.mode)
	if err != nil {
		h.diag.Error("failed to open file for alert logging", err, keyvalue.KV("file", file))
		return
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(ad)
	if err != nil {
		h.diag.Error("failed to marshal alert data json", err)
	}
}

type ExecHandlerConfig struct {
	Prog      string            `mapstructure:"prog"`
	Args      []string          `mapstructure:"args"`
	Commander command.Commander `mapstructure:"-"`
}

type execHandler struct {
	bp        *bufpool.Pool
	s         command.Spec
	commander command.Commander
	diag      HandlerDiagnostic
}

func NewExecHandler(c ExecHandlerConfig, d HandlerDiagnostic) alert.Handler {
	s := command.Spec{
		Prog: c.Prog,
		Args: c.Args,
	}
	return &execHandler{
		bp:        bufpool.New(),
		s:         s,
		commander: c.Commander,
		diag:      d,
	}
}

func (h *execHandler) Handle(event alert.Event) {
	buf := h.bp.Get()
	defer h.bp.Put(buf)
	ad := event.AlertData()

	err := json.NewEncoder(buf).Encode(ad)
	if err != nil {
		h.diag.Error("failed to marshal alert data json", err)
		return
	}

	cmd := h.commander.NewCommand(h.s)
	cmd.Stdin(buf)
	var out bytes.Buffer
	cmd.Stdout(&out)
	cmd.Stderr(&out)
	err = cmd.Start()
	if err != nil {
		h.diag.Error("exec command failed", err, keyvalue.KV("output", out.String()))
		return
	}
	err = cmd.Wait()
	if err != nil {
		h.diag.Error("exec command failed", err, keyvalue.KV("output", out.String()))
		return
	}
}

type TCPHandlerConfig struct {
	Address string `mapstructure:"address"`
}

type tcpHandler struct {
	bp   *bufpool.Pool
	addr string
	diag HandlerDiagnostic
}

func NewTCPHandler(c TCPHandlerConfig, d HandlerDiagnostic) alert.Handler {
	return &tcpHandler{
		bp:   bufpool.New(),
		addr: c.Address,
		diag: d,
	}
}

func (h *tcpHandler) Handle(event alert.Event) {
	buf := h.bp.Get()
	defer h.bp.Put(buf)
	ad := event.AlertData()

	err := json.NewEncoder(buf).Encode(ad)
	if err != nil {
		h.diag.Error("failed to marshal alert data json", err)
		return
	}

	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		h.diag.Error("tcp handler failed to connect", err, keyvalue.KV("address", h.addr))
		return
	}
	defer conn.Close()

	buf.WriteByte('\n')
	conn.Write(buf.Bytes())
}

type AggregateHandlerConfig struct {
	ID       string        `mapstructure:"id"`
	Interval time.Duration `mapstructure:"interval"`
	Topic    string        `mapstructure:"topic"`
	Message  string        `mapstructure:"message"`
	ec       EventCollector
}

type aggregateMessageData struct {
	Count    int
	Interval time.Duration
}

func newDefaultAggregateHandlerConfig(ec EventCollector) AggregateHandlerConfig {
	return AggregateHandlerConfig{
		Message: "Received {{ .Count }} events in the last {{.Interval}}.",
		ec:      ec,
	}
}

type aggregateHandler struct {
	interval time.Duration
	id       string
	topic    string
	ec       EventCollector

	messageTmpl *text.Template

	diag    HandlerDiagnostic
	events  chan alert.Event
	closing chan struct{}

	wg sync.WaitGroup
}

func NewAggregateHandler(c AggregateHandlerConfig, d HandlerDiagnostic) (alert.Handler, error) {
	// Parse and validate message template
	tmpl, err := text.New("message").Parse(c.Message)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	md := aggregateMessageData{}
	err = tmpl.Execute(&buf, md)
	if err != nil {
		return nil, errors.Wrap(err, "failed to evaluate message template with aggregate message data")
	}

	h := &aggregateHandler{
		interval:    time.Duration(c.Interval),
		id:          c.ID,
		topic:       c.Topic,
		ec:          c.ec,
		messageTmpl: tmpl,
		diag:        d,
		events:      make(chan alert.Event),
		closing:     make(chan struct{}),
	}
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.run()
	}()
	return h, nil
}

func (h *aggregateHandler) run() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	var events []alert.Event
	var messageBuf bytes.Buffer
	// Keep track if this batch of events should be external.
	external := false
	for {
		select {
		case <-h.closing:
			return
		case e := <-h.events:
			events = append(events, e)
			external = external || !e.NoExternal
		case <-ticker.C:
			if len(events) == 0 {
				continue
			}
			messageBuf.Reset()
			md := aggregateMessageData{
				Interval: h.interval,
				Count:    len(events),
			}
			// Ignore error since we have validated the template already
			_ = h.messageTmpl.Execute(&messageBuf, md)
			details := make([]string, len(events))
			agg := alert.Event{
				Topic: h.topic,
				State: alert.EventState{
					ID:      h.id,
					Message: messageBuf.String(),
				},
				NoExternal: !external,
			}
			for i, e := range events {
				if e.State.Level > agg.State.Level {
					agg.State.Level = e.State.Level
				}
				if e.State.Time.After(agg.State.Time) {
					agg.State.Time = e.State.Time
				}
				if e.State.Duration > agg.State.Duration {
					agg.State.Duration = e.State.Duration
				}
				details[i] = e.State.Message
				agg.Data.Result.Series = append(agg.Data.Result.Series, e.Data.Result.Series...)
			}
			agg.State.Details = strings.Join(details, "\n")
			h.ec.Collect(agg)
			events = events[0:0]
			external = false
		}
	}
}

func (h *aggregateHandler) Handle(event alert.Event) {
	select {
	case h.events <- event:
	case <-h.closing:
	}
}

func (h *aggregateHandler) Close() {
	close(h.closing)
	h.wg.Wait()
}

type PublishHandlerConfig struct {
	Topics []string `mapstructure:"topics"`
	ec     EventCollector
}
type publishHandler struct {
	c    PublishHandlerConfig
	diag HandlerDiagnostic
}

func NewPublishHandler(c PublishHandlerConfig, d HandlerDiagnostic) alert.Handler {
	return &publishHandler{
		c:    c,
		diag: d,
	}
}

func (h *publishHandler) Handle(event alert.Event) {
	for _, t := range h.c.Topics {
		event.Topic = t
		h.c.ec.Collect(event)
	}
}

// ExternalHandler wraps an existing handler that calls out to external services.
// The events are checked for the NoExternal flag before being passed to the external handler.
type externalHandler struct {
	h alert.Handler
}

func newExternalHandler(h alert.Handler) *externalHandler {
	return &externalHandler{
		h: h,
	}
}

func (h *externalHandler) Handle(event alert.Event) {
	if !event.NoExternal {
		h.h.Handle(event)
	}
}

type matchHandler struct {
	h alert.Handler

	scope *stateful.Scope
	expr  stateful.Expression

	// scope optimization
	usesChanged,
	usesLevel,
	usesName,
	usesTaskName,
	usesDuration bool

	vars []string

	diag HandlerDiagnostic
}

const (
	changedFunc  = "changed"
	levelFunc    = "level"
	nameFunc     = "name"
	taskNameFunc = "taskName"
	durationFunc = "duration"
)

var matchIdentifiers = map[string]interface{}{
	"OK":       int64(alert.OK),
	"INFO":     int64(alert.Info),
	"WARNING":  int64(alert.Warning),
	"CRITICAL": int64(alert.Critical),
}

func newMatchHandler(match string, h alert.Handler, d HandlerDiagnostic) (*matchHandler, error) {
	lambda, err := ast.ParseLambda(match)
	if err != nil {
		return nil, errors.Wrap(err, "invalid match expression")
	}

	// Replace identifiers with static values
	_, err = ast.Walk(lambda, func(n ast.Node) (ast.Node, error) {
		if ident, ok := n.(*ast.IdentifierNode); ok {
			v, ok := matchIdentifiers[ident.Ident]
			if !ok {
				return nil, fmt.Errorf("unknown identifier %q", ident.Ident)
			}
			return ast.ValueToLiteralNode(n, v)
		}
		return n, nil
	})
	if err != nil {
		return nil, err
	}

	expr, err := stateful.NewExpression(lambda.Expression)
	if err != nil {
		return nil, errors.Wrap(err, "invalid match expression")
	}

	mh := &matchHandler{
		h:     h,
		expr:  expr,
		scope: stateful.NewScope(),
		vars:  ast.FindReferenceVariables(lambda),
		diag:  d,
	}

	// Determine which functions are called
	funcs := ast.FindFunctionCalls(lambda)
	for _, f := range funcs {
		switch f {
		case changedFunc:
			mh.usesChanged = true
		case levelFunc:
			mh.usesLevel = true
		case nameFunc:
			mh.usesName = true
		case taskNameFunc:
			mh.usesTaskName = true
		case durationFunc:
			mh.usesDuration = true
		default:
			// ignore the function
		}
	}

	return mh, nil
}

func (h *matchHandler) Handle(event alert.Event) {
	if ok, err := h.match(event); err != nil {
		h.diag.Error("failed to evaluate match expression", err)
	} else if ok {
		h.h.Handle(event)
	}
}

var changedFuncSignature = map[stateful.Domain]ast.ValueType{}
var levelFuncSignature = map[stateful.Domain]ast.ValueType{}
var nameFuncSignature = map[stateful.Domain]ast.ValueType{}
var taskNameFuncSignature = map[stateful.Domain]ast.ValueType{}
var durationFuncSignature = map[stateful.Domain]ast.ValueType{}

func init() {
	d := stateful.Domain{}
	changedFuncSignature[d] = ast.TBool
	levelFuncSignature[d] = ast.TInt
	nameFuncSignature[d] = ast.TString
	taskNameFuncSignature[d] = ast.TString
	durationFuncSignature[d] = ast.TDuration
}

func (h *matchHandler) match(event alert.Event) (bool, error) {
	// Populate scope
	h.scope.Reset()

	if h.usesChanged {
		h.scope.SetDynamicFunc(changedFunc, &stateful.DynamicFunc{
			F: func(args ...interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s takes no arguments", changedFunc)
				}
				return event.State.Level != event.PreviousState().Level, nil
			},
			Sig: changedFuncSignature,
		})
	}

	if h.usesLevel {
		h.scope.SetDynamicFunc(levelFunc, &stateful.DynamicFunc{
			F: func(args ...interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s takes no arguments", levelFunc)
				}
				return int64(event.State.Level), nil
			},
			Sig: levelFuncSignature,
		})
	}

	if h.usesName {
		h.scope.SetDynamicFunc(nameFunc, &stateful.DynamicFunc{
			F: func(args ...interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s takes no arguments", nameFunc)
				}
				return event.Data.Name, nil
			},
			Sig: nameFuncSignature,
		})
	}

	if h.usesTaskName {
		h.scope.SetDynamicFunc(taskNameFunc, &stateful.DynamicFunc{
			F: func(args ...interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s takes no arguments", taskNameFunc)
				}
				return event.Data.TaskName, nil
			},
			Sig: taskNameFuncSignature,
		})
	}

	if h.usesDuration {
		h.scope.SetDynamicFunc(durationFunc, &stateful.DynamicFunc{
			F: func(args ...interface{}) (interface{}, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("%s takes no arguments", durationFunc)
				}
				return event.State.Duration, nil
			},
			Sig: durationFuncSignature,
		})
	}

	// Set tag values on scope
	for _, v := range h.vars {
		if tag, ok := event.Data.Tags[v]; ok {
			h.scope.Set(v, tag)
		} else {
			return false, fmt.Errorf("no tag exists for %s", v)
		}
	}

	// Evaluate expression with scope
	return h.expr.EvalBool(h.scope)
}
