package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	text "text/template"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/bufpool"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/models"
	"github.com/pkg/errors"
)

// AlertData is a structure that contains relevant data about an alert event.
// The structure is intended to be JSON encoded, providing a consistent data format.
type AlertData struct {
	ID       string        `json:"id"`
	Message  string        `json:"message"`
	Details  string        `json:"details"`
	Time     time.Time     `json:"time"`
	Duration time.Duration `json:"duration"`
	Level    alert.Level   `json:"level"`
	Data     models.Result `json:"data"`
}

func alertDataFromEvent(event alert.Event) AlertData {
	return AlertData{
		ID:       event.State.ID,
		Message:  event.State.Message,
		Details:  event.State.Details,
		Time:     event.State.Time,
		Duration: event.State.Duration,
		Level:    event.State.Level,
		Data:     event.Data.Result,
	}
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
	logger  *log.Logger
}

func DefaultLogHandlerConfig() LogHandlerConfig {
	return LogHandlerConfig{
		Mode: defaultLogFileMode,
	}
}

func NewLogHandler(c LogHandlerConfig, l *log.Logger) (alert.Handler, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &logHandler{
		logpath: c.Path,
		mode:    c.Mode,
		logger:  l,
	}, nil
}

func (h *logHandler) Handle(event alert.Event) {
	ad := alertDataFromEvent(event)

	f, err := os.OpenFile(h.logpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, h.mode)
	if err != nil {
		h.logger.Printf("E! failed to open file %s for alert logging: %v", h.logpath, err)
		return
	}
	defer f.Close()

	err = json.NewEncoder(f).Encode(ad)
	if err != nil {
		h.logger.Printf("E! failed to marshal alert data json: %v", err)
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
	logger    *log.Logger
}

func NewExecHandler(c ExecHandlerConfig, l *log.Logger) alert.Handler {
	s := command.Spec{
		Prog: c.Prog,
		Args: c.Args,
	}
	return &execHandler{
		bp:        bufpool.New(),
		s:         s,
		commander: c.Commander,
		logger:    l,
	}
}

func (h *execHandler) Handle(event alert.Event) {
	buf := h.bp.Get()
	defer h.bp.Put(buf)
	ad := alertDataFromEvent(event)

	err := json.NewEncoder(buf).Encode(ad)
	if err != nil {
		h.logger.Printf("E! failed to marshal alert data json: %v", err)
		return
	}

	cmd := h.commander.NewCommand(h.s)
	cmd.Stdin(buf)
	var out bytes.Buffer
	cmd.Stdout(&out)
	cmd.Stderr(&out)
	err = cmd.Start()
	if err != nil {
		h.logger.Printf("E! exec command failed: Output: %s: %v", out.String(), err)
		return
	}
	err = cmd.Wait()
	if err != nil {
		h.logger.Printf("E! exec command failed: Output: %s: %v", out.String(), err)
		return
	}
}

type TCPHandlerConfig struct {
	Address string `mapstructure:"address"`
}

type tcpHandler struct {
	bp     *bufpool.Pool
	addr   string
	logger *log.Logger
}

func NewTCPHandler(c TCPHandlerConfig, l *log.Logger) alert.Handler {
	return &tcpHandler{
		bp:     bufpool.New(),
		addr:   c.Address,
		logger: l,
	}
}

func (h *tcpHandler) Handle(event alert.Event) {
	buf := h.bp.Get()
	defer h.bp.Put(buf)
	ad := alertDataFromEvent(event)

	err := json.NewEncoder(buf).Encode(ad)
	if err != nil {
		h.logger.Printf("E! failed to marshal alert data json: %v", err)
		return
	}

	conn, err := net.Dial("tcp", h.addr)
	if err != nil {
		h.logger.Printf("E! failed to connect to %s: %v", h.addr, err)
		return
	}
	defer conn.Close()

	buf.WriteByte('\n')
	conn.Write(buf.Bytes())
}

type PostHandlerConfig struct {
	URL string `mapstructure:"url"`
}

type postHandler struct {
	bp     *bufpool.Pool
	url    string
	logger *log.Logger
}

func NewPostHandler(c PostHandlerConfig, l *log.Logger) alert.Handler {
	return &postHandler{
		bp:     bufpool.New(),
		url:    c.URL,
		logger: l,
	}
}

func (h *postHandler) Handle(event alert.Event) {
	body := h.bp.Get()
	defer h.bp.Put(body)
	ad := alertDataFromEvent(event)

	err := json.NewEncoder(body).Encode(ad)
	if err != nil {
		h.logger.Printf("E! failed to marshal alert data json: %v", err)
		return
	}

	resp, err := http.Post(h.url, "application/json", body)
	if err != nil {
		h.logger.Printf("E! failed to POST alert data: %v", err)
		return
	}
	resp.Body.Close()
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

	logger  *log.Logger
	events  chan alert.Event
	closing chan struct{}

	wg sync.WaitGroup
}

func NewAggregateHandler(c AggregateHandlerConfig, l *log.Logger) (alert.Handler, error) {
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
		logger:      l,
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
	c      PublishHandlerConfig
	logger *log.Logger
}

func NewPublishHandler(c PublishHandlerConfig, l *log.Logger) alert.Handler {
	return &publishHandler{
		c:      c,
		logger: l,
	}
}

func (h *publishHandler) Handle(event alert.Event) {
	for _, t := range h.c.Topics {
		event.Topic = t
		h.c.ec.Collect(event)
	}
}

// TODO implement state changes only as a match condition
//type StateChangesOnlyHandlerConfig struct {
//	topics *alert.Topics
//}
//
//type stateChangesOnlyHandler struct {
//	topics *alert.Topics
//	logger *log.Logger
//	next   alert.Handler
//}
//
//func NewStateChangesOnlyHandler(c StateChangesOnlyHandlerConfig, l *log.Logger) handlerAction {
//	return &stateChangesOnlyHandler{
//		topics: c.topics,
//		logger: l,
//	}
//}
//
//func (h *stateChangesOnlyHandler) Handle(event alert.Event) {
//	if event.State.Level != event.PreviousState().Level {
//		h.next.Handle(event)
//	}
//}
//
//func (h *stateChangesOnlyHandler) SetNext(n alert.Handler) {
//	h.next = n
//}
//func (h *stateChangesOnlyHandler) Close() {}
