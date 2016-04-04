package kapacitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	html "html/template"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	text "text/template"
	"time"

	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick"
)

const (
	statsAlertsTriggered = "alerts_triggered"
)

// The newest state change is weighted 'weightDiff' times more than oldest state change.
const weightDiff = 1.5

// Maximum weight applied to newest state change.
const maxWeight = 1.2

type AlertHandler func(ad *AlertData)

type AlertLevel int

const (
	OKAlert AlertLevel = iota
	InfoAlert
	WarnAlert
	CritAlert
)

func (l AlertLevel) String() string {
	switch l {
	case OKAlert:
		return "OK"
	case InfoAlert:
		return "INFO"
	case WarnAlert:
		return "WARNING"
	case CritAlert:
		return "CRITICAL"
	default:
		panic("unknown AlertLevel")
	}
}

func (l AlertLevel) MarshalText() ([]byte, error) {
	return []byte(l.String()), nil
}

func (l *AlertLevel) UnmarshalText(text []byte) error {
	s := string(text)
	switch s {
	case "OK":
		*l = OKAlert
	case "INFO":
		*l = InfoAlert
	case "WARNING":
		*l = WarnAlert
	case "CRITICAL":
		*l = CritAlert
	default:
		return fmt.Errorf("unknown AlertLevel %s", s)
	}
	return nil
}

type AlertData struct {
	ID      string          `json:"id"`
	Message string          `json:"message"`
	Details string          `json:"details"`
	Time    time.Time       `json:"time"`
	Level   AlertLevel      `json:"level"`
	Data    influxql.Result `json:"data"`

	// Info for custom templates
	info detailsInfo
}

type AlertNode struct {
	node
	a           *pipeline.AlertNode
	endpoint    string
	handlers    []AlertHandler
	levels      []*tick.StatefulExpr
	states      map[models.GroupID]*alertState
	idTmpl      *text.Template
	messageTmpl *text.Template
	detailsTmpl *html.Template

	bufPool sync.Pool
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode, l *log.Logger) (an *AlertNode, err error) {
	an = &AlertNode{
		node: node{Node: n, et: et, logger: l},
		a:    n,
	}
	an.node.runF = an.runAlert

	// Create buffer pool for the templates
	an.bufPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	// Parse templates
	an.idTmpl, err = text.New("id").Parse(n.Id)
	if err != nil {
		return nil, err
	}

	an.messageTmpl, err = text.New("message").Parse(n.Message)
	if err != nil {
		return nil, err
	}

	an.detailsTmpl, err = html.New("details").Funcs(html.FuncMap{
		"json": func(v interface{}) html.JS {
			a, _ := json.Marshal(v)
			return html.JS(a)
		},
	}).Parse(n.Details)
	if err != nil {
		return nil, err
	}

	// Construct alert handlers
	an.handlers = make([]AlertHandler, 0)

	for _, post := range n.PostHandlers {
		post := post
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handlePost(post, ad) })
	}

	for _, email := range n.EmailHandlers {
		email := email
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleEmail(email, ad) })
	}
	if len(n.EmailHandlers) == 0 && (et.tm.SMTPService != nil && et.tm.SMTPService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleEmail(&pipeline.EmailHandler{}, ad) })
	}
	// If email has been configured with state changes only set it.
	if et.tm.SMTPService != nil &&
		et.tm.SMTPService.Global() &&
		et.tm.SMTPService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, exec := range n.ExecHandlers {
		exec := exec
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleExec(exec, ad) })
	}

	for _, log := range n.LogHandlers {
		log := log
		if !filepath.IsAbs(log.FilePath) {
			return nil, fmt.Errorf("alert log path must be absolute: %s is not absolute", log.FilePath)
		}
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleLog(log, ad) })
	}

	for _, vo := range n.VictorOpsHandlers {
		vo := vo
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleVictorOps(vo, ad) })
	}
	if len(n.VictorOpsHandlers) == 0 && (et.tm.VictorOpsService != nil && et.tm.VictorOpsService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleVictorOps(&pipeline.VictorOpsHandler{}, ad) })
	}

	for _, pd := range n.PagerDutyHandlers {
		pd := pd
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handlePagerDuty(pd, ad) })
	}
	if len(n.PagerDutyHandlers) == 0 && (et.tm.PagerDutyService != nil && et.tm.PagerDutyService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handlePagerDuty(&pipeline.PagerDutyHandler{}, ad) })
	}

	for _, sensu := range n.SensuHandlers {
		sensu := sensu
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleSensu(sensu, ad) })
	}

	for _, slack := range n.SlackHandlers {
		slack := slack
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleSlack(slack, ad) })
	}
	if len(n.SlackHandlers) == 0 && (et.tm.SlackService != nil && et.tm.SlackService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleSlack(&pipeline.SlackHandler{}, ad) })
	}
	// If slack has been configured with state changes only set it.
	if et.tm.SlackService != nil &&
		et.tm.SlackService.Global() &&
		et.tm.SlackService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, hipchat := range n.HipChatHandlers {
		hipchat := hipchat
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleHipChat(hipchat, ad) })
	}
	if len(n.HipChatHandlers) == 0 && (et.tm.HipChatService != nil && et.tm.HipChatService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleHipChat(&pipeline.HipChatHandler{}, ad) })
	}
	// If HipChat has been configured with state changes only set it.
	if et.tm.HipChatService != nil &&
		et.tm.HipChatService.Global() &&
		et.tm.HipChatService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, alerta := range n.AlertaHandlers {
		// Validate alerta templates
		rtmpl, err := text.New("resource").Parse(alerta.Resource)
		if err != nil {
			return nil, err
		}
		etmpl, err := text.New("environment").Parse(alerta.Environment)
		if err != nil {
			return nil, err
		}
		gtmpl, err := text.New("group").Parse(alerta.Group)
		if err != nil {
			return nil, err
		}
		vtmpl, err := text.New("value").Parse(alerta.Value)
		if err != nil {
			return nil, err
		}
		ai := alertaHandler{
			AlertaHandler:   alerta,
			resourceTmpl:    rtmpl,
			environmentTmpl: etmpl,
			groupTmpl:       gtmpl,
			valueTmpl:       vtmpl,
		}
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleAlerta(ai, ad) })
	}

	for _, og := range n.OpsGenieHandlers {
		og := og
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleOpsGenie(og, ad) })
	}
	if len(n.OpsGenieHandlers) == 0 && (et.tm.OpsGenieService != nil && et.tm.OpsGenieService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleOpsGenie(&pipeline.OpsGenieHandler{}, ad) })
	}

	for _, talk := range n.TalkHandlers {
		talk := talk
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleTalk(talk, ad) })
	}

	// Parse level expressions
	an.levels = make([]*tick.StatefulExpr, CritAlert+1)
	if n.Info != nil {
		an.levels[InfoAlert] = tick.NewStatefulExpr(n.Info)
	}
	if n.Warn != nil {
		an.levels[WarnAlert] = tick.NewStatefulExpr(n.Warn)
	}
	if n.Crit != nil {
		an.levels[CritAlert] = tick.NewStatefulExpr(n.Crit)
	}
	// Setup states
	if n.History < 2 {
		n.History = 2
	}
	an.states = make(map[models.GroupID]*alertState)

	// Configure flapping
	if n.UseFlapping {
		if n.FlapLow > 1 || n.FlapHigh > 1 {
			return nil, errors.New("alert flap thresholds are percentages and should be between 0 and 1")
		}
	}

	return
}

func (a *AlertNode) runAlert([]byte) error {
	a.statMap.Add(statsAlertsTriggered, 0)
	switch a.Wants() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			a.timer.Start()
			l := a.determineLevel(p.Time, p.Fields, p.Tags)
			state := a.updateState(l, p.Group)
			if (a.a.UseFlapping && state.flapping) || (a.a.IsStateChangesOnly && !state.changed) {
				a.timer.Stop()
				continue
			}
			// send alert if we are not OK or we are OK and state changed (i.e recovery)
			if l != OKAlert || state.changed {
				batch := models.Batch{
					Name:   p.Name,
					Group:  p.Group,
					Tags:   p.Tags,
					Points: []models.BatchPoint{models.BatchPointFromPoint(p)},
				}
				ad, err := a.alertData(p.Name, p.Group, p.Tags, p.Fields, l, p.Time, batch)
				if err != nil {
					return err
				}
				a.handleAlert(ad)
			}
			a.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			a.timer.Start()
			triggered := false
			for _, p := range b.Points {
				l := a.determineLevel(p.Time, p.Fields, p.Tags)
				if l > OKAlert {
					triggered = true
					state := a.updateState(l, b.Group)
					if (a.a.UseFlapping && state.flapping) || (a.a.IsStateChangesOnly && !state.changed) {
						break
					}
					ad, err := a.alertData(b.Name, b.Group, b.Tags, p.Fields, l, p.Time, b)
					if err != nil {
						return err
					}
					a.handleAlert(ad)
					break
				}
			}
			if !triggered {
				state := a.updateState(OKAlert, b.Group)
				if state.changed {
					var fields models.Fields
					if l := len(b.Points); l > 0 {
						fields = b.Points[l-1].Fields
					}
					ad, err := a.alertData(b.Name, b.Group, b.Tags, fields, OKAlert, b.TMax, b)
					if err != nil {
						return err
					}
					a.handleAlert(ad)
				}
			}
			a.timer.Stop()
		}
	}
	return nil
}
func (a *AlertNode) handleAlert(ad *AlertData) {
	a.statMap.Add(statsAlertsTriggered, 1)
	a.logger.Printf("D! %v alert triggered id:%s msg:%s data:%v", ad.Level, ad.ID, ad.Message, ad.Data.Series[0])
	for _, h := range a.handlers {
		h(ad)
	}
}

func (a *AlertNode) determineLevel(now time.Time, fields models.Fields, tags map[string]string) (level AlertLevel) {
	for l, se := range a.levels {
		if se == nil {
			continue
		}
		if pass, err := EvalPredicate(se, now, fields, tags); pass {
			level = AlertLevel(l)
		} else if err != nil {
			a.logger.Println("E! error evaluating expression:", err)
			return
		} else {
			return
		}
	}
	return
}

func (a *AlertNode) batchToResult(b models.Batch) influxql.Result {
	row := models.BatchToRow(b)
	r := influxql.Result{
		Series: imodels.Rows{row},
	}
	return r
}

func (a *AlertNode) alertData(
	name string,
	group models.GroupID,
	tags models.Tags,
	fields models.Fields,
	level AlertLevel,
	t time.Time,
	b models.Batch,
) (*AlertData, error) {
	id, err := a.renderID(name, group, tags)
	if err != nil {
		return nil, err
	}
	msg, details, info, err := a.renderMessageAndDetails(id, name, t, group, tags, fields, level)
	if err != nil {
		return nil, err
	}
	ad := &AlertData{
		ID:      id,
		Message: msg,
		Details: details,
		Time:    t,
		Level:   level,
		Data:    a.batchToResult(b),
		info:    info,
	}
	return ad, nil
}

type alertState struct {
	history  []AlertLevel
	idx      int
	flapping bool
	changed  bool
}

func (a *alertState) addEvent(level AlertLevel) {
	a.changed = a.history[a.idx] != level
	a.idx = (a.idx + 1) % len(a.history)
	a.history[a.idx] = level
}

func (a *alertState) percentChange() float64 {
	l := len(a.history)
	changes := 0.0
	weight := (maxWeight / weightDiff)
	step := (maxWeight - weight) / float64(l-1)
	for i := 0; i < l-1; i++ {
		// get current index
		c := (i + a.idx) % l
		// get previous index
		p := c - 1
		// check for wrap around
		if p < 0 {
			p = l - 1
		}
		if a.history[c] != a.history[p] {
			changes += weight
		}
		weight += step
	}

	p := changes / float64(l-1)
	return p
}

func (a *AlertNode) updateState(level AlertLevel, group models.GroupID) *alertState {
	state, ok := a.states[group]
	if !ok {
		state = &alertState{
			history: make([]AlertLevel, a.a.History),
		}
		a.states[group] = state
	}
	state.addEvent(level)

	if a.a.UseFlapping {
		p := state.percentChange()
		if state.flapping && p < a.a.FlapLow {
			state.flapping = false
		} else if !state.flapping && p > a.a.FlapHigh {
			state.flapping = true
		}
	}

	return state
}

// Type containing information available to ID template.
type idInfo struct {
	// Measurement name
	Name string

	// Task name
	TaskName string

	// Concatenation of all group-by tags of the form [key=value,]+.
	// If not groupBy is performed equal to literal 'nil'
	Group string

	// Map of tags
	Tags map[string]string
}

type messageInfo struct {
	idInfo

	// The ID of the alert.
	ID string

	// Fields of alerting data point.
	Fields map[string]interface{}

	// Alert Level, one of: INFO, WARNING, CRITICAL.
	Level string

	// Time
	Time time.Time
}

type detailsInfo struct {
	messageInfo
	// The Message of the Alert
	Message string
}

func (a *AlertNode) renderID(name string, group models.GroupID, tags models.Tags) (string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	info := idInfo{
		Name:     name,
		TaskName: a.et.Task.Name,
		Group:    g,
		Tags:     tags,
	}
	id := a.bufPool.Get().(*bytes.Buffer)
	defer a.bufPool.Put(id)
	id.Reset()
	err := a.idTmpl.Execute(id, info)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (a *AlertNode) renderMessageAndDetails(id, name string, t time.Time, group models.GroupID, tags models.Tags, fields models.Fields, level AlertLevel) (string, string, detailsInfo, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	minfo := messageInfo{
		idInfo: idInfo{
			Name:     name,
			TaskName: a.et.Task.Name,
			Group:    g,
			Tags:     tags,
		},
		ID:     id,
		Fields: fields,
		Level:  level.String(),
		Time:   t,
	}

	// Grab a buffer for the message template and the details template
	tmpBuffer := a.bufPool.Get().(*bytes.Buffer)
	defer a.bufPool.Put(tmpBuffer)
	tmpBuffer.Reset()

	err := a.messageTmpl.Execute(tmpBuffer, minfo)
	if err != nil {
		return "", "", detailsInfo{}, err
	}

	msg := tmpBuffer.String()
	dinfo := detailsInfo{
		messageInfo: minfo,
		Message:     msg,
	}

	// Reuse the buffer, for the details template
	tmpBuffer.Reset()
	err = a.detailsTmpl.Execute(tmpBuffer, dinfo)
	if err != nil {
		return "", "", dinfo, err
	}

	details := tmpBuffer.String()
	return msg, details, dinfo, nil
}

//--------------------------------
// Alert handlers

func (a *AlertNode) handlePost(post *pipeline.PostHandler, ad *AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	buf := bytes.NewBuffer(b)
	_, err = http.Post(post.URL, "application/json", buf)
	if err != nil {
		a.logger.Println("E! failed to POST batch", err)
	}
}

func (a *AlertNode) handleEmail(email *pipeline.EmailHandler, ad *AlertData) {
	if a.et.tm.SMTPService != nil {
		err := a.et.tm.SMTPService.SendMail(email.ToList, ad.Message, ad.Details)
		if err != nil {
			a.logger.Println("E!", err)
		}
	} else {
		a.logger.Println("E! smtp service not enabled, cannot send email.")
	}
}

func (a *AlertNode) handleExec(ex *pipeline.ExecHandler, ad *AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	cmd := exec.Command(ex.Command[0], ex.Command[1:]...)
	cmd.Stdin = bytes.NewBuffer(b)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	err = cmd.Run()
	if err != nil {
		a.logger.Println("E! error running alert command:", err, out.String())
		return
	}
}

func (a *AlertNode) handleLog(l *pipeline.LogHandler, ad *AlertData) {
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	f, err := os.OpenFile(l.FilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(l.Mode))
	if err != nil {
		a.logger.Println("E! failed to open file for alert logging", err)
		return
	}
	defer f.Close()
	n, err := f.Write(b)
	if n != len(b) || err != nil {
		a.logger.Println("E! failed to write to file", err)
	}
	n, err = f.Write([]byte("\n"))
	if n != 1 || err != nil {
		a.logger.Println("E! failed to write to file", err)
	}
}

func (a *AlertNode) handleVictorOps(vo *pipeline.VictorOpsHandler, ad *AlertData) {
	if a.et.tm.VictorOpsService == nil {
		a.logger.Println("E! failed to send VictorOps alert. VictorOps is not enabled")
		return
	}
	var messageType string
	switch ad.Level {
	case OKAlert:
		messageType = "RECOVERY"
	default:
		messageType = ad.Level.String()
	}
	err := a.et.tm.VictorOpsService.Alert(
		vo.RoutingKey,
		messageType,
		ad.Message,
		ad.ID,
		ad.Time,
		ad.Data,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to VictorOps:", err)
		return
	}
}

func (a *AlertNode) handlePagerDuty(pd *pipeline.PagerDutyHandler, ad *AlertData) {
	if a.et.tm.PagerDutyService == nil {
		a.logger.Println("E! failed to send PagerDuty alert. PagerDuty is not enabled")
		return
	}
	err := a.et.tm.PagerDutyService.Alert(
		ad.ID,
		ad.Message,
		ad.Level,
		ad.Data,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to PagerDuty:", err)
		return
	}
}

func (a *AlertNode) handleSensu(sensu *pipeline.SensuHandler, ad *AlertData) {
	if a.et.tm.SensuService == nil {
		a.logger.Println("E! failed to send Sensu message. Sensu is not enabled")
		return
	}

	err := a.et.tm.SensuService.Alert(
		ad.ID,
		ad.Message,
		ad.Level,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to Sensu:", err)
		return
	}
}

func (a *AlertNode) handleSlack(slack *pipeline.SlackHandler, ad *AlertData) {
	if a.et.tm.SlackService == nil {
		a.logger.Println("E! failed to send Slack message. Slack is not enabled")
		return
	}
	err := a.et.tm.SlackService.Alert(
		slack.Channel,
		ad.Message,
		ad.Level,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to Slack:", err)
		return
	}
}

func (a *AlertNode) handleHipChat(hipchat *pipeline.HipChatHandler, ad *AlertData) {
	if a.et.tm.HipChatService == nil {
		a.logger.Println("E! failed to send HipChat message. HipChat is not enabled")
		return
	}
	err := a.et.tm.HipChatService.Alert(
		hipchat.Room,
		hipchat.Token,
		ad.Message,
		ad.Level,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to HipChat:", err)
		return
	}
}

type alertaHandler struct {
	*pipeline.AlertaHandler

	resourceTmpl    *text.Template
	environmentTmpl *text.Template
	valueTmpl       *text.Template
	groupTmpl       *text.Template
}

func (a *AlertNode) handleAlerta(alerta alertaHandler, ad *AlertData) {
	if a.et.tm.AlertaService == nil {
		a.logger.Println("E! failed to send Alerta message. Alerta is not enabled")
		return
	}

	var severity string
	var status string

	switch ad.Level {
	case OKAlert:
		severity = "ok"
		status = "closed"
	case InfoAlert:
		severity = "informational"
		status = "open"
	case WarnAlert:
		severity = "warning"
		status = "open"
	case CritAlert:
		severity = "critical"
		status = "open"
	default:
		severity = "unknown"
		status = "unknown"
	}
	var buf bytes.Buffer
	err := alerta.resourceTmpl.Execute(&buf, ad.info)
	if err != nil {
		a.logger.Printf("E! failed to evaluate Alerta Resource template %s", alerta.Resource)
		return
	}
	resource := buf.String()
	buf.Reset()

	err = alerta.environmentTmpl.Execute(&buf, ad.info)
	if err != nil {
		a.logger.Printf("E! failed to evaluate Alerta Environment template %s", alerta.Environment)
		return
	}
	environment := buf.String()
	buf.Reset()

	err = alerta.groupTmpl.Execute(&buf, ad.info)
	if err != nil {
		a.logger.Printf("E! failed to evaluate Alerta Group template %s", alerta.Group)
		return
	}
	group := buf.String()
	buf.Reset()

	err = alerta.valueTmpl.Execute(&buf, ad.info)
	if err != nil {
		a.logger.Printf("E! failed to evaluate Alerta Value template %s", alerta.Value)
		return
	}
	value := buf.String()

	service := alerta.Service
	if len(alerta.Service) == 0 {
		service = []string{ad.info.Name}
	}

	err = a.et.tm.AlertaService.Alert(
		alerta.Token,
		resource,
		ad.ID,
		environment,
		severity,
		status,
		group,
		value,
		ad.Message,
		alerta.Origin,
		service,
		ad.Data,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to Alerta:", err)
		return
	}
}

func (a *AlertNode) handleOpsGenie(og *pipeline.OpsGenieHandler, ad *AlertData) {
	if a.et.tm.OpsGenieService == nil {
		a.logger.Println("E! failed to send OpsGenie alert. OpsGenie is not enabled")
		return
	}
	var messageType string
	switch ad.Level {
	case OKAlert:
		messageType = "RECOVERY"
	default:
		messageType = ad.Level.String()
	}

	err := a.et.tm.OpsGenieService.Alert(
		og.TeamsList,
		og.RecipientsList,
		messageType,
		ad.Message,
		ad.ID,
		ad.Time,
		ad.Data,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to OpsGenie:", err)
		return
	}
}

func (a *AlertNode) handleTalk(talk *pipeline.TalkHandler, ad *AlertData) {
	if a.et.tm.TalkService == nil {
		a.logger.Println("E! failed to send Talk message. Talk is not enabled")
		return
	}

	err := a.et.tm.TalkService.Alert(
		ad.ID,
		ad.Message,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to Talk:", err)
		return
	}
}
