package kapacitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path"
	"text/template"
	"time"

	"github.com/influxdb/influxdb/influxql"
	imodels "github.com/influxdb/influxdb/models"
	"github.com/influxdb/kapacitor/models"
	"github.com/influxdb/kapacitor/pipeline"
	"github.com/influxdb/kapacitor/tick"
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

type AlertData struct {
	ID      string          `json:"id"`
	Message string          `json:"message"`
	Time    time.Time       `json:"time"`
	Level   AlertLevel      `json:"level"`
	Data    influxql.Result `json:"data"`
}

type AlertNode struct {
	node
	a           *pipeline.AlertNode
	endpoint    string
	handlers    []AlertHandler
	levels      []*tick.StatefulExpr
	states      map[models.GroupID]*alertState
	idTmpl      *template.Template
	messageTmpl *template.Template
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode) (an *AlertNode, err error) {
	an = &AlertNode{
		node: node{Node: n, et: et},
		a:    n,
	}
	an.node.runF = an.runAlert

	// Parse templates
	tmpl, err := template.New("id").Parse(n.Id)
	if err != nil {
		return nil, err
	}
	an.idTmpl = tmpl
	tmpl, err = template.New("message").Parse(n.Message)
	if err != nil {
		return nil, err
	}
	an.messageTmpl = tmpl

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
	// If email has been configured globally only send state changes.
	if et.tm.SMTPService != nil && et.tm.SMTPService.Global() {
		n.IsStateChangesOnly = true
	}

	for _, exec := range n.ExecHandlers {
		exec := exec
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleExec(exec, ad) })
	}

	for _, log := range n.LogHandlers {
		log := log
		if !path.IsAbs(log.FilePath) {
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

	for _, slack := range n.SlackHandlers {
		slack := slack
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleSlack(slack, ad) })
	}
	if len(n.SlackHandlers) == 0 && (et.tm.SlackService != nil && et.tm.SlackService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleSlack(&pipeline.SlackHandler{}, ad) })
	}
	// If slack has been configured globally only send state changes.
	if et.tm.SlackService != nil && et.tm.SlackService.Global() {
		n.IsStateChangesOnly = true
	}

	for _, og := range n.OpsGenieHandlers {
		og := og
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleOpsGenie(og, ad) })
	}
	if len(n.OpsGenieHandlers) == 0 && (et.tm.OpsGenieService != nil && et.tm.OpsGenieService.Global()) {
		an.handlers = append(an.handlers, func(ad *AlertData) { an.handleOpsGenie(&pipeline.OpsGenieHandler{}, ad) })
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

func (a *AlertNode) runAlert() error {
	switch a.Wants() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			l := a.determineLevel(p.Fields, p.Tags)
			state := a.updateState(l, p.Group)
			if (a.a.UseFlapping && state.flapping) || (a.a.IsStateChangesOnly && !state.changed) {
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
				for _, h := range a.handlers {
					h(ad)
				}
			}
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			triggered := false
			for _, p := range b.Points {
				l := a.determineLevel(p.Fields, p.Tags)
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
					for _, h := range a.handlers {
						h(ad)
					}
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
					for _, h := range a.handlers {
						h(ad)
					}
				}
			}
		}
	}
	return nil
}

func (a *AlertNode) determineLevel(fields models.Fields, tags map[string]string) (level AlertLevel) {
	for l, se := range a.levels {
		if se == nil {
			continue
		}
		if pass, err := EvalPredicate(se, fields, tags); pass {
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
	msg, err := a.renderMessage(id, name, group, tags, fields, level)
	if err != nil {
		return nil, err
	}
	ad := &AlertData{
		id,
		msg,
		t,
		level,
		a.batchToResult(b),
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
}

func (a *AlertNode) renderID(name string, group models.GroupID, tags models.Tags) (string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	info := idInfo{
		Name:  name,
		Group: g,
		Tags:  tags,
	}
	var id bytes.Buffer
	err := a.idTmpl.Execute(&id, info)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (a *AlertNode) renderMessage(id, name string, group models.GroupID, tags models.Tags, fields models.Fields, level AlertLevel) (string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	info := messageInfo{
		idInfo: idInfo{
			Name:  name,
			Group: g,
			Tags:  tags,
		},
		ID:     id,
		Fields: fields,
		Level:  level.String(),
	}
	var msg bytes.Buffer
	err := a.messageTmpl.Execute(&msg, info)
	if err != nil {
		return "", err
	}
	return msg.String(), nil
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
	b, err := json.Marshal(ad)
	if err != nil {
		a.logger.Println("E! failed to marshal alert data json", err)
		return
	}
	if a.et.tm.SMTPService != nil {
		a.et.tm.SMTPService.SendMail(email.ToList, ad.Message, string(b))
	} else {
		a.logger.Println("W! smtp service not enabled, cannot send email.")
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
	f, err := os.OpenFile(l.FilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
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
		ad.Data,
	)
	if err != nil {
		a.logger.Println("E! failed to send alert data to PagerDuty:", err)
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
