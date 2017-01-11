package kapacitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	html "html/template"
	"log"
	"os"
	"sync"
	text "text/template"
	"time"

	"github.com/influxdata/influxdb/influxql"
	imodels "github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	alertservice "github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/tick/stateful"
	"github.com/pkg/errors"
)

const (
	statsAlertsTriggered = "alerts_triggered"
	statsOKsTriggered    = "oks_triggered"
	statsInfosTriggered  = "infos_triggered"
	statsWarnsTriggered  = "warns_triggered"
	statsCritsTriggered  = "crits_triggered"
	statsEventsDropped   = "events_dropped"
)

// The newest state change is weighted 'weightDiff' times more than oldest state change.
const weightDiff = 1.5

// Maximum weight applied to newest state change.
const maxWeight = 1.2

type AlertNode struct {
	node
	a           *pipeline.AlertNode
	topic       string
	anonTopic   string
	handlers    []alert.Handler
	levels      []stateful.Expression
	scopePools  []stateful.ScopePool
	states      map[models.GroupID]*alertState
	idTmpl      *text.Template
	messageTmpl *text.Template
	detailsTmpl *html.Template

	alertsTriggered *expvar.Int
	oksTriggered    *expvar.Int
	infosTriggered  *expvar.Int
	warnsTriggered  *expvar.Int
	critsTriggered  *expvar.Int
	eventsDropped   *expvar.Int

	bufPool sync.Pool

	levelResets  []stateful.Expression
	lrScopePools []stateful.ScopePool
}

// Create a new  AlertNode which caches the most recent item and exposes it over the HTTP API.
func newAlertNode(et *ExecutingTask, n *pipeline.AlertNode, l *log.Logger) (an *AlertNode, err error) {
	an = &AlertNode{
		node: node{Node: n, et: et, logger: l},
		a:    n,
	}
	an.node.runF = an.runAlert

	an.topic = n.Topic
	// Create anonymous topic name
	an.anonTopic = fmt.Sprintf("%s:%s:%s", et.tm.ID(), et.Task.ID, an.Name())

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

			tmpBuffer := an.bufPool.Get().(*bytes.Buffer)
			defer func() {
				tmpBuffer.Reset()
				an.bufPool.Put(tmpBuffer)
			}()

			_ = json.NewEncoder(tmpBuffer).Encode(v)

			return html.JS(tmpBuffer.String())
		},
	}).Parse(n.Details)
	if err != nil {
		return nil, err
	}

	// Construct alert handlers
	for _, post := range n.PostHandlers {
		c := alertservice.PostHandlerConfig{
			URL: post.URL,
		}
		h := alertservice.NewPostHandler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for _, tcp := range n.TcpHandlers {
		c := alertservice.TCPHandlerConfig{
			Address: tcp.Address,
		}
		h := alertservice.NewTCPHandler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for _, email := range n.EmailHandlers {
		c := smtp.HandlerConfig{
			To: email.ToList,
		}
		h := et.tm.SMTPService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.EmailHandlers) == 0 && (et.tm.SMTPService != nil && et.tm.SMTPService.Global()) {
		c := smtp.HandlerConfig{}
		h := et.tm.SMTPService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	// If email has been configured with state changes only set it.
	if et.tm.SMTPService != nil &&
		et.tm.SMTPService.Global() &&
		et.tm.SMTPService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, e := range n.ExecHandlers {
		c := alertservice.ExecHandlerConfig{
			Prog:      e.Command[0],
			Args:      e.Command[1:],
			Commander: et.tm.Commander,
		}
		h := alertservice.NewExecHandler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for _, log := range n.LogHandlers {
		c := alertservice.DefaultLogHandlerConfig()
		c.Path = log.FilePath
		if log.Mode != 0 {
			c.Mode = os.FileMode(log.Mode)
		}
		h, err := alertservice.NewLogHandler(c, l)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create log alert handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, vo := range n.VictorOpsHandlers {
		c := victorops.HandlerConfig{
			RoutingKey: vo.RoutingKey,
		}
		h := et.tm.VictorOpsService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.VictorOpsHandlers) == 0 && (et.tm.VictorOpsService != nil && et.tm.VictorOpsService.Global()) {
		c := victorops.HandlerConfig{}
		h := et.tm.VictorOpsService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for _, pd := range n.PagerDutyHandlers {
		c := pagerduty.HandlerConfig{
			ServiceKey: pd.ServiceKey,
		}
		h := et.tm.PagerDutyService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.PagerDutyHandlers) == 0 && (et.tm.PagerDutyService != nil && et.tm.PagerDutyService.Global()) {
		c := pagerduty.HandlerConfig{}
		h := et.tm.PagerDutyService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for range n.SensuHandlers {
		h := et.tm.SensuService.Handler(l)
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.SlackHandlers {
		c := slack.HandlerConfig{
			Channel:   s.Channel,
			Username:  s.Username,
			IconEmoji: s.IconEmoji,
		}
		h := et.tm.SlackService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.SlackHandlers) == 0 && (et.tm.SlackService != nil && et.tm.SlackService.Global()) {
		h := et.tm.SlackService.Handler(slack.HandlerConfig{}, l)
		an.handlers = append(an.handlers, h)
	}
	// If slack has been configured with state changes only set it.
	if et.tm.SlackService != nil &&
		et.tm.SlackService.Global() &&
		et.tm.SlackService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, t := range n.TelegramHandlers {
		c := telegram.HandlerConfig{
			ChatId:                t.ChatId,
			ParseMode:             t.ParseMode,
			DisableWebPagePreview: t.IsDisableWebPagePreview,
			DisableNotification:   t.IsDisableNotification,
		}
		h := et.tm.TelegramService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for _, s := range n.SNMPTrapHandlers {
		dataList := make([]snmptrap.Data, len(s.DataList))
		for i, d := range s.DataList {
			dataList[i] = snmptrap.Data{
				Oid:   d.Oid,
				Type:  d.Type,
				Value: d.Value,
			}
		}
		c := snmptrap.HandlerConfig{
			TrapOid:  s.TrapOid,
			DataList: dataList,
		}
		h, err := et.tm.SNMPTrapService.Handler(c, l)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create SNMP handler")
		}
		an.handlers = append(an.handlers, h)
	}

	if len(n.TelegramHandlers) == 0 && (et.tm.TelegramService != nil && et.tm.TelegramService.Global()) {
		c := telegram.HandlerConfig{}
		h := et.tm.TelegramService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	// If telegram has been configured with state changes only set it.
	if et.tm.TelegramService != nil &&
		et.tm.TelegramService.Global() &&
		et.tm.TelegramService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, hc := range n.HipChatHandlers {
		c := hipchat.HandlerConfig{
			Room:  hc.Room,
			Token: hc.Token,
		}
		h := et.tm.HipChatService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.HipChatHandlers) == 0 && (et.tm.HipChatService != nil && et.tm.HipChatService.Global()) {
		c := hipchat.HandlerConfig{}
		h := et.tm.HipChatService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	// If HipChat has been configured with state changes only set it.
	if et.tm.HipChatService != nil &&
		et.tm.HipChatService.Global() &&
		et.tm.HipChatService.StateChangesOnly() {
		n.IsStateChangesOnly = true
	}

	for _, a := range n.AlertaHandlers {
		c := et.tm.AlertaService.DefaultHandlerConfig()
		if a.Token != "" {
			c.Token = a.Token
		}
		if a.Resource != "" {
			c.Resource = a.Resource
		}
		if a.Event != "" {
			c.Event = a.Event
		}
		if a.Environment != "" {
			c.Environment = a.Environment
		}
		if a.Group != "" {
			c.Group = a.Group
		}
		if a.Value != "" {
			c.Value = a.Value
		}
		if a.Origin != "" {
			c.Origin = a.Origin
		}
		if len(a.Service) != 0 {
			c.Service = a.Service
		}
		h, err := et.tm.AlertaService.Handler(c, l)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Alerta handler")
		}
		an.handlers = append(an.handlers, h)
	}

	for _, og := range n.OpsGenieHandlers {
		c := opsgenie.HandlerConfig{
			TeamsList:      og.TeamsList,
			RecipientsList: og.RecipientsList,
		}
		h := et.tm.OpsGenieService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}
	if len(n.OpsGenieHandlers) == 0 && (et.tm.OpsGenieService != nil && et.tm.OpsGenieService.Global()) {
		c := opsgenie.HandlerConfig{}
		h := et.tm.OpsGenieService.Handler(c, l)
		an.handlers = append(an.handlers, h)
	}

	for range n.TalkHandlers {
		h := et.tm.TalkService.Handler(l)
		an.handlers = append(an.handlers, h)
	}

	// Parse level expressions
	an.levels = make([]stateful.Expression, alert.Critical+1)
	an.scopePools = make([]stateful.ScopePool, alert.Critical+1)

	an.levelResets = make([]stateful.Expression, alert.Critical+1)
	an.lrScopePools = make([]stateful.ScopePool, alert.Critical+1)

	if n.Info != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Info.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for info: %s", expressionCompileError)
		}

		an.levels[alert.Info] = statefulExpression
		an.scopePools[alert.Info] = stateful.NewScopePool(stateful.FindReferenceVariables(n.Info.Expression))
		if n.InfoReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.InfoReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for infoReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Info] = lstatefulExpression
			an.lrScopePools[alert.Info] = stateful.NewScopePool(stateful.FindReferenceVariables(n.InfoReset.Expression))
		}
	}

	if n.Warn != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Warn.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for warn: %s", expressionCompileError)
		}
		an.levels[alert.Warning] = statefulExpression
		an.scopePools[alert.Warning] = stateful.NewScopePool(stateful.FindReferenceVariables(n.Warn.Expression))
		if n.WarnReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.WarnReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for warnReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Warning] = lstatefulExpression
			an.lrScopePools[alert.Warning] = stateful.NewScopePool(stateful.FindReferenceVariables(n.WarnReset.Expression))
		}
	}

	if n.Crit != nil {
		statefulExpression, expressionCompileError := stateful.NewExpression(n.Crit.Expression)
		if expressionCompileError != nil {
			return nil, fmt.Errorf("Failed to compile stateful expression for crit: %s", expressionCompileError)
		}
		an.levels[alert.Critical] = statefulExpression
		an.scopePools[alert.Critical] = stateful.NewScopePool(stateful.FindReferenceVariables(n.Crit.Expression))
		if n.CritReset != nil {
			lstatefulExpression, lexpressionCompileError := stateful.NewExpression(n.CritReset.Expression)
			if lexpressionCompileError != nil {
				return nil, fmt.Errorf("Failed to compile stateful expression for critReset: %s", lexpressionCompileError)
			}
			an.levelResets[alert.Critical] = lstatefulExpression
			an.lrScopePools[alert.Critical] = stateful.NewScopePool(stateful.FindReferenceVariables(n.CritReset.Expression))
		}
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
	// Register delete hook
	if a.hasAnonTopic() {
		a.et.tm.registerDeleteHookForTask(a.et.Task.ID, deleteAlertHook(a.anonTopic))

		// Register Handlers on topic
		for _, h := range a.handlers {
			a.et.tm.AlertService.RegisterHandler([]string{a.anonTopic}, h)
		}
		// Restore anonTopic
		a.et.tm.AlertService.RestoreTopic(a.anonTopic)
	}

	// Setup stats
	a.alertsTriggered = &expvar.Int{}
	a.statMap.Set(statsAlertsTriggered, a.alertsTriggered)

	a.oksTriggered = &expvar.Int{}
	a.statMap.Set(statsOKsTriggered, a.oksTriggered)

	a.infosTriggered = &expvar.Int{}
	a.statMap.Set(statsInfosTriggered, a.infosTriggered)

	a.warnsTriggered = &expvar.Int{}
	a.statMap.Set(statsWarnsTriggered, a.warnsTriggered)

	a.critsTriggered = &expvar.Int{}
	a.statMap.Set(statsCritsTriggered, a.critsTriggered)

	a.eventsDropped = &expvar.Int{}
	a.statMap.Set(statsCritsTriggered, a.critsTriggered)

	switch a.Wants() {
	case pipeline.StreamEdge:
		for p, ok := a.ins[0].NextPoint(); ok; p, ok = a.ins[0].NextPoint() {
			a.timer.Start()
			id, err := a.renderID(p.Name, p.Group, p.Tags)
			if err != nil {
				return err
			}
			var currentLevel alert.Level
			if state, ok := a.states[p.Group]; ok {
				currentLevel = state.currentLevel()
			} else {
				// Check for previous state
				currentLevel = a.restoreEventState(id)
				if currentLevel != alert.OK {
					// Update the state with the restored state
					a.updateState(p.Time, currentLevel, p.Group)
				}
			}
			l := a.determineLevel(p.Time, p.Fields, p.Tags, currentLevel)
			state := a.updateState(p.Time, l, p.Group)
			if (a.a.UseFlapping && state.flapping) || (a.a.IsStateChangesOnly && !state.changed && !state.expired) {
				a.timer.Stop()
				continue
			}
			// send alert if we are not OK or we are OK and state changed (i.e recovery)
			if l != alert.OK || state.changed {
				batch := models.Batch{
					Name:   p.Name,
					Group:  p.Group,
					ByName: p.Dimensions.ByName,
					Tags:   p.Tags,
					Points: []models.BatchPoint{models.BatchPointFromPoint(p)},
				}
				state.triggered(p.Time)
				// Suppress the recovery event.
				if a.a.NoRecoveriesFlag && l == alert.OK {
					a.timer.Stop()
					continue
				}
				duration := state.duration()
				event, err := a.event(id, p.Name, p.Group, p.Tags, p.Fields, l, p.Time, duration, batch)
				if err != nil {
					return err
				}
				a.handleEvent(event)
				if a.a.LevelTag != "" || a.a.IdTag != "" {
					p.Tags = p.Tags.Copy()
					if a.a.LevelTag != "" {
						p.Tags[a.a.LevelTag] = l.String()
					}
					if a.a.IdTag != "" {
						p.Tags[a.a.IdTag] = event.State.ID
					}
				}
				if a.a.LevelField != "" || a.a.IdField != "" || a.a.DurationField != "" || a.a.MessageField != "" {
					p.Fields = p.Fields.Copy()
					if a.a.LevelField != "" {
						p.Fields[a.a.LevelField] = l.String()
					}
					if a.a.MessageField != "" {
						p.Fields[a.a.MessageField] = event.State.Message
					}
					if a.a.IdField != "" {
						p.Fields[a.a.IdField] = event.State.ID
					}
					if a.a.DurationField != "" {
						p.Fields[a.a.DurationField] = int64(duration)
					}
				}
				a.timer.Pause()
				for _, child := range a.outs {
					err := child.CollectPoint(p)
					if err != nil {
						return err
					}
				}
				a.timer.Resume()
			}
			a.timer.Stop()
		}
	case pipeline.BatchEdge:
		for b, ok := a.ins[0].NextBatch(); ok; b, ok = a.ins[0].NextBatch() {
			a.timer.Start()
			id, err := a.renderID(b.Name, b.Group, b.Tags)
			if err != nil {
				return err
			}
			if len(b.Points) == 0 {
				a.timer.Stop()
				continue
			}
			// Keep track of lowest level for any point
			lowestLevel := alert.Critical
			// Keep track of highest level and point
			highestLevel := alert.OK
			var highestPoint *models.BatchPoint

			var currentLevel alert.Level
			if state, ok := a.states[b.Group]; ok {
				currentLevel = state.currentLevel()
			} else {
				// Check for previous state
				currentLevel = a.restoreEventState(id)
				if currentLevel != alert.OK {
					// Update the state with the restored state
					a.updateState(b.TMax, currentLevel, b.Group)
				}
			}
			for i, p := range b.Points {
				l := a.determineLevel(p.Time, p.Fields, p.Tags, currentLevel)
				if l < lowestLevel {
					lowestLevel = l
				}
				if l > highestLevel || highestPoint == nil {
					highestLevel = l
					highestPoint = &b.Points[i]
				}
			}

			// Default the determined level to lowest.
			l := lowestLevel
			// Update determined level to highest if we don't care about all
			if !a.a.AllFlag {
				l = highestLevel
			}
			// Create alert Data
			t := highestPoint.Time
			if a.a.AllFlag || l == alert.OK {
				t = b.TMax
			}

			// Update state
			state := a.updateState(t, l, b.Group)
			// Trigger alert if:
			//  l == OK and state.changed (aka recovery)
			//    OR
			//  l != OK and flapping/statechanges checkout
			if state.changed && l == alert.OK ||
				(l != alert.OK &&
					!((a.a.UseFlapping && state.flapping) ||
						(a.a.IsStateChangesOnly && !state.changed && !state.expired))) {
				state.triggered(t)
				// Suppress the recovery event.
				if a.a.NoRecoveriesFlag && l == alert.OK {
					a.timer.Stop()
					continue
				}

				duration := state.duration()
				event, err := a.event(id, b.Name, b.Group, b.Tags, highestPoint.Fields, l, t, duration, b)
				if err != nil {
					return err
				}
				a.handleEvent(event)
				// Update tags or fields for Level property
				if a.a.LevelTag != "" ||
					a.a.LevelField != "" ||
					a.a.IdTag != "" ||
					a.a.IdField != "" ||
					a.a.DurationField != "" ||
					a.a.MessageField != "" {
					for i := range b.Points {
						if a.a.LevelTag != "" || a.a.IdTag != "" {
							b.Points[i].Tags = b.Points[i].Tags.Copy()
							if a.a.LevelTag != "" {
								b.Points[i].Tags[a.a.LevelTag] = l.String()
							}
							if a.a.IdTag != "" {
								b.Points[i].Tags[a.a.IdTag] = event.State.ID
							}
						}
						if a.a.LevelField != "" || a.a.IdField != "" || a.a.DurationField != "" || a.a.MessageField != "" {
							b.Points[i].Fields = b.Points[i].Fields.Copy()
							if a.a.LevelField != "" {
								b.Points[i].Fields[a.a.LevelField] = l.String()
							}
							if a.a.MessageField != "" {
								b.Points[i].Fields[a.a.MessageField] = event.State.Message
							}
							if a.a.IdField != "" {
								b.Points[i].Fields[a.a.IdField] = event.State.ID
							}
							if a.a.DurationField != "" {
								b.Points[i].Fields[a.a.DurationField] = int64(duration)
							}
						}
					}
					if a.a.LevelTag != "" || a.a.IdTag != "" {
						b.Tags = b.Tags.Copy()
						if a.a.LevelTag != "" {
							b.Tags[a.a.LevelTag] = l.String()
						}
						if a.a.IdTag != "" {
							b.Tags[a.a.IdTag] = event.State.ID
						}
					}
				}
				a.timer.Pause()
				for _, child := range a.outs {
					err := child.CollectBatch(b)
					if err != nil {
						return err
					}
				}
				a.timer.Resume()
			}
			a.timer.Stop()
		}
	}
	// Close the anonymous topic.
	a.et.tm.AlertService.CloseTopic(a.anonTopic)
	// Deregister Handlers on topic
	for _, h := range a.handlers {
		a.et.tm.AlertService.DeregisterHandler([]string{a.anonTopic}, h)
	}
	return nil
}

func deleteAlertHook(anonTopic string) deleteHook {
	return func(tm *TaskMaster) {
		tm.AlertService.DeleteTopic(anonTopic)
	}
}

func (a *AlertNode) hasAnonTopic() bool {
	return len(a.handlers) > 0
}
func (a *AlertNode) hasTopic() bool {
	return a.topic != ""
}

func (a *AlertNode) restoreEventState(id string) alert.Level {
	var topicState, anonTopicState alert.EventState
	var anonFound, topicFound bool
	// Check for previous state on anonTopic
	if a.hasAnonTopic() {
		if state, ok := a.et.tm.AlertService.EventState(a.anonTopic, id); ok {
			anonTopicState = state
			anonFound = true
		}
	}
	// Check for previous state on topic.
	if a.hasTopic() {
		if state, ok := a.et.tm.AlertService.EventState(a.topic, id); ok {
			topicState = state
			topicFound = true
		}
	}
	if topicState.Level != anonTopicState.Level {
		if anonFound && topicFound {
			// Anon topic takes precedence
			if err := a.et.tm.AlertService.UpdateEvent(a.topic, anonTopicState); err != nil {
				a.logger.Printf("E! failed to update topic %q event state for event %q", a.topic, id)
			}
		} else if topicFound && a.hasAnonTopic() {
			// Update event state for topic
			if err := a.et.tm.AlertService.UpdateEvent(a.anonTopic, topicState); err != nil {
				a.logger.Printf("E! failed to update topic %q event state for event %q", a.topic, id)
			}
		} // else nothing was found, nothing to do
	}
	if anonFound {
		return anonTopicState.Level
	}
	return topicState.Level
}

func (a *AlertNode) handleEvent(event alert.Event) {
	a.alertsTriggered.Add(1)
	switch event.State.Level {
	case alert.OK:
		a.oksTriggered.Add(1)
	case alert.Info:
		a.infosTriggered.Add(1)
	case alert.Warning:
		a.warnsTriggered.Add(1)
	case alert.Critical:
		a.critsTriggered.Add(1)
	}
	a.logger.Printf("D! %v alert triggered id:%s msg:%s data:%v", event.State.Level, event.State.ID, event.State.Message, event.Data.Result.Series[0])

	// If we have anon handlers, emit event to the anonTopic
	if a.hasAnonTopic() {
		event.Topic = a.anonTopic
		err := a.et.tm.AlertService.Collect(event)
		if err != nil {
			a.eventsDropped.Add(1)
			a.logger.Println("E!", err)
		}
	}

	// If we have a user define topic, emit event to the topic.
	if a.hasTopic() {
		event.Topic = a.topic
		err := a.et.tm.AlertService.Collect(event)
		if err != nil {
			a.eventsDropped.Add(1)
			a.logger.Println("E!", err)
		}
	}
}

func (a *AlertNode) determineLevel(now time.Time, fields models.Fields, tags map[string]string, currentLevel alert.Level) alert.Level {
	if higherLevel, found := a.findFirstMatchLevel(alert.Critical, currentLevel-1, now, fields, tags); found {
		return higherLevel
	}
	if rse := a.levelResets[currentLevel]; rse != nil {
		if pass, err := EvalPredicate(rse, a.lrScopePools[currentLevel], now, fields, tags); err != nil {
			a.logger.Printf("E! error evaluating reset expression for current level %v: %s", currentLevel, err)
		} else if !pass {
			return currentLevel
		}
	}
	if newLevel, found := a.findFirstMatchLevel(currentLevel, alert.OK, now, fields, tags); found {
		return newLevel
	}
	return alert.OK
}

func (a *AlertNode) findFirstMatchLevel(start alert.Level, stop alert.Level, now time.Time, fields models.Fields, tags map[string]string) (alert.Level, bool) {
	if stop < alert.OK {
		stop = alert.OK
	}
	for l := start; l > stop; l-- {
		se := a.levels[l]
		if se == nil {
			continue
		}
		if pass, err := EvalPredicate(se, a.scopePools[l], now, fields, tags); err != nil {
			a.logger.Printf("E! error evaluating expression for level %v: %s", alert.Level(l), err)
			continue
		} else if pass {
			return alert.Level(l), true
		}
	}
	return alert.OK, false
}

func (a *AlertNode) batchToResult(b models.Batch) influxql.Result {
	row := models.BatchToRow(b)
	r := influxql.Result{
		Series: imodels.Rows{row},
	}
	return r
}

func (a *AlertNode) event(
	id, name string,
	group models.GroupID,
	tags models.Tags,
	fields models.Fields,
	level alert.Level,
	t time.Time,
	d time.Duration,
	b models.Batch,
) (alert.Event, error) {
	msg, details, err := a.renderMessageAndDetails(id, name, t, group, tags, fields, level)
	if err != nil {
		return alert.Event{}, err
	}
	event := alert.Event{
		Topic: a.anonTopic,
		State: alert.EventState{
			ID:       id,
			Message:  msg,
			Details:  details,
			Time:     t,
			Duration: d,
			Level:    level,
		},
		Data: alert.EventData{
			Name:     name,
			TaskName: a.et.Task.ID,
			Group:    string(group),
			Tags:     tags,
			Fields:   fields,
			Result:   a.batchToResult(b),
		},
	}
	return event, nil
}

type alertState struct {
	history  []alert.Level
	idx      int
	flapping bool
	changed  bool
	// Time when first alert was triggered
	firstTriggered time.Time
	// Time when last alert was triggered.
	// Note: Alerts are not triggered for every event.
	lastTriggered time.Time
	expired       bool
}

// Return the duration of the current alert state.
func (a *alertState) duration() time.Duration {
	return a.lastTriggered.Sub(a.firstTriggered)
}

// Record that the alert was triggered at time t.
func (a *alertState) triggered(t time.Time) {
	a.lastTriggered = t
	// Check if we are being triggered for first time since an alert.OKAlert
	// If so reset firstTriggered time
	p := a.idx - 1
	if p == -1 {
		p = len(a.history) - 1
	}
	if a.history[p] == alert.OK {
		a.firstTriggered = t
	}
}

// Record an event in the alert history.
func (a *alertState) addEvent(level alert.Level) {
	a.changed = a.history[a.idx] != level
	a.idx = (a.idx + 1) % len(a.history)
	a.history[a.idx] = level
}

// Return current level of this state
func (a *alertState) currentLevel() alert.Level {
	return a.history[a.idx]
}

// Compute the percentage change in the alert history.
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

func (a *AlertNode) updateState(t time.Time, level alert.Level, group models.GroupID) *alertState {
	state, ok := a.states[group]
	if !ok {
		state = &alertState{
			history: make([]alert.Level, a.a.History),
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
	state.expired = !state.changed && a.a.StateChangesOnlyDuration != 0 && t.Sub(state.lastTriggered) >= a.a.StateChangesOnlyDuration
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
		TaskName: a.et.Task.ID,
		Group:    g,
		Tags:     tags,
	}
	id := a.bufPool.Get().(*bytes.Buffer)
	defer func() {
		id.Reset()
		a.bufPool.Put(id)
	}()

	err := a.idTmpl.Execute(id, info)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func (a *AlertNode) renderMessageAndDetails(id, name string, t time.Time, group models.GroupID, tags models.Tags, fields models.Fields, level alert.Level) (string, string, error) {
	g := string(group)
	if group == models.NilGroup {
		g = "nil"
	}
	minfo := messageInfo{
		idInfo: idInfo{
			Name:     name,
			TaskName: a.et.Task.ID,
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
	defer func() {
		tmpBuffer.Reset()
		a.bufPool.Put(tmpBuffer)
	}()
	tmpBuffer.Reset()

	err := a.messageTmpl.Execute(tmpBuffer, minfo)
	if err != nil {
		return "", "", err
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
		return "", "", err
	}

	details := tmpBuffer.String()
	return msg, details, nil
}
