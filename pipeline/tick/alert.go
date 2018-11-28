package tick

import (
	"sort"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

// AlertNode converts the Alert pipeline node into the TICKScript AST
type AlertNode struct {
	Function
}

// NewAlert creates an Alert function builder
func NewAlert(parents []ast.Node) *AlertNode {
	return &AlertNode{
		Function{
			Parents: parents,
		},
	}
}

// Build creates a Alert ast.Node
func (n *AlertNode) Build(a *pipeline.AlertNode) (ast.Node, error) {
	n.Pipe("alert").
		Dot("topic", a.Topic).
		Dot("id", a.Id).
		Dot("message", a.Message).
		Dot("details", a.Details).
		Dot("info", a.Info).
		Dot("warn", a.Warn).
		Dot("crit", a.Crit).
		Dot("infoReset", a.InfoReset).
		Dot("warnReset", a.WarnReset).
		Dot("critReset", a.CritReset).
		Dot("history", a.History).
		Dot("levelTag", a.LevelTag).
		Dot("levelField", a.LevelField).
		Dot("messageField", a.MessageField).
		Dot("durationField", a.DurationField).
		Dot("idTag", a.IdTag).
		Dot("idField", a.IdField).
		DotIf("all", a.AllFlag).
		DotIf("noRecoveries", a.NoRecoveriesFlag)

	for _, in := range a.Inhibitors {
		args := make([]interface{}, len(in.EqualTags)+1)
		args[0] = in.Category
		for i, t := range in.EqualTags {
			args[i+1] = t
		}
		n.Dot("inhibit", args...)
	}

	if a.IsStateChangesOnly {
		if a.StateChangesOnlyDuration == 0 {
			n.Dot("stateChangesOnly")
		} else {
			n.Dot("stateChangesOnly", a.StateChangesOnlyDuration)
		}
	}

	if a.UseFlapping {
		n.DotZeroValueOK("flapping", a.FlapLow, a.FlapHigh)
	}

	for _, h := range a.HTTPPostHandlers {
		n.DotRemoveZeroValue("post", h.URL).
			Dot("endpoint", h.Endpoint).
			DotIf("captureResponse", h.CaptureResponseFlag).
			Dot("timeout", h.Timeout)

		var headers []string
		for k := range h.Headers {
			headers = append(headers, k)
		}
		sort.Strings(headers)
		for _, k := range headers {
			n.Dot("header", k, h.Headers[k])
		}
	}

	for _, h := range a.TcpHandlers {
		n.DotRemoveZeroValue("tcp", h.Address)
	}

	for _, h := range a.EmailHandlers {
		n.Dot("email")
		for _, to := range h.ToList {
			n.Dot("to", to)
		}
	}

	for _, h := range a.ExecHandlers {
		n.DotRemoveZeroValue("exec", args(h.Command)...)
	}

	for _, h := range a.LogHandlers {
		n.DotRemoveZeroValue("log", h.FilePath)
		if h.Mode != 0 {
			mode := &ast.NumberNode{
				IsInt: true,
				Int64: h.Mode,
				Base:  8,
			}
			n.Dot("mode", mode)
		}
	}

	for _, h := range a.VictorOpsHandlers {
		n.Dot("victorOps").
			Dot("routingKey", h.RoutingKey)
	}

	for _, h := range a.PagerDutyHandlers {
		n.Dot("pagerDuty").
			Dot("serviceKey", h.ServiceKey)
	}

	for _, h := range a.PagerDuty2Handlers {
		n.Dot("pagerDuty2").
			Dot("routingKey", h.RoutingKey)
		for _, l := range h.Links {
			if len(l.Text) > 0 {
				n.Dot("link", l.Href, l.Text)
			} else {
				n.Dot("link", l.Href)
			}
		}
	}

	for _, h := range a.PushoverHandlers {
		n.Dot("pushover").
			Dot("userKey", h.UserKey).
			Dot("device", h.Device).
			Dot("title", h.Title).
			Dot("uRL", h.URL).
			Dot("uRLTitle", h.URLTitle).
			Dot("sound", h.Sound)
	}

	for _, h := range a.SensuHandlers {
		n.Dot("sensu").
			Dot("source", h.Source).
			Dot("handlers", args(h.HandlersList)...)

		// Use stable key order
		keys := make([]string, 0, len(h.MetadataMap))
		for k := range h.MetadataMap {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			n.Dot("metadata", k, h.MetadataMap[k])
		}
	}

	for _, h := range a.SlackHandlers {
		n.Dot("slack").
			Dot("workspace", h.Workspace).
			Dot("channel", h.Channel).
			Dot("username", h.Username).
			Dot("iconEmoji", h.IconEmoji)
	}

	for _, h := range a.TelegramHandlers {
		n.Dot("telegram").
			Dot("chatId", h.ChatId).
			Dot("parseMode", h.ParseMode).
			DotIf("disableWebPagePreview", h.IsDisableWebPagePreview).
			DotIf("disableNotification", h.IsDisableNotification)
	}

	for _, h := range a.HipChatHandlers {
		n.Dot("hipChat").
			Dot("room", h.Room).
			Dot("token", h.Token)
	}

	for _, h := range a.KafkaHandlers {
		n.Dot("kafka").
			Dot("cluster", h.Cluster).
			Dot("kafkaTopic", h.KafkaTopic).
			Dot("template", h.Template)
	}

	for _, h := range a.AlertaHandlers {
		n.Dot("alerta").
			Dot("token", h.Token).
			Dot("resource", h.Resource).
			Dot("event", h.Event).
			Dot("environment", h.Environment).
			Dot("group", h.Group).
			Dot("value", h.Value).
			Dot("origin", h.Origin).
			Dot("services", args(h.Service)...).
			Dot("timeout", h.Timeout)
	}

	for _, h := range a.OpsGenieHandlers {
		n.Dot("opsGenie").
			Dot("teams", args(h.TeamsList)...).
			Dot("recipients", args(h.RecipientsList)...)
	}
	for _, h := range a.OpsGenie2Handlers {
		n.Dot("opsGenie2").
			Dot("teams", args(h.TeamsList)...).
			Dot("recipients", args(h.RecipientsList)...)
	}

	for _ = range a.TalkHandlers {
		n.Dot("talk")
	}

	for _, h := range a.MQTTHandlers {
		n.DotRemoveZeroValue("mqtt", h.Topic).
			Dot("brokerName", h.BrokerName).
			Dot("qos", h.Qos).
			Dot("retained", h.Retained)
	}

	for _, h := range a.SNMPTrapHandlers {
		n.DotRemoveZeroValue("snmpTrap", h.TrapOid)
		for _, d := range h.DataList {
			n.Dot("data", d.Oid, d.Type, d.Value)
		}
	}

	return n.prev, n.err
}
