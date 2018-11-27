package tick_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

func newLambda(value int) *ast.LambdaNode {
	return &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Left: &ast.ReferenceNode{
				Reference: "cpu",
			},
			Right: &ast.NumberNode{
				IsInt: true,
				Int64: int64(value),
				Base:  10,
			},
			Operator: ast.TokenGreater,
		},
	}
}

func TestAlert(t *testing.T) {
	pipe, _, from := StreamFrom()
	alert := from.Alert()
	alert.Topic = "topic"
	alert.Id = "id"
	alert.Message = "Message"
	alert.Details = "details"
	alert.Crit = newLambda(90)
	alert.Warn = newLambda(80)
	alert.Info = newLambda(70)
	alert.CritReset = newLambda(50)
	alert.WarnReset = newLambda(40)
	alert.InfoReset = newLambda(30)
	alert.Flapping(0.4, 0.7)
	alert.History = 10
	alert.LevelTag = "levelTag"
	alert.LevelField = "levelField"
	alert.MessageField = "messageField"
	alert.DurationField = "1000000"
	alert.IdTag = "idTag"
	alert.IdField = "idField"
	alert.All().NoRecoveries().StateChangesOnly(time.Hour)
	alert.Inhibitors = []pipeline.Inhibitor{{Category: "other", EqualTags: []string{"t1", "t2"}}}

	want := `stream
    |from()
    |alert()
        .topic('topic')
        .id('id')
        .message('Message')
        .details('details')
        .info(lambda: "cpu" > 70)
        .warn(lambda: "cpu" > 80)
        .crit(lambda: "cpu" > 90)
        .infoReset(lambda: "cpu" > 30)
        .warnReset(lambda: "cpu" > 40)
        .critReset(lambda: "cpu" > 50)
        .history(10)
        .levelTag('levelTag')
        .levelField('levelField')
        .messageField('messageField')
        .durationField('1000000')
        .idTag('idTag')
        .idField('idField')
        .all()
        .noRecoveries()
        .inhibit('other', 't1', 't2')
        .stateChangesOnly(1h)
        .flapping(0.4, 0.7)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertStateChanges(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().StateChangesOnly()

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .stateChangesOnly()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertHTTPPost(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Post("http://coinop.com", "http://polybius.gov")
	handler.Endpoint = "CIA"
	handler.Header("publisher", "Sinneslöschen")
	handler.CaptureResponseFlag = true
	handler.Timeout = 10 * time.Second

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .post('http://coinop.com')
        .endpoint('CIA')
        .captureResponse()
        .timeout(10s)
        .header('publisher', 'Sinneslöschen')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertHTTPPostMultipleHeaders(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Post("")
	handler.Endpoint = "CIA"
	handler.Header("publisher", "Sinneslöschen")
	handler.Header("howdy", "doody")
	handler.CaptureResponseFlag = true
	handler.Timeout = 10 * time.Second

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .post()
        .endpoint('CIA')
        .captureResponse()
        .timeout(10s)
        .header('howdy', 'doody')
        .header('publisher', 'Sinneslöschen')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertHTTPPostEmptyURL(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Post("")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .post()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertTCP(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Tcp("echo:7")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .tcp('echo:7')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertTCPJSON(t *testing.T) {
	pipe, _, from := StreamFrom()
	j := `
    {
        "typeOf": "alert",
        "stateChangesOnly": false,
        "useFlapping": false,
        "message": "",
        "details": "",
        "post": null,
        "tcp": [
            {
                "address": "echo:7"
            }
        ],
        "email": null,
        "exec": null,
        "log": null,
        "victorOps": null,
        "pagerDuty": null,
        "pagerDuty2": null,
        "pushover": null,
        "sensu": null,
        "slack": null,
        "telegram": null,
        "hipChat": null,
        "alerta": null,
        "opsGenie": null,
        "opsGenie2": null,
        "talk": null
    }`
	node := from.Alert()
	if err := json.Unmarshal([]byte(j), node); err != nil {
		t.Errorf("unable to unmarshal alert %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .history(21)
        .tcp('echo:7')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertEmail(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Email("gaben@valvesoftware.com", "zoidberg@freemail.web")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .email()
        .to('gaben@valvesoftware.com')
        .to('zoidberg@freemail.web')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertExec(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Exec("send", "-watch", "-verbose") // maybe I should rewrite mh in go?

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .exec('send', '-watch', '-verbose')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertLog(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Log("/var/log/messages")
	handler.Mode = 420

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .log('/var/log/messages')
        .mode(0644)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertVictorOps(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().VictorOps()
	handler.RoutingKey = "Seatec Astronomy"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .victorOps()
        .routingKey('Seatec Astronomy')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPagerDuty(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty()
	handler.ServiceKey = "Seatec Astronomy"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pagerDuty()
        .serviceKey('Seatec Astronomy')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPagerDuty2(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty2()
	handler.RoutingKey = "LeafsNation"
	handler.Link("https://example.com/chart", "some chart")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pagerDuty2()
        .routingKey('LeafsNation')
        .link('https://example.com/chart', 'some chart')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPagerDuty2MissingLinkText(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty2()
	handler.RoutingKey = "LeafsNation"
	handler.Link("https://example.com/chart")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pagerDuty2()
        .routingKey('LeafsNation')
        .link('https://example.com/chart')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPagerDuty2MultipleLinks(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty2()
	handler.RoutingKey = "LeafsNation"
	handler.Link("https://example.com/chart", "some chart")
	handler.Link("https://example.com/details", "details")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pagerDuty2()
        .routingKey('LeafsNation')
        .link('https://example.com/chart', 'some chart')
        .link('https://example.com/details', 'details')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPagerDuty2_ServiceKey(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty2()
	handler.ServiceKey("LeafsNation")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pagerDuty2()
        .routingKey('LeafsNation')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertPushover(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Pushover()
	handler.UserKey = "mother"
	handler.Device = "LTX-71"
	handler.Title = "Faked Apollo Moon Landings"
	handler.URL = "http://playtronics.com"
	handler.URLTitle = "Cosmo's Office"
	handler.Sound = "click"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .pushover()
        .userKey('mother')
        .device('LTX-71')
        .title('Faked Apollo Moon Landings')
        .uRL('http://playtronics.com')
        .uRLTitle('Cosmo\'s Office')
        .sound('click')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertSensu(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Sensu()
	handler.Source = "Henry Hill"
	handler.Handlers("FBI", "Witness", "Protection")
	handler.MetadataMap = map[string]interface{}{
		"k1": "v1",
		"k2": int64(5),
		"k3": float64(5),
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .sensu()
        .source('Henry Hill')
        .handlers('FBI', 'Witness', 'Protection')
        .metadata('k1', 'v1')
        .metadata('k2', 5)
        .metadata('k3', 5.0)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertSlack(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Slack()
	handler.Workspace = "openchannel"
	handler.Channel = "#application"
	handler.Username = "prbot"
	handler.IconEmoji = ":non-potable_water:"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .slack()
        .workspace('openchannel')
        .channel('#application')
        .username('prbot')
        .iconEmoji(':non-potable_water:')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertTelegram(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Telegram()
	handler.ChatId = "samuel morris"
	handler.ParseMode = "analog"
	handler.DisableWebPagePreview().DisableNotification()

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .telegram()
        .chatId('samuel morris')
        .parseMode('analog')
        .disableWebPagePreview()
        .disableNotification()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertHipchat(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().HipChat()
	handler.Room = "escape room"
	handler.Token = "waxed mustache and plaid shirt"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .hipChat()
        .room('escape room')
        .token('waxed mustache and plaid shirt')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertKafka(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Kafka()
	handler.Cluster = "default"
	handler.KafkaTopic = "test"
	handler.Template = "tmpl"

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .kafka()
        .cluster('default')
        .kafkaTopic('test')
        .template('tmpl')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertAlerta(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Alerta()
	handler.Token = "ASSUMING DIRECT CONTROL"
	handler.Resource = "Harbinger"
	handler.Event = "Jump through Omega-4 Relay"
	handler.Environment = "Collector base"
	handler.Group = "I brought Jack, Miranda and Tali"
	handler.Value = "Save the Galaxy"
	handler.Origin = "Omega"
	handler.Services("legion", "vent", "garrus", "distraction team", "grunt", "crew", "samara", "barrier")
	handler.Timeout = 10 * time.Second

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .alerta()
        .token('ASSUMING DIRECT CONTROL')
        .resource('Harbinger')
        .event('Jump through Omega-4 Relay')
        .environment('Collector base')
        .group('I brought Jack, Miranda and Tali')
        .value('Save the Galaxy')
        .origin('Omega')
        .services('legion', 'vent', 'garrus', 'distraction team', 'grunt', 'crew', 'samara', 'barrier')
        .timeout(10s)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertOpsGenie(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().OpsGenie()
	handler.Teams("radiant", "dire")
	handler.Recipients("huskar", "dazzle", "nature's prophet", "faceless void", "bounty hunter")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .opsGenie()
        .teams('radiant', 'dire')
        .recipients('huskar', 'dazzle', 'nature\'s prophet', 'faceless void', 'bounty hunter')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertOpsGenie2(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().OpsGenie2()
	handler.Teams("radiant", "dire")
	handler.Recipients("huskar", "dazzle", "nature's prophet", "faceless void", "bounty hunter")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .opsGenie2()
        .teams('radiant', 'dire')
        .recipients('huskar', 'dazzle', 'nature\'s prophet', 'faceless void', 'bounty hunter')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertTalk(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Talk()

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .talk()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertMQTT(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Mqtt("mattel")
	handler.BrokerName = "toy 'r us"
	handler.Qos = 1
	handler.Retained = true

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .mqtt('mattel')
        .brokerName('toy \'r us')
        .qos(1)
        .retained(TRUE)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertMQTTNotRetained(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Mqtt("mattel")
	handler.BrokerName = "toy 'r us"
	handler.Qos = 1
	handler.Retained = false

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .mqtt('mattel')
        .brokerName('toy \'r us')
        .qos(1)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestAlertSNMP(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().SnmpTrap("trap")
	handler.Data("Petrov's Defence", "t", "Marshall Trap")
	handler.Data("Queen's Gambit Declined", "t", "Rubinstein Trap")

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .snmpTrap('trap')
        .data('Petrov\'s Defence', 't', 'Marshall Trap')
        .data('Queen\'s Gambit Declined', 't', 'Rubinstein Trap')
`
	PipelineTickTestHelper(t, pipe, want)
}
