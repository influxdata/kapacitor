package tick_test

import (
	"testing"
	"time"

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

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
        .idField('idField')
        .idField('idField')
        .stateChangesOnly(1h)
        .flapping(0.4, 0.7)
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertStateChanges(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().StateChangesOnly()

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .stateChangesOnly()
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertHTTPPost(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Post("http://coinop.com", "http://polybius.gov")
	handler.Endpoint = "CIA"
	handler.Header("publisher", "Sinneslöschen")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .post('http://coinop.com')
        .endpoint('CIA')
        .header('publisher', 'Sinneslöschen')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
func TestAlertTCP(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Tcp("echo:7")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .tcp()
        .address('echo:7')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertEmail(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Email("gaben@valvesoftware.com", "zoidberg@freemail.web")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertExec(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Exec("send", "-watch", "-verbose") // maybe I should rewrite mh in go?
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .exec('send', '-watch', '-verbose')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertLog(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Log("/var/log/messages")
	handler.Mode = 420
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertVictorOps(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().VictorOps()
	handler.RoutingKey = "Seatec Astronomy"
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertPagerDuty(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().PagerDuty()
	handler.ServiceKey = "Seatec Astronomy"
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
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
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
        .url('http://playtronics.com')
        .urlTitle('Cosmo\'s Office')
        .sound('click')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertSensu(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Sensu()
	handler.Source = "Henry Hill"
	handler.Handlers("FBI", "Witness", "Protection")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
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
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertSlack(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Slack()
	handler.Channel = "#application"
	handler.Username = "prbot"
	handler.IconEmoji = ":non-potable_water:"
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .slack()
        .channel('#application')
        .username('prbot')
        .iconEmoji(':non-potable_water:')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertTelegram(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Telegram()
	handler.ChatId = "samuel morris"
	handler.ParseMode = "analog"
	handler.DisableWebPagePreview().DisableNotification()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertHipchat(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().HipChat()
	handler.Room = "escape room"
	handler.Token = "waxed mustache and plaid shirt"
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
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
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertOpsGenie(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().OpsGenie()
	handler.Teams("radiant", "dire")
	handler.Recipients("huskar", "dazzle", "nature's prophet", "faceless void", "bounty hunter")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

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
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertTalk(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Alert().Talk()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .talk()
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertMQTT(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().Mqtt("mattel")
	handler.BrokerName = "toy 'r us"
	handler.Qos = 1
	handler.Retained = true
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .mqtt()
        .brokerName('toy \'r us')
        .topic('mattel')
        .qos(1)
        .retained()
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}

func TestAlertSNMP(t *testing.T) {
	pipe, _, from := StreamFrom()
	handler := from.Alert().SnmpTrap("trap")
	handler.Data("Petrov's Defence", "opening trap", "Marshall Trap")
	handler.Data("Queen's Gambit Declined", "opening trap", "Rubinstein Trap")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |alert()
        .id('{{ .Name }}:{{ .Group }}')
        .message('{{ .ID }} is {{ .Level }}')
        .details('{{ json . }}')
        .history(21)
        .snmpTrap('trap')
        .data('Petrov\'s Defence', 'opening trap', 'Marshall Trap')
        .data('Queen\'s Gambit Declined', 'opening trap', 'Rubinstein Trap')
`
	if got != want {
		t.Errorf("TestAlert = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
