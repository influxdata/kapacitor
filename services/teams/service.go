package teams

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync/atomic"

	"math"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
)

const summaryCutoff = 70

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	diag        Diagnostic
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
	}
	return nil
}

func (s *Service) Global() bool {
	return s.config().Global
}

func (s *Service) StateChangesOnly() bool {
	return s.config().StateChangesOnly
}

type testOptions struct {
	ChannelURL string      `json:"channel_url"`
	AlertTopic string      `json:"alert_topic"`
	AlertID    string      `json:"alert_id"`
	Message    string      `json:"message"`
	Level      alert.Level `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		ChannelURL: s.config().ChannelURL,
		AlertTopic: "test kapacitor alert topic",
		AlertID:    "foo/bar/bat",
		Message:    "test teams message",
		Level:      alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	c := s.config()
	return s.Alert(c.ChannelURL, o.AlertTopic, o.AlertID, o.Message, o.Level)
}

func (s *Service) Alert(channelURL, alertTopic, alertID, message string, level alert.Level) error {
	url, post, err := s.preparePost(channelURL, alertTopic, alertID, message, level)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Teams response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

// Card is a Microsoft MessageCard structure.
// See https://docs.microsoft.com/en-us/outlook/actionable-messages/message-card-reference#card-fields.
type Card struct {
	CardType   string `json:"@type"`
	Context    string `json:"@context"`
	Title      string `json:"title"`
	Text       string `json:"text"`
	Summary    string `json:"summary"`
	ThemeColor string `json:"themeColor"`
}

func (s *Service) preparePost(channelURL, alertTopic, alertID, message string, level alert.Level) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}

	if channelURL == "" {
		channelURL = c.ChannelURL
	}

	u, err := url.Parse(channelURL)
	if err != nil {
		return "", nil, err
	}

	var title, summary string
	switch {
	case alertID == "" && alertTopic == "":
		title = level.String()
		summary = title + ": " + message
	case alertID == "":
		title = level.String() + ": [" + alertTopic + "]"
		summary = title + " - " + message
	default:
		title = level.String() + ": [" + alertID + "]"
		summary = title + " - " + message
	}

	var color string
	switch level {
	case alert.Warning:
		color = "FFA533"
	case alert.Critical:
		color = "CC4A31"
	default:
		color = "34CC25"
	}

	card := &Card{
		CardType:   "MessageCard",
		Context:    "http://schema.org/extensions",
		Title:      title,
		Text:       message,
		Summary:    summary[:int(math.Min(float64(summaryCutoff), float64(len(summary))))] + "...",
		ThemeColor: color,
	}

	postBytes, err := json.Marshal(card)
	if err != nil {
		return "", nil, errors.Wrap(err, "error marshaling card struct")
	}

	post := bytes.NewBuffer(postBytes)
	return u.String(), post, nil
}

type HandlerConfig struct {
	// Teams channel webhook URL used to post messages.
	// If empty uses the channel URL from the configuration.
	ChannelURL string `mapstructure:"channel-url"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(
		h.c.ChannelURL,
		event.Topic,
		event.State.ID,
		event.State.Message,
		event.State.Level,
	); err != nil {
		h.diag.Error("failed to send event to Teams", err)
	}
}
