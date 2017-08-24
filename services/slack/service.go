package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	InsecureSkipVerify()

	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	clientValue atomic.Value
	diag        Diagnostic
	client      *http.Client
}

func NewService(c Config, d Diagnostic) (*Service, error) {
	tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}
	if tlsConfig.InsecureSkipVerify {
		d.InsecureSkipVerify()
	}
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	s.clientValue.Store(&http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
	})
	return s, nil
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
		tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return err
		}
		if tlsConfig.InsecureSkipVerify {
			s.diag.InsecureSkipVerify()
		}
		s.configValue.Store(c)
		s.clientValue.Store(&http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: tlsConfig,
			},
		})
	}
	return nil
}

func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

func (s *Service) StateChangesOnly() bool {
	c := s.config()
	return c.StateChangesOnly
}

// slack attachment info
type attachment struct {
	Fallback  string   `json:"fallback"`
	Color     string   `json:"color"`
	Text      string   `json:"text"`
	Mrkdwn_in []string `json:"mrkdwn_in"`
}

type testOptions struct {
	Channel   string      `json:"channel"`
	Message   string      `json:"message"`
	Level     alert.Level `json:"level"`
	Username  string      `json:"username"`
	IconEmoji string      `json:"icon-emoji"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Channel: c.Channel,
		Message: "test slack message",
		Level:   alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(o.Channel, o.Message, o.Username, o.IconEmoji, o.Level)
}

func (s *Service) Alert(channel, message, username, iconEmoji string, level alert.Level) error {
	url, post, err := s.preparePost(channel, message, username, iconEmoji, level)
	if err != nil {
		return err
	}
	client := s.clientValue.Load().(*http.Client)
	resp, err := client.Post(url, "application/json", post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Slack response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(channel, message, username, iconEmoji string, level alert.Level) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	if channel == "" {
		channel = c.Channel
	}
	var color string
	switch level {
	case alert.Warning:
		color = "warning"
	case alert.Critical:
		color = "danger"
	default:
		color = "good"
	}
	a := attachment{
		Fallback:  message,
		Text:      message,
		Color:     color,
		Mrkdwn_in: []string{"text"},
	}
	postData := make(map[string]interface{})
	postData["as_user"] = false
	postData["channel"] = channel
	postData["text"] = ""
	postData["attachments"] = []attachment{a}

	if username == "" {
		username = c.Username
	}
	postData["username"] = username

	if iconEmoji == "" {
		iconEmoji = c.IconEmoji
	}
	postData["icon_emoji"] = iconEmoji

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

type HandlerConfig struct {
	// Slack channel in which to post messages.
	// If empty uses the channel from the configuration.
	Channel string `mapstructure:"channel"`

	// Username of the Slack bot.
	// If empty uses the username from the configuration.
	Username string `mapstructure:"username"`

	// IconEmoji is an emoji name surrounded in ':' characters.
	// The emoji image will replace the normal user icon for the slack bot.
	IconEmoji string `mapstructure:"icon-emoji"`
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
		h.c.Channel,
		event.State.Message,
		h.c.Username,
		h.c.IconEmoji,
		event.State.Level,
	); err != nil {
		h.diag.Error("failed to send event", err)
	}
}
