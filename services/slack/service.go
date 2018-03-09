package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
	"sync"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic

	InsecureSkipVerify()

	Error(msg string, err error)
}

type Service struct {
	mu                  sync.Mutex
	defaultConfig       *Config
	configs             map[string]*Config
	clients             map[string]*http.Client
	hasGlobal           bool
	hasStateChangesOnly bool
	diag                Diagnostic
}

func NewService(confs []Config, d Diagnostic) (*Service, error) {
	s := &Service{
		diag:    d,
		configs: make(map[string]*Config),
		clients: make(map[string]*http.Client),
	}

	for _, c := range confs {
		s.configs[c.Workspace] = &c
		if c.Default || c.Workspace == "" {
			// if c.Workspace == "" then there's only one config.  take it as default regardless
			// of the value given to Default
			s.defaultConfig = &c
		}

		s.hasGlobal = s.hasGlobal || c.Global
		s.hasStateChangesOnly = s.hasStateChangesOnly || c.StateChangesOnly

		tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
		if err != nil {
			return nil, err
		}
		if tlsConfig.InsecureSkipVerify {
			d.InsecureSkipVerify()
		}

		s.clients[c.Workspace] = &http.Client{
			Transport: &http.Transport{
				Proxy:           http.ProxyFromEnvironment,
				TLSClientConfig: tlsConfig,
			},
		}

	}

	return s, nil
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config(wid string) (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if wid == "" {
		return *s.defaultConfig, nil
	}
	v, ok := s.configs[wid]
	if !ok {
		return Config{}, errors.New("workspace id not found")
	}
	return *v, nil
}

func (s *Service) Update(newConfigs []interface{}) error {

	if c, ok := newConfigs[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfigs[0])
	} else {
		s.mu.Lock()
		defer s.mu.Unlock()

		for _, v := range newConfigs {
			c = v.(Config)
			s.configs[c.Workspace] = &c
			if c.Default || c.Workspace == "" {
				// if c.Workspace == "" then there's only one config.  take it as default regardless
				// of the value given to Default
				s.defaultConfig = &c
			}
			tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
			if err != nil {
				return err
			}
			if tlsConfig.InsecureSkipVerify {
				s.diag.InsecureSkipVerify()
			}

			s.clients[c.Workspace] = &http.Client{
				Transport: &http.Transport{
					Proxy:           http.ProxyFromEnvironment,
					TLSClientConfig: tlsConfig,
				},
			}

		}
	}
	return nil
}

func (s *Service) Global() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasGlobal
}

func (s *Service) StateChangesOnly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasStateChangesOnly
}

// slack attachment info
type attachment struct {
	Fallback  string   `json:"fallback"`
	Color     string   `json:"color"`
	Text      string   `json:"text"`
	Mrkdwn_in []string `json:"mrkdwn_in"`
}

type testOptions struct {
	Workspace string      `json:"workspace"`
	Channel   string      `json:"channel"`
	Message   string      `json:"message"`
	Level     alert.Level `json:"level"`
	Username  string      `json:"username"`
	IconEmoji string      `json:"icon-emoji"`
}

func (s *Service) TestOptions() interface{} {
	c, _ := s.config("")
	return &testOptions{
		Workspace: c.Workspace,
		Channel:   c.Channel,
		Message:   "test slack message",
		Level:     alert.Critical,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert("", o.Channel, o.Message, o.Username, o.IconEmoji, o.Level)
}

func (s *Service) Alert(workspace, channel, message, username, iconEmoji string, level alert.Level) error {
	url, post, err := s.preparePost(workspace, channel, message, username, iconEmoji, level)
	if err != nil {
		return err
	}
	client := s.clients[workspace]
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

func (s *Service) preparePost(workspace, channel, message, username, iconEmoji string, level alert.Level) (string, io.Reader, error) {
	c, err := s.config(workspace)
	if err != nil {
		return "", nil, err
	}
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
	err = enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

type HandlerConfig struct {
	// Slack workspace ID to use when posting messages
	// If empty uses the default config
	Workspace string `mapstructure:"workspace"`

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
		h.c.Workspace,
		h.c.Channel,
		event.State.Message,
		h.c.Username,
		h.c.IconEmoji,
		event.State.Level,
	); err != nil {
		h.diag.Error("failed to send event", err)
	}
}
