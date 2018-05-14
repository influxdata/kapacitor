package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"sync"

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

type Workspace struct {
	mu     sync.RWMutex
	config Config
	client *http.Client
}

func NewWorkspace(c Config) (*Workspace, error) {
	tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return nil, err
	}

	cl := &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
	}

	return &Workspace{
		config: c,
		client: cl,
	}, nil
}

func (w *Workspace) Config() Config {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.config
}

func (w *Workspace) Client() *http.Client {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.client
}

func (w *Workspace) Update(c Config) error {
	tlsConfig, err := tlsconfig.Create(c.SSLCA, c.SSLCert, c.SSLKey, c.InsecureSkipVerify)
	if err != nil {
		return err
	}

	cl := &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: tlsConfig,
		},
	}

	w.client = cl
	w.config = c

	return nil
}

type Service struct {
	mu         sync.RWMutex
	workspaces map[string]*Workspace
	diag       Diagnostic
}

func NewService(confs []Config, d Diagnostic) (*Service, error) {
	s := &Service{
		diag:       d,
		workspaces: make(map[string]*Workspace),
	}

	if len(confs) == 1 {
		confs[0].Default = true
	}
	for _, c := range confs {

		if c.InsecureSkipVerify {
			s.diag.InsecureSkipVerify()
		}

		w, err := NewWorkspace(c)
		if err != nil {
			return nil, err
		}
		s.workspaces[c.Workspace] = w

		// We'll stash the default workspace with the empty string as a key.
		// Either there's a single config with no workspace name, or else
		// we have multiple configs that all have names.
		if c.Default && c.Workspace != "" {
			s.workspaces[""] = s.workspaces[c.Workspace]
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

func (s *Service) workspace(wid string) (*Workspace, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.workspaces) == 0 {
		return &Workspace{}, errors.New("no slack configuration found")
	}
	v, ok := s.workspaces[wid]
	if !ok {
		return &Workspace{}, errors.New("workspace id not found")
	}
	return v, nil
}

func (s *Service) config(wid string) (Config, error) {
	w, err := s.workspace(wid)
	if err != nil {
		return Config{}, err
	}

	return w.Config(), nil
}

func (s *Service) defaultConfig() Config {
	conf, _ := s.config("")
	return conf
}

func (s *Service) client(wid string) (*http.Client, error) {
	w, err := s.workspace(wid)
	if err != nil {
		return &http.Client{}, err
	}
	return w.Client(), nil
}

func (s *Service) Update(newConfigs []interface{}) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, v := range newConfigs {
		if conf, ok := v.(Config); ok {
			if conf.InsecureSkipVerify {
				s.diag.InsecureSkipVerify()
			}
			_, ok := s.workspaces[conf.Workspace]
			if !ok {
				w, err := NewWorkspace(conf)
				s.workspaces[conf.Workspace] = w
				if err != nil {
					return err
				}
			} else {
				s.workspaces[conf.Workspace].Update(conf)
			}

			// We'll stash the default workspace with the empty string as a key.
			// Either there's a single config with no workspace name, or else
			// we have multiple configs that all have names.
			if conf.Default && conf.Workspace != "" {
				s.workspaces[""] = s.workspaces[conf.Workspace]
			}
		} else {
			return fmt.Errorf("expected config object to be of type %T, got %T", v, conf)
		}
	}
	return nil
}

func (s *Service) Global() bool {
	return s.defaultConfig().Global
}

func (s *Service) StateChangesOnly() bool {
	return s.defaultConfig().Global
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
	return s.Alert(o.Workspace, o.Channel, o.Message, o.Username, o.IconEmoji, o.Level)
}

func (s *Service) Alert(workspace, channel, message, username, iconEmoji string, level alert.Level) error {
	url, post, err := s.preparePost(workspace, channel, message, username, iconEmoji, level)
	if err != nil {
		return err
	}

	client, err := s.client(workspace)
	if err != nil {
		return err
	}

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
