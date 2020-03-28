package discord

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	text "text/template"
	"time"

	"sync"

	"github.com/influxdata/kapacitor/alert"
	khttp "github.com/influxdata/kapacitor/http"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/tlsconfig"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	TemplateError(err error, kv keyvalue.T)
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
		Transport: khttp.NewDefaultTransportWithTLS(tlsConfig),
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
		Transport: khttp.NewDefaultTransportWithTLS(tlsConfig),
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
		return &Workspace{}, errors.New("no discord configuration found")
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

// Discord rich embed info
type embed struct {
	Color       int    `json:"color"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Timestamp   string `json:"timestamp"`
}

type testOptions struct {
	Workspace  string      `json:"workspace"`
	Message    string      `json:"message"`
	Level      alert.Level `json:"level"`
	Username   string      `json:"username"`
	AvatarURL  string      `json:"avatar-url"`
	EmbedTitle string      `json:"embed-title"`
	Time       time.Time   `json:"time-val"`
}

func (s *Service) TestOptions() interface{} {
	c, _ := s.config("")
	c.Timestamp = true
	t, _ := time.Parse(time.RFC3339, "1970-01-01T00:00:01Z")
	return &testOptions{
		Workspace:  c.Workspace,
		Message:    "test discord message",
		Level:      alert.Info,
		AvatarURL:  "https://influxdata.github.io/branding/img/downloads/influxdata-logo--symbol--pool-alpha.png",
		Username:   "Kapacitor",
		EmbedTitle: "Kapacitor Alert",
		Time:       t,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(o.Workspace, o.Message, o.Username, o.AvatarURL, o.EmbedTitle, o.Time, o.Level)
}

// Alert sends a message to the specified room.
func (s *Service) Alert(workspace, message, username, avatarURL, embedTitle string, time time.Time, level alert.Level) error {
	url, post, err := s.preparePost(workspace, message, username, avatarURL, embedTitle, time, level)
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
	if resp.StatusCode != http.StatusNoContent {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Discord response. code: %d content: %s", resp.StatusCode, string(body))}
		b := bytes.NewReader(body)
		dec := json.NewDecoder(b)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

type HandlerConfig struct {
	// Discord workspace ID to use when posting to webhook
	// If empty uses the default config
	Workspace string `mapstructure:"workspace"`
	// Username of webhook
	// If empty uses the default config
	Username string `mapstructure:"username"`
	// URL of webhook's avatar
	// If empty uses the default config
	AvatarURL string `mapstructure:"avatar-url"`
	// Embed title
	// If empty uses the default config
	EmbedTitle string `mapstructure:"embed-title"`
}

func (s *Service) preparePost(workspace, message, username, avatarURL, embedTitle string, timeVal time.Time, level alert.Level) (string, io.Reader, error) {
	c, err := s.config(workspace)
	if err != nil {
		return "", nil, err
	}
	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	var color int
	switch level {
	case alert.Critical:
		color = 0xF95F53 // #F95F53
	case alert.Warning:
		color = 0xF48D38 // #F48D38
	default:
		color = 0x7A65F2 // #7A65F2
	}
	var timeStr string
	if c.Timestamp {
		timeStr = timeVal.Format(time.RFC3339)
	}
	a := embed{
		Description: message,
		Color:       color,
		Title:       embedTitle,
		Timestamp:   timeStr,
	}
	postData := make(map[string]interface{})
	if username == "" {
		username = c.Username
	}
	postData["username"] = username
	if avatarURL == "" {
		avatarURL = c.AvatarURL
	}
	postData["avatar_url"] = avatarURL
	postData["embeds"] = []embed{a}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic

	embedTitleTmpl *text.Template
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	ettmpl, err := text.New("embedTitle").Parse(c.EmbedTitle)
	if err != nil {
		return nil, err
	}
	return &handler{
		s:              s,
		c:              c,
		diag:           s.diag.WithContext(ctx...),
		embedTitleTmpl: ettmpl,
	}, nil
}

func (h *handler) Handle(event alert.Event) {
	td := event.TemplateData()
	var buf bytes.Buffer

	if err := h.embedTitleTmpl.Execute(&buf, td); err != nil {
		h.diag.TemplateError(err, keyvalue.KV("embedTitle", h.c.EmbedTitle))
		return
	}
	if err := h.s.Alert(
		h.c.Workspace,
		event.State.Message,
		h.c.Username,
		h.c.AvatarURL,
		buf.String(), // Parsed embedtitle template
		event.State.Time,
		event.State.Level,
	); err != nil {
		h.diag.Error("failed to send event to Discord", err)
	}
}
