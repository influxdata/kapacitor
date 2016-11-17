package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/alert"
	"github.com/pkg/errors"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
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
	c := s.config()
	return c.Global
}
func (s *Service) StateChangesOnly() bool {
	c := s.config()
	return c.StateChangesOnly
}

type testOptions struct {
	ChatId                string `json:"chat-id"`
	ParseMode             string `json:"parse-mode"`
	Message               string `json:"message"`
	DisableWebPagePreview bool   `json:"disable-web-page-preview"`
	DisableNotification   bool   `json:"disable-notification"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		ChatId:                c.ChatId,
		ParseMode:             c.ParseMode,
		Message:               "test telegram message",
		DisableWebPagePreview: c.DisableWebPagePreview,
		DisableNotification:   c.DisableNotification,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(
		nil,
		o.ChatId,
		o.ParseMode,
		o.Message,
		o.DisableWebPagePreview,
		o.DisableNotification,
	)
}

func (s *Service) Alert(ctxt context.Context, chatId, parseMode, message string, disableWebPagePreview, disableNotification bool) error {
	url, post, err := s.preparePost(chatId, parseMode, message, disableWebPagePreview, disableNotification)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, post)
	req.Header.Set("Content-Type", "application/json")
	if ctxt != nil {
		req = req.WithContext(ctxt)
	}
	resp, err := http.DefaultClient.Do(req)
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
			Description string `json:"description"`
			ErrorCode   int    `json:"error_code"`
			Ok          bool   `json:"ok"`
		}
		res := &response{}

		err = json.Unmarshal(body, res)

		if err != nil {
			return fmt.Errorf("failed to understand Telegram response (err: %s). code: %d content: %s", err.Error(), resp.StatusCode, string(body))
		}
		return fmt.Errorf("sendMessage error (%d) description: %s", res.ErrorCode, res.Description)

	}
	return nil
}

func (s *Service) preparePost(chatId, parseMode, message string, disableWebPagePreview, disableNotification bool) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	if chatId == "" {
		chatId = c.ChatId
	}

	if parseMode == "" {
		parseMode = c.ParseMode
	}

	if parseMode != "" && parseMode != "Markdown" && parseMode != "HTML" {
		return "", nil, fmt.Errorf("parseMode %s is not valid, please use 'Markdown' or 'HTML'", parseMode)
	}

	postData := make(map[string]interface{})
	postData["chat_id"] = chatId
	postData["text"] = message

	if parseMode != "" {
		postData["parse_mode"] = parseMode
	}

	if disableWebPagePreview || c.DisableWebPagePreview {
		postData["disable_web_page_preview"] = true
	}

	if disableNotification || c.DisableNotification {
		postData["disable_notification"] = true
	}

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	u, err := url.Parse(c.URL)
	if err != nil {
		return "", nil, errors.Wrap(err, "invalid URL")
	}
	u.Path = path.Join(u.Path+c.Token, "sendMessage")
	return u.String(), &post, nil
}

type HandlerConfig struct {

	// Telegram user/group ID to post messages to.
	// If empty uses the chati-d from the configuration.
	ChatId string

	// Parse node, defaults to Mardown
	// If empty uses the parse-mode from the configuration.
	ParseMode string

	// Web Page preview
	// If empty uses the disable-web-page-preview from the configuration.
	DisableWebPagePreview bool

	// Disables Notification
	// If empty uses the disable-notification from the configuration.
	DisableNotification bool
}

type handler struct {
	s *Service
	c HandlerConfig
}

func (s *Service) Handler(c HandlerConfig) alert.Handler {
	return &handler{
		s: s,
		c: c,
	}
}

func (h *handler) Name() string {
	return "Telegram"
}

func (h *handler) Handle(ctxt context.Context, event alert.Event) error {
	return h.s.Alert(
		ctxt,
		h.c.ChatId,
		h.c.ParseMode,
		event.State.Message,
		h.c.DisableWebPagePreview,
		h.c.DisableNotification,
	)
}
