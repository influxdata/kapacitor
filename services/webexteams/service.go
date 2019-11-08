package webexteams

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
)

// Diagnostic is an interface
type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

// Service is a struct
type Service struct {
	configValue atomic.Value
	diag        Diagnostic
}

// NewService returns a service
func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	return s
}

// Open peform any init
func (s *Service) Open() error {
	return nil
}

// Close preform any actions prior to close
func (s *Service) Close() error {
	return nil
}

// Update performs any actions required to update the config
func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	c, ok := newConfig[0].(Config)
	if !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	}
	s.configValue.Store(c)
	return nil
}

// config loads the config struct stored in the configValue field.
func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

// Alert sends a message to ONE of the roomID, personID, or personEmail.
func (s *Service) Alert(roomID, personID, personEmail, token, text string, markdown bool) error {
	c := s.config()
	if !c.Enabled {
		return errors.New(ServiceUnavailableErr)
	}
	// debugmessage := fmt.Sprintf(`
	// c.Enabled				= %v
	// c.RoomID				= %v
	// c.ToPersonEmail			= %v
	// c.ToPersonID			= %v
	// c.Token					= %v
	// c.URL					= %v
	// roomID					= %v
	// personID				= %v
	// personEmail				= %v
	// markdown				= %v
	// token					= %v
	// text					= %v
	// `, c.Enabled,
	// 	c.RoomID,
	// 	c.ToPersonEmail,
	// 	c.ToPersonID,
	// 	c.Token,
	// 	c.URL,
	// 	roomID,
	// 	personID,
	// 	personEmail,
	// 	markdown,
	// 	token,
	// 	text,
	// )
	postData := map[string]interface{}{}
	// decide the message destination

	// check for incoming values FIRST
	// if none of these conditions pass
	// fall back to the kapacitor configuration
	if personID != "" {
		postData["toPersonId"] = personID
	} else if personEmail != "" {
		postData["toPersonEmail"] = personEmail
	} else if roomID != "" {
		postData["roomId"] = roomID
	} else if c.ToPersonID != "" {
		postData["toPersonId"] = c.ToPersonID
	} else if c.ToPersonEmail != "" {
		postData["toPersonEmail"] = c.ToPersonEmail
	} else if c.RoomID != "" {
		postData["roomId"] = c.RoomID
	}
	if markdown == true {
		postData["markdown"] = true
	}
	authToken := ""
	if token != "" {
		authToken = token
	} else if c.Token != "" {
		authToken = c.Token
	}
	postData["text"] = text
	data, err := json.Marshal(&postData)
	if err != nil {
		return err
	}
	r, err := http.NewRequest(http.MethodPost, c.URL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	r.Header.Add("Authorization", "Bearer "+authToken)
	r.Header.Add("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code %d from Webex Teams service", response.StatusCode)
	}
	return nil
}

// HandlerConfig is the Webex Teams Configuration for the Handler
type HandlerConfig struct {
	RoomID        string `mapstructure:"room-id"`
	ToPersonID    string `mapstructure:"to-person-id"`
	ToPersonEmail string `mapstructure:"to-person-email"`
	Token         string `mapstructure:"token"`
	Markdown      bool   `mapstructure:"markdown"`
}

// handler provides the implementation of the alert.Handler interface for the Foo service.
type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

// DefaultHandlerConfig returns a HandlerConfig struct with defaults applied.
func (s *Service) DefaultHandlerConfig() HandlerConfig {
	// return a handler config populated with the default room from the service config.
	c := s.config()
	return HandlerConfig{
		RoomID: c.RoomID,
	}
}

// Handler creates a handler from the config.
func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}
}

// StateChangesOnly configures whether or not alerts should be generated only when state changs
func (s *Service) StateChangesOnly() bool {
	fmt.Println("state changes only", s.config().StateChangesOnly)
	return s.config().StateChangesOnly
}

// StateChangesOnly configures whether or not alerts should be generated only when state changs
func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

// Handle takes an event and posts its message to the Foo service chat room.
func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.RoomID, h.c.ToPersonID, h.c.ToPersonEmail, h.c.Token, event.State.Message, h.c.Markdown); err != nil {
		h.diag.Error("E! failed to handle event", err)
	}
}

type testOptions struct {
	RoomID  string `json:"roomId"`
	Message string `json:"message"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		RoomID:  c.RoomID,
		Message: "test webex message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.RoomID, "", "", "", options.Message, false)
}
