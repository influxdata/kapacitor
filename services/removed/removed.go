package removed

import (
	"fmt"
	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"strings"
)

var ServiceNames = map[string]struct{}{
	HipChatName: struct{}{},
}

const HipChatName = "hipchat"

var HipChatLCName = strings.ToLower(HipChatName)

var ErrHipChatRemoved = ErrRemoved(HipChatName)

func ErrRemoved(name string) error {
	return fmt.Errorf("Sorry but %s is no longer supported and has been removed", name)
}

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error, ctx ...keyvalue.T)
}

type Service struct {
	Name      string
	ObjectCFG any
	diag      Diagnostic
}

func NewService(name string, cfg any, d Diagnostic) *Service {
	return &Service{
		Name:      name,
		ObjectCFG: cfg,
		diag:      d,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Handle(event alert.Event) {
	switch strings.ToLower(s.Name) {
	case HipChatName:
		s.diag.Error("failed to send event because service is removed", ErrHipChatRemoved)
	default:
		panic(fmt.Sprintf("handler name %q is either not removed or not defined", s.Name))
	}
}

func (s *Service) Update(_ []interface{}) error {
	s.diag.Error(fmt.Sprintf("the %q service is removed", s.Name), ErrHipChatRemoved)
	return nil
}

type testOptions struct {
	Name string
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Name: s.Name,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	switch strings.ToLower(o.Name) {
	case HipChatName:
		return ErrHipChatRemoved
	default:
		panic(fmt.Sprintf("handler name %q is either not removed or not defined", s.Name))
	}
}

type Config struct {
	// Whether Removed integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	return nil
}
