package deadman

import (
	"time"
)

type Diagnostic interface {
	ConfiguredGlobally()
}

type Service struct {
	c    Config
	diag Diagnostic
}

func NewService(c Config, d Diagnostic) *Service {
	return &Service{
		c:    c,
		diag: d,
	}
}

func (s *Service) Interval() time.Duration {
	return time.Duration(s.c.Interval)
}

func (s *Service) Threshold() float64 {
	return s.c.Threshold
}

func (s *Service) Id() string {
	return s.c.Id
}

func (s *Service) Message() string {
	return s.c.Message
}

func (s *Service) Global() bool {
	return s.c.Global
}

func (s *Service) Open() error {
	if s.Global() {
		s.diag.ConfiguredGlobally()
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}
