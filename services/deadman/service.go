package deadman

import (
	"time"

	"github.com/influxdata/kapacitor/services/diagnostic"
)

type Service struct {
	c          Config
	diagnostic diagnostic.Diagnostic
}

func NewService(c Config, d diagnostic.Diagnostic) *Service {
	return &Service{
		c:          c,
		diagnostic: d,
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
		s.diagnostic.Diag(
			"level", "info",
			"msg", "Deadman's switch is configured globally",
		)
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}
