package deadman

import (
	"time"

	"github.com/uber-go/zap"
)

type Service struct {
	c      Config
	logger zap.Logger
}

func NewService(c Config, l zap.Logger) *Service {
	return &Service{
		c:      c,
		logger: l,
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
		s.logger.Info("Deadman's switch is configured globally")
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}
