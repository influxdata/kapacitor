package vars

import (
	"log"
	"sync"

	"github.com/pkg/errors"
)

type Service struct {
	mu     sync.Mutex
	vars   Config
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) (*Service, error) {
	if c == nil {
		return nil, errors.New("must pass non nil config")
	}
	s := &Service{
		vars:   c,
		logger: l,
	}
	return s, nil
}

func (s *Service) Open() error {
	return nil
}
func (s *Service) Close() error {
	return nil
}

func (s *Service) Get(key string) string {
	s.mu.Lock()
	str := s.vars[key]
	s.mu.Unlock()
	return str
}
