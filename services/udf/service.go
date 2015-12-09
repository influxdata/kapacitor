package udf

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
)

type Service struct {
	functionConfig map[string]FunctionConfig
	functions      map[string]kapacitor.UDFProcessInfo
	logger         *log.Logger
	mu             sync.RWMutex
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		functionConfig: c.Functions,
		functions:      make(map[string]kapacitor.UDFProcessInfo),
		logger:         l,
	}
}

func (s *Service) Open() error {
	for name := range s.functionConfig {
		err := s.RefreshFunction(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) FunctionList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	functions := make([]string, 0, len(s.functions))
	for name := range s.functions {
		functions = append(functions, name)
	}
	return functions
}

func (s *Service) FunctionInfo(name string) (kapacitor.UDFProcessInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.functions[name]
	return info, ok
}

func (s *Service) RefreshFunction(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fc, ok := s.functionConfig[name]
	if ok {
		info, err := s.loadProcessInfo(fc)
		if err != nil {
			return fmt.Errorf("failed to load process info for %q: %v", name, err)
		}
		s.functions[name] = info
		s.logger.Printf("D! loaded UDF %q", name)
		return nil
	}
	return fmt.Errorf("no function %s configured", name)
}

func (s *Service) loadProcessInfo(f FunctionConfig) (kapacitor.UDFProcessInfo, error) {
	env := os.Environ()
	for k, v := range f.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	commander := command.CommandInfo{
		Prog: f.Prog,
		Args: f.Args,
		Env:  env,
	}
	p := kapacitor.NewUDFProcess(commander, s.logger, time.Duration(f.Timeout), nil)
	err := p.Start()
	if err != nil {
		return kapacitor.UDFProcessInfo{}, err
	}
	defer p.Stop()
	return p.Info()
}
