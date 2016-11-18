package udf

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/udf"
)

type Service struct {
	configs map[string]FunctionConfig
	infos   map[string]udf.Info
	logger  *log.Logger
	mu      sync.RWMutex
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		configs: c.Functions,
		infos:   make(map[string]udf.Info),
		logger:  l,
	}
}

func (s *Service) Open() error {
	for name := range s.configs {
		err := s.Refresh(name)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	infos := make([]string, 0, len(s.infos))
	for name := range s.infos {
		infos = append(infos, name)
	}
	return infos
}

func (s *Service) Info(name string) (udf.Info, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	info, ok := s.infos[name]
	return info, ok
}

func (s *Service) Create(
	name string,
	l *log.Logger,
	abortCallback func(),
) (udf.Interface, error) {
	conf, ok := s.configs[name]
	if !ok {
		return nil, fmt.Errorf("no such UDF %s", name)
	}
	if conf.Socket != "" {
		// Create socket UDF
		return kapacitor.NewUDFSocket(
			kapacitor.NewSocketConn(conf.Socket),
			l,
			time.Duration(conf.Timeout),
			abortCallback,
		), nil
	} else {
		// Create process UDF
		env := os.Environ()
		for k, v := range conf.Env {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		cmdInfo := command.CommandInfo{
			Prog: conf.Prog,
			Args: conf.Args,
			Env:  env,
		}
		return kapacitor.NewUDFProcess(
			command.ExecCommander,
			cmdInfo,
			l,
			time.Duration(conf.Timeout),
			abortCallback,
		), nil
	}
}

func (s *Service) Refresh(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.loadUDFInfo(name)
	if err != nil {
		return fmt.Errorf("failed to load process info for %q: %v", name, err)
	}
	s.infos[name] = info
	s.logger.Printf("D! loaded UDF info %q", name)
	return nil
}

func (s *Service) loadUDFInfo(name string) (udf.Info, error) {
	u, err := s.Create(name, s.logger, nil)
	if err != nil {
		return udf.Info{}, err
	}
	err = u.Open()
	if err != nil {
		return udf.Info{}, err
	}
	defer u.Close()
	info, err := u.Info()
	if err != nil {
		return udf.Info{}, err
	}
	return info, nil
}
