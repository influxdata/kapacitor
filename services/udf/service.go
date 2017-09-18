package udf

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/udf"
)

type Diagnostic interface {
	LoadedUDFInfo(udf string)

	WithUDFContext() udf.Diagnostic
}

type Service struct {
	configs map[string]FunctionConfig
	infos   map[string]udf.Info
	diag    Diagnostic
	mu      sync.RWMutex
}

func NewService(c Config, d Diagnostic) *Service {
	return &Service{
		configs: c.Functions,
		infos:   make(map[string]udf.Info),
		diag:    d,
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
	name, taskID, nodeID string,
	d udf.Diagnostic,
	abortCallback func(),
) (udf.Interface, error) {
	conf, ok := s.configs[name]
	if !ok {
		return nil, fmt.Errorf("no such UDF %s", name)
	}
	if conf.Socket != "" {
		// Create socket UDF
		return kapacitor.NewUDFSocket(
			taskID, nodeID,
			kapacitor.NewSocketConn(conf.Socket),
			d,
			time.Duration(conf.Timeout),
			abortCallback,
		), nil
	} else {
		// Create process UDF
		env := os.Environ()
		for k, v := range conf.Env {
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
		cmdSpec := command.Spec{
			Prog: conf.Prog,
			Args: conf.Args,
			Env:  env,
		}
		return kapacitor.NewUDFProcess(
			taskID, nodeID,
			command.ExecCommander,
			cmdSpec,
			d,
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
	s.diag.LoadedUDFInfo(name)
	return nil
}

func (s *Service) loadUDFInfo(name string) (udf.Info, error) {
	// loadUDFInfo creates a UDF connection outside the context of a task or node
	// because it only makes the Info request and never makes an Init request.
	// As such it does not need to provide actual task and node IDs.
	u, err := s.Create(name, "", "", s.diag.WithUDFContext(), nil)
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
