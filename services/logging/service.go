package logging

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"go.uber.org/zap"

	zaplogfmt "github.com/jsternberg/zap-logfmt"
)

// Interface for creating new loggers
type Interface interface {
	Root() zap.Logger
	Writer() io.Writer
	SetLevel(level string) error
}

type Service struct {
	root   zap.Logger
	c      Config
	stdout WriteSyncer
	stderr WriteSyncer
	writer io.Writer
	closer io.Closer
	level  zap.AtomicLevel

	wg sync.WaitGroup

	entries chan entry
}

type WriteSyncer interface {
	io.Writer
	Sync() error
}

func NewService(c Config, stdout, stderr WriteSyncer) *Service {
	return &Service{
		c:       c,
		stdout:  stdout,
		stderr:  stderr,
		level:   zap.DynamicLevel(),
		entries: make(chan entry, 5000),
	}
}

func (s *Service) Open() error {
	var output WriteSyncer
	switch s.c.File {
	case "STDERR":
		output = s.stderr
	case "STDOUT":
		output = s.stdout
	default:
		dir := path.Dir(s.c.File)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err := os.MkdirAll(dir, 0755)
			if err != nil {
				return err
			}
		}

		f, err := os.OpenFile(s.c.File, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
		if err != nil {
			return err
		}
		output = f
		s.closer = f
	}
	s.writer = output

	// Set level from configuration
	if err := s.SetLevel(s.c.Level); err != nil {
		return err
	}

	var encoder zap.Encoder
	switch s.c.Encoding {
	case "logfmt":
		encoder = zaplogfmt.NewEncoder()
	case "text":
		encoder = zap.NewTextEncoder()
	default:
		return fmt.Errorf("unknown log encoding %s", s.c.Encoding)
	}

	// Start  readEntries goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.readEntries()
	}()

	// Create root logger
	s.root = zap.New(
		newTeeEncoder([2]zap.Encoder{newChanEncoder(s.entries), encoder}),
		zap.Output(output),
		s.level,
	)

	// Configure default logger, should not be used.
	log.SetPrefix("[log] ")
	log.SetFlags(log.LstdFlags)
	log.SetOutput(output)

	return nil
}

func (s *Service) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *Service) Root() zap.Logger {
	return s.root
}

func (s *Service) Writer() io.Writer {
	return s.writer
}

func (s *Service) SetLevel(level string) error {
	log.Println("setting log level", level)
	switch strings.ToUpper(level) {
	case "DEBUG":
		s.level.SetLevel(zap.DebugLevel)
	case "INFO":
		s.level.SetLevel(zap.InfoLevel)
	case "WARN":
		s.level.SetLevel(zap.WarnLevel)
	case "ERROR":
		s.level.SetLevel(zap.ErrorLevel)
	default:
		return fmt.Errorf("unknown logging level %s", level)
	}
	return nil
}

func (s *Service) Subscribe(level zap.Level, match map[string]interface{}) *Subscription {
	return nil
}

func (s *Service) readEntries() {
	for e := range s.entries {
		log.Println(e)
	}
}
