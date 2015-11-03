package logging

import (
	"io"
	"log"
	"os"
	"path"

	"github.com/influxdb/kapacitor/wlog"
)

type Service struct {
	f      io.WriteCloser
	c      Config
	stdout io.Writer
	stderr io.Writer
}

func NewService(c Config, stdout, stderr io.Writer) *Service {
	return &Service{
		c:      c,
		stdout: stdout,
		stderr: stderr,
	}
}

func (s *Service) Open() error {
	switch s.c.File {
	case "STDERR":
		s.f = &nopCloser{f: s.stderr}
	case "STDOUT":
		s.f = &nopCloser{f: s.stdout}
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
		s.f = f
	}

	wlog.SetLevel(s.c.Level)
	return nil
}

func (s *Service) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

func (s *Service) NewLogger(prefix string, flag int) *log.Logger {
	return wlog.New(s.f, prefix, flag)
}

type nopCloser struct {
	f io.Writer
}

func (c *nopCloser) Write(b []byte) (int, error) { return c.f.Write(b) }
func (c *nopCloser) Close() error                { return nil }
