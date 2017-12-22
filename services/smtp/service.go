package smtp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"

	"gopkg.in/gomail.v2"
)

var ErrNoRecipients = errors.New("not sending email, no recipients defined")

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

type Service struct {
	mu          sync.Mutex
	configValue atomic.Value
	mail        chan *gomail.Message
	updates     chan bool
	diag        Diagnostic
	wg          sync.WaitGroup
	opened      bool
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		updates: make(chan bool),
		diag:    d,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opened {
		return nil
	}
	s.opened = true

	s.mail = make(chan *gomail.Message)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runMailer()
	}()

	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.opened {
		return nil
	}
	s.opened = false

	close(s.mail)
	s.wg.Wait()

	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
		s.mu.Lock()
		opened := s.opened
		s.mu.Unlock()
		if opened {
			// Signal to create new dialer
			s.updates <- true
		}
	}
	return nil
}

func (s *Service) Global() bool {
	c := s.config()
	return c.Global
}

func (s *Service) StateChangesOnly() bool {
	c := s.config()
	return c.StateChangesOnly
}

func (s *Service) dialer() (d *gomail.Dialer, idleTimeout time.Duration) {
	c := s.config()
	if c.Username == "" {
		d = &gomail.Dialer{Host: c.Host, Port: c.Port}
	} else {
		d = gomail.NewPlainDialer(c.Host, c.Port, c.Username, c.Password)
	}
	if c.NoVerify {
		d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	idleTimeout = time.Duration(c.IdleTimeout)
	return
}

func (s *Service) runMailer() {
	var idleTimeout time.Duration
	var d *gomail.Dialer
	d, idleTimeout = s.dialer()

	var conn gomail.SendCloser
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	var err error
	open := false
	for {
		timer := time.NewTimer(idleTimeout)
		select {
		case <-s.updates:
			// Close old connection
			if conn != nil {
				if err := conn.Close(); err != nil {
					s.diag.Error("error closing connection to old SMTP server", err)
				}
				conn = nil
			}
			// Create new dialer
			d, idleTimeout = s.dialer()
			open = false
		case m, ok := <-s.mail:
			if !ok {
				return
			}
			if !open {
				if conn, err = d.Dial(); err != nil {
					s.diag.Error("error closing connection to SMTP server", err)
					break
				}
				open = true
			}
			if err := gomail.Send(conn, m); err != nil {
				s.diag.Error("error sending", err)
			}
		// Close the connection to the SMTP server if no email was sent in
		// the last IdleTimeout duration.
		case <-timer.C:
			if open {
				if err := conn.Close(); err != nil {
					s.diag.Error("error closing connection to SMTP server", err)
				}
				open = false
			}
		}
		timer.Stop()
	}
}

func (s *Service) SendMail(to []string, subject, body string) error {
	m, err := s.prepareMessge(to, subject, body)
	if err != nil {
		return err
	}
	s.mail <- m
	return nil
}

func (s *Service) prepareMessge(to []string, subject, body string) (*gomail.Message, error) {
	c := s.config()
	if !c.Enabled {
		return nil, errors.New("service is not enabled")
	}
	if len(to) == 0 {
		to = c.To
	}
	if len(to) == 0 {
		return nil, ErrNoRecipients
	}
	m := gomail.NewMessage()
	m.SetHeader("From", c.From)
	m.SetHeader("To", to...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)
	return m, nil
}

type testOptions struct {
	To      []string `json:"to"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		To:      c.To,
		Subject: "test subject",
		Body:    "test body",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.SendMail(
		o.To,
		o.Subject,
		o.Body,
	)
}

type HandlerConfig struct {
	// List of email recipients.
	To []string `mapstructure:"to"`
}

type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) alert.Handler {
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}
}

func (h *handler) Handle(event alert.Event) {
	if err := h.s.SendMail(
		h.c.To,
		event.State.Message,
		event.State.Details,
	); err != nil {
		h.diag.Error("failed to send email", err)
	}
}
