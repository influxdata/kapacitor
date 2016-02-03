package smtp

import (
	"crypto/tls"
	"errors"
	"log"
	"sync"
	"time"

	"gopkg.in/gomail.v2"
)

var ErrNoRecipients = errors.New("not sending email, no recipients defined")

type Service struct {
	c      Config
	mail   chan *gomail.Message
	logger *log.Logger
	wg     sync.WaitGroup
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		c:      c,
		mail:   make(chan *gomail.Message),
		logger: l,
	}
}

func (s *Service) Open() error {
	s.logger.Println("I! Starting SMTP service")
	if s.c.From == "" {
		return errors.New("cannot open smtp service: missing from address in configuration")
	}
	s.wg.Add(1)
	go s.runMailer()
	return nil
}

func (s *Service) Close() error {
	s.logger.Println("I! Closing SMTP service")
	close(s.mail)
	s.wg.Wait()
	return nil
}

func (s *Service) Global() bool {
	return s.c.Global
}

func (s *Service) runMailer() {
	defer s.wg.Done()
	var d *gomail.Dialer
	if s.c.Username == "" {
		d = &gomail.Dialer{Host: s.c.Host, Port: s.c.Port}
	} else {
		d = gomail.NewPlainDialer(s.c.Host, s.c.Port, s.c.Username, s.c.Password)
	}
	if s.c.NoVerify {
		d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	var conn gomail.SendCloser
	var err error
	open := false
	for {
		select {
		case m, ok := <-s.mail:
			if !ok {
				return
			}
			if !open {
				if conn, err = d.Dial(); err != nil {
					s.logger.Println("E! error connecting to SMTP server", err)
					continue
				}
				open = true
			}
			if err := gomail.Send(conn, m); err != nil {
				s.logger.Println("E!", err)
			}
		// Close the connection to the SMTP server if no email was sent in
		// the last IdleTimeout duration.
		case <-time.After(time.Duration(s.c.IdleTimeout)):
			if open {
				if err := conn.Close(); err != nil {
					panic(err)
				}
				open = false
			}
		}
	}
}

func (s *Service) SendMail(to []string, subject, body string) error {
	if len(to) == 0 {
		to = s.c.To
	}
	if len(to) == 0 {
		return ErrNoRecipients
	}
	m := gomail.NewMessage()
	m.SetHeader("From", s.c.From)
	m.SetHeader("To", to...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)
	s.mail <- m
	return nil
}
