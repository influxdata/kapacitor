package smtp

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdb/kapacitor/wlog"
	"gopkg.in/gomail.v2"
)

type Service struct {
	c      Config
	mail   chan *gomail.Message
	logger *log.Logger
	wg     sync.WaitGroup
}

func NewService(c Config) *Service {
	return &Service{
		c:      c,
		mail:   make(chan *gomail.Message),
		logger: wlog.New(os.Stderr, "[smtp] ", log.LstdFlags),
	}
}

func (s *Service) Open() error {
	s.wg.Add(1)
	go s.runMailer()
	return nil
}

func (s *Service) Close() error {
	close(s.mail)
	s.wg.Wait()
	return nil
}

func (s *Service) runMailer() {
	defer s.wg.Done()
	d := gomail.NewPlainDialer(s.c.Host, s.c.Port, s.c.Username, s.c.Password)

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
				log.Print(err)
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

func (s *Service) SendMail(from string, to []string, subject string, msg string) {
	if len(to) == 0 {
		return
	}
	m := gomail.NewMessage()
	m.SetHeader("From", from)
	m.SetHeader("To", to...)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", msg)
	s.mail <- m
}
