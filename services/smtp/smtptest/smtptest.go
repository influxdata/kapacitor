package smtptest

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/mail"
	"net/textproto"
	"strconv"
	"sync"
)

type Server struct {
	Host string
	Port int
	Err  error

	l            *net.TCPListener
	wg           sync.WaitGroup
	mu           sync.Mutex
	sentMessages []*Message
	errors       []error
}

func NewServer() (*Server, error) {
	laddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return nil, err
	}

	addr := l.Addr()
	host, portStr, err := net.SplitHostPort(addr.String())
	if err != nil {
		return nil, err
	}
	port, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return nil, err
	}
	s := &Server{
		Host: host,
		Port: int(port),
		l:    l,
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	return s, nil
}

func (s *Server) Errors() []error {
	return s.errors
}

func (s *Server) SentMessages() []*Message {
	return s.sentMessages
}

func (s *Server) Close() error {
	s.l.Close()
	s.wg.Wait()
	return nil
}

func (s *Server) run() {
	for {
		conn, err := s.l.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			s.handleConn(conn)
		}()
	}
}

const (
	replyGreeting = "220 hello"
	replyOK       = "250 Ok"
	replyData     = "354 Go ahead"
	replyGoodbye  = "221 Goodbye"
)

// handleConn takes a connection and implements a simplified SMTP protocol,
// while capturing the message contents.
func (s *Server) handleConn(conn net.Conn) {
	var err error
	var line string
	tc := textproto.NewConn(conn)
	err = tc.PrintfLine(replyGreeting)
	if err != nil {
		goto FAIL
	}
	for {
		line, err = tc.ReadLine()
		if err != nil {
			goto FAIL
		}
		if len(line) < 4 {
			err = fmt.Errorf("unexpected data %q", line)
			goto FAIL
		}
		switch line[:4] {
		case "EHLO", "MAIL", "RCPT":
			tc.PrintfLine(replyOK)
		case "DATA":
			var message *mail.Message
			var body []byte
			err = tc.PrintfLine(replyData)
			if err != nil {
				goto FAIL
			}
			dotReader := tc.DotReader()
			message, err = mail.ReadMessage(dotReader)
			if err != nil {
				goto FAIL
			}
			body, err = ioutil.ReadAll(message.Body)
			if err != nil {
				goto FAIL
			}
			s.mu.Lock()
			s.sentMessages = append(s.sentMessages, &Message{
				Header: message.Header,
				Body:   string(body),
			})
			s.mu.Unlock()
			err = tc.PrintfLine(replyOK)
			if err != nil {
				goto FAIL
			}
		case "QUIT":
			err = tc.PrintfLine(replyGoodbye)
			if err != nil {
				goto FAIL
			}
			return
		}
	}
FAIL:
	tc.PrintfLine(replyGoodbye)
	s.mu.Lock()
	s.errors = append(s.errors, err)
	s.mu.Unlock()
}

type Message struct {
	Header mail.Header
	Body   string
}

// Compare returns a useful error message if the two message are note equal.
// Only headers that are present in the exp message are compared, thus ignoring any extra headers in the got message.
func (exp *Message) Compare(got *Message) error {
	if exp.Body != got.Body {
		return fmt.Errorf("unequal bodies:\ngot\n%q\nexp\n%q\n", got.Body, exp.Body)
	}
	// Compare only the header keys specified in the exp message.
	for k, ev := range exp.Header {
		gv, ok := got.Header[k]
		if !ok {
			return fmt.Errorf("missing header %s", k)
		}
		if len(gv) != len(ev) {
			return fmt.Errorf("unexpected header %s: got %v exp %v", k, gv, ev)
		}
		for i := range ev {
			if gv[i] != ev[i] {
				return fmt.Errorf("unexpected header %s: got %v exp %v", k, gv, ev)
			}
		}
	}
	return nil
}
