package smtptest

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/mail"
	"net/textproto"
	"os"
	"strconv"
	"sync"
)

type Message struct {
	Header mail.Header
	Body   string
}

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

var replyGreeting = `220 hello`
var replyOK = `250 Ok`
var replyData = `354 Go ahead`
var replyGoodbye = `221 Goodbye`

type teeReadWriteCloser struct {
	io.Reader
	io.Writer
	io.Closer
}

func newTeeReadWriteCloser(orig io.ReadWriteCloser, tee io.Writer) io.ReadWriteCloser {
	r := io.TeeReader(orig, tee)
	return teeReadWriteCloser{
		r,
		orig,
		orig,
	}
}

func (s *Server) handleConn(conn net.Conn) {
	trwc := newTeeReadWriteCloser(conn, os.Stdout)
	errC := make(chan error, 2)
	go func() {
		var err error
		var line string
		tc := textproto.NewConn(trwc)
		tc.PrintfLine(replyGreeting)
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
				tc.PrintfLine(replyData)
				dotReader := tc.DotReader()
				message, err = mail.ReadMessage(dotReader)
				if err != nil {
					goto FAIL
				}
				body, err := ioutil.ReadAll(message.Body)
				if err != nil {
					goto FAIL
				}
				msg := &Message{
					Header: message.Header,
					Body:   string(body),
				}
				s.mu.Lock()
				s.sentMessages = append(s.sentMessages, msg)
				s.mu.Unlock()
				tc.PrintfLine(replyOK)
			case "QUIT":
				tc.PrintfLine(replyGoodbye)
				errC <- nil
				return
			}
		}
	FAIL:
		errC <- err
		tc.PrintfLine(replyGoodbye)
		return
	}()
	err := <-errC
	if err != nil {
		s.mu.Lock()
		s.errors = append(s.errors, err)
		s.mu.Unlock()
	}
}
