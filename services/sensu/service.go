package sensu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"regexp"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	addr   string
	source string
	logger *log.Logger
}

var validNamePattern = regexp.MustCompile(`^[\w\.-]+$`)

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		addr:   c.Addr,
		source: c.Source,
		logger: l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Alert(name, output string, level kapacitor.AlertLevel) error {
	if !validNamePattern.MatchString(name) {
		return fmt.Errorf("invalid name %q for sensu alert. Must match %v", name, validNamePattern)
	}

	var status int
	switch level {
	case kapacitor.OKAlert:
		status = 0
	case kapacitor.InfoAlert:
		status = 0
	case kapacitor.WarnAlert:
		status = 1
	case kapacitor.CritAlert:
		status = 2
	default:
		status = 3
	}

	postData := make(map[string]interface{})
	postData["name"] = name
	postData["source"] = s.source
	postData["output"] = output
	postData["status"] = status

	addr, err := net.ResolveTCPAddr("tcp", s.addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	err = enc.Encode(postData)
	if err != nil {
		return err
	}
	resp, err := ioutil.ReadAll(conn)
	if string(resp) != "ok" {
		return errors.New("sensu socket error: " + string(resp))
	}
	return nil
}
