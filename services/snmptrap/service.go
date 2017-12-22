package snmptrap

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	text "text/template"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/k-sone/snmpgo"
	"github.com/pkg/errors"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	Error(msg string, err error)
}

type Service struct {
	configValue atomic.Value
	clientMu    sync.Mutex
	client      *snmpgo.SNMP
	diag        Diagnostic
}

func NewService(c Config, d Diagnostic) *Service {
	s := &Service{
		diag: d,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	c := s.config()
	if c.Enabled {
		err := s.loadNewSNMPClient(c)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Close() error {
	s.closeClient()
	return nil
}

func (s *Service) closeClient() {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.client != nil {
		s.client.Close()
	}
	s.client = nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) loadNewSNMPClient(c Config) error {
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Address:   c.Addr,
		Retries:   uint(c.Retries),
		Community: c.Community,
	})
	if err != nil {
		return errors.Wrap(err, "invalid SNMP configuration")
	}
	s.clientMu.Lock()
	s.client = snmp
	s.clientMu.Unlock()
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		old := s.config()
		if old != c {
			if c.Enabled {
				err := s.loadNewSNMPClient(c)
				if err != nil {
					return err
				}
			} else {
				s.closeClient()
			}
			s.configValue.Store(c)
		}
	}
	return nil
}

type testOptions struct {
	TrapOid  string `json:"trap-oid"`
	DataList []Data `json:"data-list"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		TrapOid: "1.1.1.1",
		DataList: []Data{{
			Oid:   "1.1.1.1.2",
			Type:  "s",
			Value: "test snmptrap message",
		}},
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Trap(o.TrapOid, o.DataList)
}

func (s *Service) Trap(trapOid string, dataList []Data) error {
	c := s.config()
	if !c.Enabled {
		return errors.New("service is not enabled")
	}

	// Add trap oid
	oid, err := snmpgo.NewOid(trapOid)
	if err != nil {
		return errors.Wrapf(err, "invalid trap Oid %q", trapOid)
	}
	varBinds := snmpgo.VarBinds{
		snmpgo.NewVarBind(snmpgo.OidSysUpTime, snmpgo.NewTimeTicks(1000)),
		snmpgo.NewVarBind(snmpgo.OidSnmpTrap, oid),
	}

	// Add Data
	for _, data := range dataList {
		oid, err := snmpgo.NewOid(data.Oid)
		if err != nil {
			return errors.Wrapf(err, "invalid data Oid %q", data.Oid)
		}
		// http://docstore.mik.ua/orelly/networking_2ndEd/snmp/ch10_03.htm
		switch data.Type {
		case "a":
			return errors.New("Snmptrap Datatype 'IP address' not supported")
		case "c":
			oidValue, err := strconv.ParseInt(data.Value, 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewCounter64(uint64(oidValue))))
		case "d":
			return errors.New("Snmptrap Datatype 'Decimal string' not supported")
		case "i":
			oidValue, err := strconv.ParseInt(data.Value, 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewInteger(int32(oidValue))))
		case "n":
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewNull()))
		case "o":
			return errors.New("Snmptrap Datatype 'Object ID' not supported")
		case "s":
			oidValue := []byte(data.Value)
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewOctetString(oidValue)))
		case "t":
			oidValue, err := strconv.ParseInt(data.Value, 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewTimeTicks(uint32(oidValue))))
		case "u":
			return errors.New("Snmptrap Datatype 'Unsigned integer' not supported")
		case "x":
			return errors.New("Snmptrap Datatype 'Hexadecimal string' not supported")
		default:
			return fmt.Errorf("Snmptrap Datatype not known: %v", data.Type)
		}
	}

	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if err = s.client.V2Trap(varBinds); err != nil {
		return errors.Wrap(err, "failed to send SNMP trap")
	}
	return nil
}

type HandlerConfig struct {
	TrapOid  string `mapstructure:"trap-oid"`
	DataList []Data `mapstructure:"data-list"`
}

type Data struct {
	Oid   string `mapstructure:"oid" json:"oid"`
	Type  string `mapstructure:"type" json:"type"`
	Value string `mapstructure:"value" json:"value"`
	tmpl  *text.Template
}

// handler provides the implementation of the alert.Handler interface for the Foo service.
type handler struct {
	s    *Service
	c    HandlerConfig
	diag Diagnostic
}

// Handler creates a handler from the config.
func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	// Compile data value templates
	for i, d := range c.DataList {
		tmpl, err := text.New("data").Parse(d.Value)
		if err != nil {
			return nil, err
		}
		c.DataList[i].tmpl = tmpl
	}
	return &handler{
		s:    s,
		c:    c,
		diag: s.diag.WithContext(ctx...),
	}, nil
}

// Handle takes an event triggers an SNMP trap.
func (h *handler) Handle(event alert.Event) {
	// Execute templates
	td := event.TemplateData()
	var buf bytes.Buffer
	for i, d := range h.c.DataList {
		err := d.tmpl.Execute(&buf, td)
		if err != nil {
			h.diag.Error("failed to handle event", err)
			return
		}
		h.c.DataList[i].Value = buf.String()
		buf.Reset()
	}
	if err := h.s.Trap(h.c.TrapOid, h.c.DataList); err != nil {
		h.diag.Error("failed to handle event", err)
	}
}
