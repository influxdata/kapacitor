package snmptrap

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync/atomic"

	"github.com/influxdata/kapacitor"
	"github.com/k-sone/snmpgo"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
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

type testOptions struct {
	TargetIp   string               `json:"target-ip"`
	TargetPort int                  `json:"target-port"`
	Community  string               `json:"community"`
	Version    string               `json:"version"`
	Message    string               `json:"message"`
	Level      kapacitor.AlertLevel `json:"level"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Message: "test snmptrap message",
		Level:   kapacitor.CritAlert,
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	trapOid := "1.1.1.1"
	var dataList [][3]string
	var data [3]string
	data[0] = "1.1.1.1.2"
	data[1] = "s"
	data[2] = "test msg"
	dataList = append(dataList, data)
	return s.Alert(trapOid, dataList, o.Level)
}

func (s *Service) Alert(trapOid string, dataList [][3]string, level kapacitor.AlertLevel) error {
	c := s.config()
	// SNMP target address
	address := c.TargetIp + ":" + strconv.Itoa(c.TargetPort)
	// SNMP version
	var version snmpgo.SNMPVersion
	switch c.Version {
	case "1":
		return errors.New("Version 1 not supported yet")
	case "2c":
		version = snmpgo.V2c
	case "3":
		return errors.New("Version 3 not supported yet")
	default:
		return errors.New("Bad snmp version should be: '1', '2c' or '3'")
	}
	// Create SNMP client
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:   version,
		Address:   address,
		Retries:   1,
		Community: c.Community,
	})

	var varBinds snmpgo.VarBinds
	// Add trap oid
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSysUpTime, snmpgo.NewTimeTicks(1000)))
	oid, _ := snmpgo.NewOid(trapOid)
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSnmpTrap, oid))
	// Add Data
	for _, data := range dataList {
		oidStr := data[0]
		oidTypeRaw := data[1]
		oid, _ := snmpgo.NewOid(oidStr)
		// http://docstore.mik.ua/orelly/networking_2ndEd/snmp/ch10_03.htm
		switch oidTypeRaw {
		case "a":
			return errors.New("Snmptrap Datatype 'IP address' not supported")
		case "c":
			oidValue, err := strconv.ParseInt(data[2], 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewCounter64(uint64(oidValue))))
		case "d":
			return errors.New("Snmptrap Datatype 'Decimal string' not supported")
		case "i":
			oidValue, err := strconv.ParseInt(data[2], 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewInteger(int32(oidValue))))
		case "n":
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewNull()))
		case "o":
			return errors.New("Snmptrap Datatype 'Object ID' not supported")
		case "s":
			oidValue := []byte(data[2])
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewOctetString(oidValue)))
		case "t":
			oidValue, err := strconv.ParseInt(data[2], 10, 64)
			if err != nil {
				return err
			}
			varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewTimeTicks(uint32(oidValue))))
		case "u":
			return errors.New("Snmptrap Datatype 'Unsigned integer' not supported")
		case "x":
			return errors.New("Snmptrap Datatype 'Hexadecimal string' not supported")
		default:
			return errors.New("Snmptrap Datatype not supported: " + oidTypeRaw)
		}
	}

	if err = snmp.Open(); err != nil {
		// Failed to open connection
		fmt.Println(err)
		return err
	}
	defer snmp.Close()

	if err = snmp.V2Trap(varBinds); err != nil {
		// Failed to request
		fmt.Println(err)
		return err
	}

	return nil
}
