package snmptraptest

import (
	"sync"

	"github.com/k-sone/snmpgo"
)

type Server struct {
	traps []Trap

	Addr      string
	Community string

	closed bool
	mu     sync.Mutex
	wg     sync.WaitGroup
	srv    *snmpgo.TrapServer
}

func NewServer() (*Server, error) {
	addr := "127.0.0.1:9162"
	srv, err := snmpgo.NewTrapServer(snmpgo.ServerArguments{
		LocalAddr: addr,
	})
	if err != nil {
		return nil, err
	}
	s := &Server{
		srv:       srv,
		Addr:      addr,
		Community: "public",
	}
	s.srv.AddSecurity(&snmpgo.SecurityEntry{
		Version:   snmpgo.V2c,
		Community: s.Community,
	})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.srv.Serve(s)
	}()
	return s, nil
}

func (s *Server) Traps() []Trap {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.traps
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	err := s.srv.Close()
	s.wg.Wait()
	return err
}

func (s *Server) OnTRAP(trap *snmpgo.TrapRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := Trap{
		Pdu:   convertPdu(trap.Pdu),
		Error: trap.Error,
	}
	s.traps = append(s.traps, t)
}

type Trap struct {
	Pdu   Pdu
	Error error
}

type Pdu struct {
	IsNil       bool
	Type        snmpgo.PduType
	ErrorStatus snmpgo.ErrorStatus
	ErrorIndex  int
	VarBinds    VarBinds
}

func convertPdu(pdu snmpgo.Pdu) Pdu {
	if pdu == nil {
		return Pdu{IsNil: true}
	}
	return Pdu{
		Type:        pdu.PduType(),
		ErrorStatus: pdu.ErrorStatus(),
		ErrorIndex:  pdu.ErrorIndex(),
		VarBinds:    convertVarBinds(pdu.VarBinds()),
	}
}

type VarBinds []VarBind

type VarBind struct {
	Oid   string
	Value string
	Type  string
}

func convertVarBinds(vbs snmpgo.VarBinds) VarBinds {
	binds := make(VarBinds, len(vbs))
	for i, vb := range vbs {
		binds[i] = VarBind{
			Oid:   vb.Oid.String(),
			Value: vb.Variable.String(),
			Type:  vb.Variable.Type(),
		}
	}
	return binds
}
