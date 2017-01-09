package snmptest

import (
	"testing"

	"github.com/k-sone/snmpgo"
)

type TrapSender struct {
	t       *testing.T
	Address string
}

func NewTrapSender(t *testing.T, address string) *TrapSender {
	return &TrapSender{t: t, Address: address}
}

func (t *TrapSender) SendV2TrapWithBindings(trap bool, community string, v snmpgo.VarBinds) {
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Address:   t.Address,
		Network:   "udp4",
		Retries:   1,
		Community: community,
	})
	if err != nil {
		// Failed to create SNMP object
		t.t.Fatal(err)
		return
	}

	if err = snmp.Open(); err != nil {
		// Failed to open connection
		t.t.Fatal(err)
		return
	}

	defer snmp.Close()

	if trap {
		err = snmp.V2Trap(v)
	} else {
		err = snmp.InformRequest(v)
	}

	if err != nil {
		// Failed to request
		t.t.Fatal(err)
		return
	}
}

func (t *TrapSender) SendV3TrapWithBindings(v snmpgo.VarBinds, l snmpgo.SecurityLevel, eid string, eBoots, eTime int) {
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:          snmpgo.V3,
		Address:          t.Address,
		Network:          "udp4",
		Retries:          1,
		UserName:         "MyName",
		SecurityLevel:    l,
		AuthPassword:     "aaaaaaaa",
		AuthProtocol:     snmpgo.Sha,
		PrivPassword:     "bbbbbbbb",
		PrivProtocol:     snmpgo.Aes,
		SecurityEngineId: eid,
	})

	if err != nil {
		// Failed to create SNMP object
		t.t.Fatal(err)
		return
	}

	if err = snmp.Open(); err != nil {
		// Failed to open connection
		t.t.Fatal(err)
		return
	}

	defer snmp.Close()

	err = snmp.V2TrapWithBootsTime(v, eBoots, eTime)
	if err != nil {
		// Failed to request
		t.t.Fatal(err)
		return
	}
}

// NewTrapServer creates a new Trap Server & Serve
func NewTrapServer(address string, listener snmpgo.TrapListener) *snmpgo.TrapServer {
	s, _ := snmpgo.NewTrapServer(snmpgo.ServerArguments{
		LocalAddr: address,
	})
	s.AddSecurity(&snmpgo.SecurityEntry{
		Version:   snmpgo.V2c,
		Community: "public",
	})
	s.AddSecurity(&snmpgo.SecurityEntry{
		Version:          snmpgo.V3,
		UserName:         "MyName",
		SecurityLevel:    snmpgo.AuthPriv,
		AuthPassword:     "aaaaaaaa",
		AuthProtocol:     snmpgo.Sha,
		PrivPassword:     "bbbbbbbb",
		PrivProtocol:     snmpgo.Aes,
		SecurityEngineId: "8000000004736e6d70676f",
	})
	s.AddSecurity(&snmpgo.SecurityEntry{
		Version:          snmpgo.V3,
		UserName:         "MyName",
		SecurityLevel:    snmpgo.NoAuthNoPriv,
		SecurityEngineId: "8000000004736e6d70676f5f6e6f61757468",
	})

	ch := make(chan struct{}, 0)
	go func() {
		ch <- struct{}{}
		s.Serve(listener)
	}()
	<-ch
	return s
}
