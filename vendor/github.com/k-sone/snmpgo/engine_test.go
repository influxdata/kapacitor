package snmpgo_test

import (
	"testing"

	"github.com/k-sone/snmpgo"
)

func TestCheckPdu(t *testing.T) {
	args := &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Md5,
		PrivPassword:  "bbbbbbbb",
		PrivProtocol:  snmpgo.Des,
	}

	eng := snmpgo.NewSNMPEngine(args)
	pdu := snmpgo.NewPdu(snmpgo.V3, snmpgo.Report)
	err := snmpgo.CheckPdu(eng, pdu, args)
	if err != nil {
		t.Errorf("checkPdu() - has error %v", err)
	}

	oids, _ := snmpgo.NewOids([]string{"1.3.6.1.6.3.11.2.1.1.0"})
	pdu = snmpgo.NewPduWithOids(snmpgo.V3, snmpgo.Report, oids)
	err = snmpgo.CheckPdu(eng, pdu, args)
	if err == nil {
		t.Error("checkPdu() - report oid")
	}
}
