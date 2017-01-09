package snmpgo_test

import (
	"math"
	"testing"

	"github.com/k-sone/snmpgo"
)

func TestSNMPArguments(t *testing.T) {
	args := &snmpgo.SNMPArguments{Version: 2}
	err := snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - version check")
	}

	args = &snmpgo.SNMPArguments{MessageMaxSize: -1}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - message size(min)")
	}

	// skip on 32 bit arch
	if (^uint(0) >> 63) > 0 {
		s := int64(math.MaxInt32) + 1
		args = &snmpgo.SNMPArguments{MessageMaxSize: int(s)}
		err = snmpgo.ArgsValidate(args)
		if err == nil {
			t.Error("validate() - message size(max)")
		}
	}

	args = &snmpgo.SNMPArguments{Version: snmpgo.V3}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - user name")
	}

	args = &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthNoPriv,
	}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - auth password")
	}

	args = &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthNoPriv,
		AuthPassword:  "aaaaaaaa",
	}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - auth protocol")
	}

	args = &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Md5,
	}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - priv password")
	}

	args = &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Md5,
		PrivPassword:  "bbbbbbbb",
	}
	err = snmpgo.ArgsValidate(args)
	if err == nil {
		t.Error("validate() - priv protocol")
	}

	args = &snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Md5,
		PrivPassword:  "bbbbbbbb",
		PrivProtocol:  snmpgo.Des,
	}
	err = snmpgo.ArgsValidate(args)
	if err != nil {
		t.Errorf("validate() - has error %v", err)
	}
}

func TestSNMP(t *testing.T) {
	_, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Md5,
		PrivPassword:  "bbbbbbbb",
		PrivProtocol:  snmpgo.Des,
	})

	if err != nil {
		t.Error("checkPdu() - report oid")
	}
}
