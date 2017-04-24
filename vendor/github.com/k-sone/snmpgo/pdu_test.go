package snmpgo_test

import (
	"bytes"
	"testing"

	"github.com/k-sone/snmpgo"
)

func testVarBind(t *testing.T, v *snmpgo.VarBind, expStr string) {
	var w snmpgo.VarBind
	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal() : %v", err)
	}
	rest, err := (&w).Unmarshal(buf)
	if len(rest) != 0 || err != nil {
		t.Errorf("Unmarshal() - len[%d] err[%v]", len(rest), err)
	}
	if expStr != w.String() {
		t.Errorf("Unmarshal() - expected [%s], actual [%s]", expStr, w.String())
	}

	buf = append(buf, 0x00)
	rest, err = (&w).Unmarshal(buf)
	if len(rest) != 1 || err != nil {
		t.Errorf("Unmarshal() with rest - len[%d] err[%v]", len(rest), err)
	}
	if expStr != w.String() {
		t.Errorf("Unmarshal() - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestVarBind(t *testing.T) {
	var v snmpgo.VarBind
	oid, _ := snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	v = snmpgo.VarBind{Oid: oid}

	v.Variable = snmpgo.NewInteger(-2147483648)
	testVarBind(t, &v,
		`{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "Integer", "Value": "-2147483648"}}`)

	v.Variable = snmpgo.NewOctetString([]byte("MyHost"))
	testVarBind(t, &v,
		`{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "OctetString", "Value": "MyHost"}}`)

	v.Variable = snmpgo.NewNull()
	testVarBind(t, &v, `{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "Null", "Value": ""}}`)

	v.Variable = snmpgo.NewCounter32(uint32(4294967295))
	testVarBind(t, &v,
		`{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "Counter32", "Value": "4294967295"}}`)

	v.Variable = snmpgo.NewCounter64(uint64(18446744073709551615))
	testVarBind(t, &v, `{"Oid": "1.3.6.1.2.1.1.1.0", `+
		`"Variable": {"Type": "Counter64", "Value": "18446744073709551615"}}`)

	expBuf := []byte{0x30, 0x00}
	v = snmpgo.VarBind{}
	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal() : %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	buf = []byte{0x00, 0x00}
	_, err = (&v).Unmarshal(buf)
	if err == nil {
		t.Errorf("Unmarshal() : can not validation")
	}
}

func TestVarBinds(t *testing.T) {
	var v snmpgo.VarBinds

	oid, _ := snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	v = append(v, snmpgo.NewVarBind(oid, snmpgo.NewOctetString([]byte("MyHost"))))
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.2.0")
	v = append(v, snmpgo.NewVarBind(oid, snmpgo.NewNull()))
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.3.0")
	v = append(v, snmpgo.NewVarBind(oid, snmpgo.NewTimeTicks(uint32(11111))))

	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	varBind := v.MatchOid(oid)
	if varBind == nil || !varBind.Oid.Equal(oid) {
		t.Errorf("Failed to MatchOid()")
	}
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.1.1")
	varBind = v.MatchOid(oid)
	if varBind != nil {
		t.Errorf("Failed to MatchOid() - no match")
	}
	varBind = v.MatchOid(nil)
	if varBind != nil {
		t.Errorf("Failed to MatchOid() - nil")
	}

	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1")
	varBinds := v.MatchBaseOids(oid)
	if len(varBinds) != 3 {
		t.Errorf("Failed to MatchBaseOids()")
	}
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	varBinds = v.MatchBaseOids(oid)
	if len(varBinds) != 1 || !varBinds[0].Oid.Equal(oid) {
		t.Errorf("Failed to MatchBaseOids() - one")
	}
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.1.1")
	varBinds = v.MatchBaseOids(oid)
	if len(varBinds) != 0 {
		t.Errorf("Failed to MatchBaseOids() - no match")
	}
	varBinds = v.MatchBaseOids(nil)
	if len(varBinds) != 0 {
		t.Errorf("Failed to MatchBaseOids() - nil")
	}

	var w snmpgo.VarBinds
	for _, o := range []string{
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.3.0",
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
	} {
		oid, _ = snmpgo.NewOid(o)
		w = append(w, snmpgo.NewVarBind(oid, snmpgo.NewNull()))
	}

	expOids, _ := snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	w = w.Sort()
	if len(expOids) != len(w) {
		t.Errorf("Sort() - expected [%d], actual [%d]", len(expOids), len(w))
	}
	for i, o := range expOids {
		if !o.Equal(w[i].Oid) {
			t.Errorf("Sort() - expected [%s], actual [%s]", o, w[i].Oid)
		}
	}

	expOids, _ = snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	w = w.Sort().Uniq()
	if len(expOids) != len(w) {
		t.Errorf("Uniq() - expected [%d], actual [%d]", len(expOids), len(w))
	}
	for i, o := range expOids {
		if !o.Equal(w[i].Oid) {
			t.Errorf("Uniq() - expected [%s], actual [%s]", o, w[i].Oid)
		}
	}
}

func TestNewPdu(t *testing.T) {
	pdu := snmpgo.NewPdu(snmpgo.V1, snmpgo.GetRequest)
	if _, ok := pdu.(*snmpgo.PduV1); !ok {
		t.Errorf("NewPdu() Invalid Pdu")
	}

	pdu = snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetRequest)
	if _, ok := pdu.(*snmpgo.PduV1); !ok {
		t.Errorf("NewPdu() Invalid Pdu")
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.GetRequest)
	if _, ok := pdu.(*snmpgo.ScopedPdu); !ok {
		t.Errorf("NewPdu() Invalid Pdu")
	}
}

func TestPduV1(t *testing.T) {
	pdu := snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetRequest)
	pdu.SetRequestId(123)
	pdu.SetErrorStatus(snmpgo.TooBig)
	pdu.SetErrorIndex(2)

	oid, _ := snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	pdu.AppendVarBind(oid, snmpgo.NewOctetString([]byte("MyHost")))
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.2.0")
	pdu.AppendVarBind(oid, snmpgo.NewNull())
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.3.0")
	pdu.AppendVarBind(oid, snmpgo.NewTimeTicks(uint32(11111)))

	expBuf := []byte{
		0xa0, 0x3d, 0x02, 0x01, 0x7b, 0x02, 0x01, 0x01, 0x02, 0x01, 0x02,
		0x30, 0x32, 0x30, 0x12, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02, 0x01,
		0x01, 0x01, 0x00, 0x04, 0x06, 0x4d, 0x79, 0x48, 0x6f, 0x73, 0x74,
		0x30, 0x0c, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02, 0x01, 0x01, 0x02,
		0x00, 0x05, 0x00, 0x30, 0x0e, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02,
		0x01, 0x01, 0x03, 0x00, 0x43, 0x02, 0x2b, 0x67,
	}
	buf, err := pdu.Marshal()
	if err != nil {
		t.Fatal("Marshal() : %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	expStr := `{"Type": "GetRequest", "RequestId": "123", ` +
		`"ErrorStatus": "TooBig", "ErrorIndex": "2", "VarBinds": [` +
		`{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "OctetString", "Value": "MyHost"}}, ` +
		`{"Oid": "1.3.6.1.2.1.1.2.0", "Variable": {"Type": "Null", "Value": ""}}, ` +
		`{"Oid": "1.3.6.1.2.1.1.3.0", "Variable": {"Type": "TimeTicks", "Value": "11111"}}]}`
	var w snmpgo.PduV1
	rest, err := (&w).Unmarshal(buf)
	if len(rest) != 0 || err != nil {
		t.Errorf("Unmarshal() - len[%d] err[%v]", len(rest), err)
	}
	if expStr != w.String() {
		t.Errorf("Unmarshal() - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestPduV1LargeObjectIdentifier(t *testing.T) {
	buf := []byte{
		0xa2, 0x30, 0x02, 0x04, 0x34, 0x99, 0x23, 0xc4,
		0x02, 0x01, 0x00, 0x02, 0x01, 0x00, 0x30, 0x22,
		0x30, 0x20, 0x06, 0x0f, 0x2b, 0x06, 0x01, 0x02,
		0x01, 0x1f, 0x01, 0x01, 0x01, 0x01, 0x84, 0x88,
		0x90, 0x80, 0x23, 0x04, 0x0d, 0x45, 0x74, 0x68,
		0x65, 0x72, 0x6e, 0x65, 0x74, 0x33, 0x35, 0x2e,
		0x36, 0x35,
	}

	expStr := `{"Type": "GetResponse", "RequestId": "882451396", ` +
		`"ErrorStatus": "NoError", "ErrorIndex": "0", "VarBinds": [` +
		`{"Oid": "1.3.6.1.2.1.31.1.1.1.1.1090781219", "Variable": {` +
		`"Type": "OctetString", "Value": "Ethernet35.65"}}]}`

	var w snmpgo.PduV1
	rest, err := (&w).Unmarshal(buf)
	if len(rest) != 0 || err != nil {
		t.Errorf("Unmarshal() - len[%d] err[%v]", len(rest), err)
	}
	if expStr != w.String() {
		t.Errorf("Unmarshal() - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestScopedPdu(t *testing.T) {
	pdu := snmpgo.NewPdu(snmpgo.V3, snmpgo.GetRequest)
	pdu.SetRequestId(123)
	pdu.SetErrorStatus(snmpgo.TooBig)
	pdu.SetErrorIndex(2)

	sp := pdu.(*snmpgo.ScopedPdu)
	sp.ContextEngineId = []byte{0x80, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}
	sp.ContextName = []byte("MyContext")

	oid, _ := snmpgo.NewOid("1.3.6.1.2.1.1.1.0")
	pdu.AppendVarBind(oid, snmpgo.NewOctetString([]byte("MyHost")))
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.2.0")
	pdu.AppendVarBind(oid, snmpgo.NewNull())
	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.1.3.0")
	pdu.AppendVarBind(oid, snmpgo.NewTimeTicks(uint32(11111)))

	expBuf := []byte{
		0x30, 0x54, 0x04, 0x08, 0x80, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x04, 0x09, 0x4d, 0x79, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74,
		0xa0, 0x3d, 0x02, 0x01, 0x7b, 0x02, 0x01, 0x01, 0x02, 0x01, 0x02,
		0x30, 0x32, 0x30, 0x12, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02, 0x01,
		0x01, 0x01, 0x00, 0x04, 0x06, 0x4d, 0x79, 0x48, 0x6f, 0x73, 0x74,
		0x30, 0x0c, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02, 0x01, 0x01, 0x02,
		0x00, 0x05, 0x00, 0x30, 0x0e, 0x06, 0x08, 0x2b, 0x06, 0x01, 0x02,
		0x01, 0x01, 0x03, 0x00, 0x43, 0x02, 0x2b, 0x67,
	}
	buf, err := pdu.Marshal()
	if err != nil {
		t.Fatal("Marshal() : %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	expStr := `{"Type": "GetRequest", "RequestId": "123", ` +
		`"ErrorStatus": "TooBig", "ErrorIndex": "2", ` +
		`"ContextEngineId": "8001020304050607", "ContextName": "MyContext", ` +
		`"VarBinds": [` +
		`{"Oid": "1.3.6.1.2.1.1.1.0", "Variable": {"Type": "OctetString", "Value": "MyHost"}}, ` +
		`{"Oid": "1.3.6.1.2.1.1.2.0", "Variable": {"Type": "Null", "Value": ""}}, ` +
		`{"Oid": "1.3.6.1.2.1.1.3.0", "Variable": {"Type": "TimeTicks", "Value": "11111"}}]}`
	var w snmpgo.ScopedPdu
	rest, err := (&w).Unmarshal(buf)
	if len(rest) != 0 || err != nil {
		t.Errorf("Unmarshal() - len[%d] err[%v]", len(rest), err)
	}
	if expStr != w.String() {
		t.Errorf("Unmarshal() - expected [%s], actual [%s]", expStr, w.String())
	}
}
