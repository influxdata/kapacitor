package snmpgo_test

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/k-sone/snmpgo"
)

func TestInteger(t *testing.T) {
	expInt := int64(2147483647)
	expStr := "2147483647"
	expBuf := []byte{0x02, 0x04, 0x7f, 0xff, 0xff, 0xff}
	var v snmpgo.Variable = snmpgo.NewInteger(int32(expInt))

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Int64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual [%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Integer
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestOctetString(t *testing.T) {
	expStr := "Test"
	expBuf := []byte{0x04, 0x04, 0x54, 0x65, 0x73, 0x74}
	var v snmpgo.Variable = snmpgo.NewOctetString([]byte(expStr))

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.OctetString
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestOctetString2(t *testing.T) {
	expStr := "\t\n\v\f\r !\"#$%&'()*+,-./0123456789:;<=>?@ABCXYZ[\\]^_`abcxyz{|}~"
	var v snmpgo.Variable = snmpgo.NewOctetString([]byte(expStr))

	if expStr != v.String() {
		t.Errorf("String() human-readable - expected [%s], actual[%s]", expStr, v.String())
	}

	expStr = "c0:a8:01:01"
	v = snmpgo.NewOctetString([]byte{192, 168, 1, 1})
	if expStr != v.String() {
		t.Errorf("String() hex-string - expected [%s], actual[%s]", expStr, v.String())
	}

	expStr = "54:65:73:74:08"
	v = snmpgo.NewOctetString([]byte{0x54, 0x65, 0x73, 0x74, 0x08})
	if expStr != v.String() {
		t.Errorf("String() hex-string - expected [%s], actual[%s]", expStr, v.String())
	}

	expStr = "54:65:73:74:a0"
	v = snmpgo.NewOctetString([]byte{0x54, 0x65, 0x73, 0x74, 0xa0})
	if expStr != v.String() {
		t.Errorf("String() hex-string - expected [%s], actual[%s]", expStr, v.String())
	}
}

func TestNull(t *testing.T) {
	expStr := ""
	expBuf := []byte{0x05, 0x00}
	var v snmpgo.Variable = snmpgo.NewNull()

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Null
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestOid(t *testing.T) {
	expStr := "1.3.6.1.2.1.1.1.0"
	expBuf := []byte{0x06, 0x08, 0x2b, 0x06, 0x01, 0x02, 0x01, 0x01, 0x01, 0x00}
	var v snmpgo.Variable

	v, err := snmpgo.NewOid(expStr)
	if err != nil {
		t.Errorf("NewOid : %v", err)
	}

	_, err = v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Oid
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestOidOperation(t *testing.T) {
	oid, _ := snmpgo.NewOid("1.2.3.4.5.6.7")

	oids, _ := snmpgo.NewOids([]string{"1.2.3.4", "1.2.3.4.5.6.7",
		"1.2.3.4.5.6.7.8", "1.1.3.4", "1.3.3.4"})

	if !oid.Contains(oids[0]) || !oid.Contains(oids[1]) || oid.Contains(oids[2]) ||
		oid.Contains(oids[3]) || oid.Contains(oids[4]) {
		t.Errorf("Failed to Contains()")
	}

	if oid.Compare(oids[0]) != 1 || oid.Compare(oids[1]) != 0 || oid.Compare(oids[2]) != -1 ||
		oid.Compare(oids[3]) != 1 || oid.Compare(oids[4]) != -1 {
		t.Errorf("Failed to Compare()")
	}

	if oid.Equal(oids[0]) || !oid.Equal(oids[1]) || oid.Equal(oids[2]) ||
		oid.Equal(oids[3]) || oid.Equal(oids[4]) {
		t.Errorf("Failed to Contains()")
	}

	oid, _ = oid.AppendSubIds([]int{8, 9, 10})
	if oid.String() != "1.2.3.4.5.6.7.8.9.10" {
		t.Errorf("Failed to AppendSubIds()")
	}
}

func TestNewOid(t *testing.T) {
	expStr := ".1.3.6.1.2.1.1.1.0"
	var v snmpgo.Variable

	v, err := snmpgo.NewOid(expStr)
	if err != nil {
		t.Errorf("NewOid : %v", err)
	}

	if expStr[1:] != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr[1:], v.String())
	}

	var s []string
	for i := 0; i <= 128; i++ {
		s = append(s, strconv.Itoa(i))
	}
	expStr = strings.Join(s, ".")
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid sub-identifiers size")
	}

	expStr = "1.3.6.1.2.1.-1.0"
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid sub-identifier range")
	}

	expStr = "1.3.6.1.2.1.4294967296.0"
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid sub-identifier range")
	}

	expStr = "3.3.6.1.2.1.1.1.0"
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid first sub-identifier range")
	}

	expStr = "1"
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid sub-identifiers size")
	}

	expStr = "1.40.6.1.2.1.1.1.0"
	v, err = snmpgo.NewOid(expStr)
	if err == nil {
		t.Errorf("NewOid first sub-identifier range")
	}
}

func TestOids(t *testing.T) {
	oids, _ := snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.3.0",
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
	})

	expOids, _ := snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	oids = oids.Sort()
	if len(expOids) != len(oids) {
		t.Errorf("Sort() - expected [%d], actual [%d]", len(expOids), len(oids))
	}
	for i, o := range expOids {
		if !o.Equal(oids[i]) {
			t.Errorf("Sort() - expected [%s], actual [%s]", o, oids[i])
		}
	}

	expOids, _ = snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1",
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	oids = oids.Sort().Uniq()
	if len(expOids) != len(oids) {
		t.Errorf("Uniq() - expected [%d], actual [%d]", len(expOids), len(oids))
	}
	for i, o := range expOids {
		if !o.Equal(oids[i]) {
			t.Errorf("Uniq() - expected [%s], actual [%s]", o, oids[i])
		}
	}

	expOids, _ = snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1",
	})
	oids = oids.Sort().UniqBase()
	if len(expOids) != len(oids) {
		t.Errorf("Uniq() - expected [%d], actual [%d]", len(expOids), len(oids))
	}
	for i, o := range expOids {
		if !o.Equal(oids[i]) {
			t.Errorf("Uniq() - expected [%s], actual [%s]", o, oids[i])
		}
	}
}

func TestIpaddress(t *testing.T) {
	expStr := "192.168.1.1"
	expInt := int64(3232235777)
	expBuf := []byte{0x40, 0x04, 0xc0, 0xa8, 0x01, 0x01}
	var v snmpgo.Variable = snmpgo.NewIpaddress(0xc0, 0xa8, 0x01, 0x01)

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Int64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Ipaddress
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestCounter32(t *testing.T) {
	expInt := int64(4294967295)
	expStr := "4294967295"
	expBuf := []byte{0x41, 0x05, 0x00, 0xff, 0xff, 0xff, 0xff}
	var v snmpgo.Variable = snmpgo.NewCounter32(uint32(expInt))

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Int64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual [%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Counter32
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestGauge32(t *testing.T) {
	expInt := int64(4294967295)
	expStr := "4294967295"
	expBuf := []byte{0x42, 0x05, 0x00, 0xff, 0xff, 0xff, 0xff}
	var v snmpgo.Variable = snmpgo.NewGauge32(uint32(expInt))

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Int64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual [%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Gauge32
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestTimeTicks(t *testing.T) {
	expInt := int64(4294967295)
	expStr := "4294967295"
	expBuf := []byte{0x43, 0x05, 0x00, 0xff, 0xff, 0xff, 0xff}
	var v snmpgo.Variable = snmpgo.NewTimeTicks(uint32(expInt))

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Int64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual [%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.TimeTicks
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestOpaque(t *testing.T) {
	expStr := "54:65:73:74"
	expBuf := []byte{0x44, 0x04, 0x54, 0x65, 0x73, 0x74}
	var v snmpgo.Variable = snmpgo.NewOpaque(expBuf[2:])

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Opaque
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestCounter64(t *testing.T) {
	expInt := uint64(18446744073709551615)
	expStr := "18446744073709551615"
	expBuf := []byte{0x46, 0x09, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	var v snmpgo.Variable = snmpgo.NewCounter64(expInt)

	big, err := v.BigInt()
	if err != nil {
		t.Errorf("Failed to call BigInt(): %v", err)
	}
	if expInt != big.Uint64() {
		t.Errorf("BigInt() - expected [%d], actual [%d]", expInt, big.Int64())
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual [%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.Counter64
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestNoSucheObject(t *testing.T) {
	expStr := ""
	expBuf := []byte{0x80, 0x00}
	var v snmpgo.Variable = snmpgo.NewNoSucheObject()

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.NoSucheObject
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestNoSucheInstance(t *testing.T) {
	expStr := ""
	expBuf := []byte{0x81, 0x00}
	var v snmpgo.Variable = snmpgo.NewNoSucheInstance()

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.NoSucheInstance
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}

func TestEndOfMibView(t *testing.T) {
	expStr := ""
	expBuf := []byte{0x82, 0x00}
	var v snmpgo.Variable = snmpgo.NewEndOfMibView()

	_, err := v.BigInt()
	if err == nil {
		t.Errorf("Failed to call BigInt()")
	}

	if expStr != v.String() {
		t.Errorf("String() - expected [%s], actual[%s]", expStr, v.String())
	}

	buf, err := v.Marshal()
	if err != nil {
		t.Errorf("Marshal(): %v", err)
	}
	if !bytes.Equal(expBuf, buf) {
		t.Errorf("Marshal() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(buf, " "))
	}

	var w snmpgo.EndOfMibView
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
		t.Errorf("Unmarshal() with rest - expected [%s], actual [%s]", expStr, w.String())
	}
}
