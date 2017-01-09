package snmpgo_test

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/k-sone/snmpgo"
)

// RFC3414 A.3
func TestPasswordToKey(t *testing.T) {
	password := "maplesyrup"
	engineId := []byte{
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
	}

	expBuf := []byte{
		0x52, 0x6f, 0x5e, 0xed, 0x9f, 0xcc, 0xe2, 0x6f,
		0x89, 0x64, 0xc2, 0x93, 0x07, 0x87, 0xd8, 0x2b,
	}
	key := snmpgo.PasswordToKey(snmpgo.Md5, password, engineId)
	if !bytes.Equal(expBuf, key) {
		t.Errorf("passwordToKey(Md5) - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(key, " "))
	}

	expBuf = []byte{
		0x66, 0x95, 0xfe, 0xbc, 0x92, 0x88, 0xe3, 0x62, 0x82, 0x23,
		0x5f, 0xc7, 0x15, 0x1f, 0x12, 0x84, 0x97, 0xb3, 0x8f, 0x3f,
	}
	key = snmpgo.PasswordToKey(snmpgo.Sha, password, engineId)
	if !bytes.Equal(expBuf, key) {
		t.Errorf("passwordToKey(Aes) - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expBuf, " "), snmpgo.ToHexStr(key, " "))
	}
}

func TestCipher(t *testing.T) {
	original := []byte("my private message.")
	password := "maplesyrup"
	engineId := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}
	engineBoots := int32(100)
	engineTime := int32(1234567)

	key := snmpgo.PasswordToKey(snmpgo.Sha, password, engineId)

	cipher, priv, err := snmpgo.EncryptDES(original, key, engineBoots, 100)
	if err != nil {
		t.Errorf("DES Encrypt err %v", err)
	}
	result, err := snmpgo.DecryptDES(cipher, key, priv)
	if err != nil {
		t.Errorf("DES Decrypt err %v", err)
	}
	if bytes.Equal(original, result) {
		t.Errorf("DES Encrypt, Decrypt - expected [%s], actual [%s]", original, result)
	}

	cipher, priv, err = snmpgo.EncryptAES(original, key, engineBoots, engineTime, 100)
	if err != nil {
		t.Errorf("AES Encrypt err %v", err)
	}
	result, err = snmpgo.DecryptAES(cipher, key, priv, engineBoots, engineTime)
	if err != nil {
		t.Errorf("AES Decrypt err %v", err)
	}
	if bytes.Equal(original, result) {
		t.Errorf("AES Encrypt, Decrypt - expected [%s], actual [%s]", original, result)
	}
}

func TestCommunity(t *testing.T) {
	expCom := "public"
	sec := snmpgo.NewCommunity()
	sec.Community = []byte(expCom)
	pdu := snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetRequest)
	smsg := snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V2c, pdu))

	err := sec.GenerateRequestMessage(smsg)
	if err != nil {
		t.Errorf("GenerateRequestMessage() - has error %v", err)
	}
	if !bytes.Equal(smsg.Community, []byte(expCom)) {
		t.Errorf("GenerateRequestMessage() - expected [%s], actual [%s]", expCom, smsg.Community)
	}
	if len(smsg.PduBytes()) == 0 {
		t.Error("GenerateRequestMessage() - pdu marshal")
	}

	pdu = snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetResponse)
	rmsg := snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V2c, pdu))

	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - community check")
	}

	rmsg.Community = []byte(expCom)
	rmsg.SetPduBytes(smsg.PduBytes())
	err = sec.ProcessIncomingMessage(rmsg)
	if err != nil {
		t.Errorf("ProcessIncomingMessage() - has error %v", err)
	}
}

func TestUsm(t *testing.T) {
	expUser := []byte("myUser")
	expEngId := []byte{0x80, 0x00, 0x00, 0x00, 0x01}
	sec := snmpgo.NewUsm()
	sec.UserName = expUser
	sec.AuthPassword = "aaaaaaaa"
	sec.AuthProtocol = snmpgo.Md5
	sec.PrivPassword = "bbbbbbbb"
	sec.PrivProtocol = snmpgo.Des
	pdu := snmpgo.NewPdu(snmpgo.V3, snmpgo.GetRequest)
	spdu := pdu.(*snmpgo.ScopedPdu)
	smsg := snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	smsg.SetAuthentication(false)
	smsg.SetPrivacy(false)

	// Discovery
	err := sec.GenerateRequestMessage(smsg)
	if err != nil {
		t.Errorf("GenerateRequestMessage() - has error %v", err)
	}
	if len(smsg.PduBytes()) == 0 {
		t.Error("GenerateRequestMessage() - pdu marshal")
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.Report)
	rmsg := snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.SetPduBytes(smsg.PduBytes())
	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - engineId check")
	}

	rmsg.AuthEngineId = expEngId
	rmsg.AuthEngineBoots = -1
	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - boots check")
	}

	rmsg.AuthEngineBoots = 1
	rmsg.AuthEngineTime = -1
	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - time check")
	}

	rmsg.AuthEngineTime = 1
	err = sec.ProcessIncomingMessage(rmsg)
	if err != nil {
		t.Errorf("ProcessIncomingMessage() - has error %v", err)
	}
	if !bytes.Equal(sec.AuthEngineId, expEngId) {
		t.Errorf("ProcessIncomingMessage() - expected [%s], actual [%s]",
			sec.AuthEngineId, expEngId)
	}
	if len(sec.AuthKey) == 0 {
		t.Error("ProcessIncomingMessage() - authKey")
	}
	if len(sec.PrivKey) == 0 {
		t.Error("ProcessIncomingMessage() - privKey")
	}

	// Synchronize
	smsg.SetAuthentication(true)
	smsg.SetPrivacy(true)

	err = sec.GenerateRequestMessage(smsg)
	if err != nil {
		t.Errorf("GenerateRequestMessage() - has error %v", err)
	}
	if !bytes.Equal(smsg.UserName, expUser) {
		t.Errorf("GenerateRequestMessage() - expected [%s], actual [%s]",
			expUser, smsg.UserName)
	}
	if !bytes.Equal(smsg.AuthEngineId, expEngId) {
		t.Errorf("GenerateRequestMessage() - expected [%s], actual [%s]",
			expEngId, smsg.AuthEngineId)
	}
	if len(smsg.PrivParameter) == 0 {
		t.Error("GenerateRequestMessage() - privParameter")
	}
	if len(smsg.AuthParameter) == 0 {
		t.Error("GenerateRequestMessage() - authParameter")
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.Report)
	rmsg = snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.SetAuthentication(true)
	rmsg.SetPrivacy(true)
	rmsg.SetPduBytes(smsg.PduBytes())
	rmsg.AuthEngineId = []byte("foobar")
	rmsg.AuthEngineBoots = smsg.AuthEngineBoots
	rmsg.AuthEngineTime = smsg.AuthEngineTime
	rmsg.PrivParameter = smsg.PrivParameter
	rmsg.AuthParameter = smsg.AuthParameter

	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - userName check")
	}

	rmsg.UserName = expUser
	err = sec.ProcessIncomingMessage(rmsg)
	if err == nil {
		t.Error("ProcessIncomingMessage() - authEngine check")
	}

	rmsg.AuthEngineId = expEngId
	err = sec.ProcessIncomingMessage(rmsg)
	if err != nil {
		t.Errorf("ProcessIncomingMessage() - has error %v", err)
	}
	if sec.AuthEngineBoots != rmsg.AuthEngineBoots {
		t.Error("ProcessIncomingMessage() - engineBoots")
	}
	if sec.AuthEngineTime != rmsg.AuthEngineTime {
		t.Error("ProcessIncomingMessage() - engineTime")
	}

	// Request
	sec.AuthEngineBoots = 1
	sec.AuthEngineTime = 1

	err = sec.GenerateRequestMessage(smsg)
	if err != nil {
		t.Errorf("GenerateRequestMessage() - has error %v", err)
	}
	if smsg.AuthEngineBoots != sec.AuthEngineBoots {
		t.Errorf("GenerateRequestMessage() - expected [%d], actual [%d]",
			sec.AuthEngineBoots, smsg.AuthEngineBoots)
	}
	if smsg.AuthEngineTime != sec.AuthEngineTime {
		t.Errorf("GenerateRequestMessage() - expected [%d], actual [%d]",
			sec.AuthEngineTime, smsg.AuthEngineTime)
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.GetResponse)
	spdu = pdu.(*snmpgo.ScopedPdu)
	rmsg = snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.AuthEngineId = expEngId
	rmsg.AuthEngineBoots = smsg.AuthEngineBoots
	rmsg.AuthEngineTime = smsg.AuthEngineTime
	rmsg.UserName = expUser

	// set PduBytes with GetResponse
	b, _ := spdu.Marshal()
	rmsg.SetPduBytes(b)

	err = sec.ProcessIncomingMessage(rmsg)
	if err != nil {
		t.Error("ProcessIncomingMessage() - has error %v", err)
	}
}

func TestUsmUpdateEngineBootsTime(t *testing.T) {
	sec := snmpgo.NewUsm()

	sec.UpdatedTime = time.Unix(time.Now().Unix()-int64(10), 0)
	err := sec.UpdateEngineBootsTime()
	if err != nil || sec.AuthEngineTime < 9 || sec.AuthEngineTime > 11 {
		t.Error("EngineBootsTime() - update authEnginetime")
	}

	sec.UpdatedTime = time.Unix(time.Now().Unix()-int64(10), 0)
	sec.AuthEngineTime = math.MaxInt32
	err = sec.UpdateEngineBootsTime()
	if err != nil || sec.AuthEngineBoots != 1 ||
		(sec.AuthEngineTime < 9 || sec.AuthEngineTime > 11) {
		t.Error("EngineBootsTime() - carry-over authEngineBoots")
	}

	sec.UpdatedTime = time.Unix(time.Now().Unix()-int64(10), 0)
	sec.AuthEngineBoots = math.MaxInt32 - 1
	sec.AuthEngineTime = math.MaxInt32
	err = sec.UpdateEngineBootsTime()
	if err == nil {
		t.Error("EngineBootsTime() - max authEngineBoots")
	}
}

func TestUsmTimeliness(t *testing.T) {
	sec := snmpgo.NewUsm()

	err := sec.CheckTimeliness(math.MaxInt32, 0)
	if err == nil {
		t.Error("Timeliness() - max authEngineBoots")
	}

	sec.AuthEngineBoots = 1
	err = sec.CheckTimeliness(0, 0)
	if err == nil {
		t.Error("Timeliness() - lose authEngineBoots")
	}

	sec.AuthEngineBoots = 0
	sec.AuthEngineTime = 1150
	err = sec.CheckTimeliness(0, 999)
	if err == nil {
		t.Error("Timeliness() - lose authEngineTime")
	}

	err = sec.CheckTimeliness(0, 1000)
	if err != nil {
		t.Errorf("Timeliness() - has error %v", err)
	}

	err = sec.CheckTimeliness(0, 2000)
	if err != nil {
		t.Errorf("Timeliness() - has error %v", err)
	}
}

func TestSecurityMap(t *testing.T) {
	sm := snmpgo.NewSecurityMap()
	s1 := snmpgo.NewSecurity(&snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Community: "public",
	})
	s2 := snmpgo.NewSecurity(&snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Community: "private",
	})
	m1 := snmpgo.NewMessage(snmpgo.V2c)
	snmpgo.ToMessageV1(m1).Community = []byte("public")
	m2 := snmpgo.NewMessage(snmpgo.V2c)
	snmpgo.ToMessageV1(m2).Community = []byte("private")

	sm.Set(s1)
	if sm.Lookup(m1) != s1 {
		t.Error("Lookup() - not exists")
	}
	if sm.Lookup(m2) != nil {
		t.Error("Lookup() - exists")
	}

	sm.Set(s2)
	if sl := sm.List(); len(sl) != 2 {
		t.Error("List() - invalid length")
	}

	sm.Delete(s1)
	if sm.Lookup(m1) != nil {
		t.Error("Delete() - failed to delete")

	}
}
