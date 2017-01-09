package snmpgo_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/k-sone/snmpgo"
)

func TestMessageProcessingV1Request(t *testing.T) {
	args := &snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Community: "public",
	}
	mp := snmpgo.NewMessageProcessing(args.Version)
	sec := snmpgo.NewSecurity(args)
	pdu := snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetRequest)

	msg, err := mp.PrepareOutgoingMessage(sec, pdu, args)
	if err != nil {
		t.Errorf("PrepareOutgoingMessage() - has error %v", err)
	}
	if len(msg.PduBytes()) == 0 {
		t.Error("PrepareOutgoingMessage() - pdu bytes")
	}
	if pdu.RequestId() == 0 {
		t.Error("PrepareOutgoingMessage() - request id")
	}
	requestId := pdu.RequestId()

	_, err = mp.PrepareDataElements(sec, msg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - pdu type check")
	}

	pdu = snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetResponse)
	rmsg := snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V1, pdu))
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - version check")
	}

	pdu.SetRequestId(requestId)
	pduBytes, _ := pdu.Marshal()
	rmsg = snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V2c, pdu))
	rmsg.Community = []byte("public")
	rmsg.SetPduBytes(pduBytes)
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err != nil {
		t.Errorf("PrepareDataElements() - has error %v", err)
	}
}

func TestMessageProcessingV1Receive(t *testing.T) {
	args := &snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Community: "public",
	}
	mp := snmpgo.NewMessageProcessing(args.Version)
	sec := snmpgo.NewSecurity(args)

	pdu := snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetResponse)
	pduBytes, _ := pdu.Marshal()
	rmsg := snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V2c, pdu))
	rmsg.Community = []byte("public")
	rmsg.SetPduBytes(pduBytes)
	_, err := mp.PrepareDataElements(sec, rmsg, nil)
	if err == nil {
		t.Error("PrepareDataElements() - pdu type check")
	}

	pdu = snmpgo.NewPdu(snmpgo.V2c, snmpgo.SNMPTrapV2)
	pduBytes, _ = pdu.Marshal()
	rmsg = snmpgo.ToMessageV1(snmpgo.NewMessageWithPdu(snmpgo.V2c, pdu))
	rmsg.Community = []byte("public")
	rmsg.SetPduBytes(pduBytes)
	_, err = mp.PrepareDataElements(sec, rmsg, nil)
	if err != nil {
		t.Errorf("PrepareDataElements() - has error %v", err)
	}

	pdu = snmpgo.NewPdu(snmpgo.V2c, snmpgo.GetResponse)
	pdu.SetRequestId(-1)
	smsg, err := mp.PrepareResponseMessage(sec, pdu, rmsg)
	if err != nil {
		t.Errorf("PrepareResponseMessage() - has error %v", err)
	}
	if len(smsg.PduBytes()) == 0 {
		t.Error("PrepareResponseMessage() - pdu bytes")
	}
	if pdu.RequestId() != rmsg.Pdu().RequestId() {
		t.Error("PrepareResponseMessage() - request id")
	}
}

func TestMessageProcessingV3Request(t *testing.T) {
	expEngId := []byte{0x80, 0x00, 0x00, 0x00, 0x01}
	expCtxId := []byte{0x80, 0x00, 0x00, 0x00, 0x05}
	expCtxName := "myName"
	args := &snmpgo.SNMPArguments{
		Version:         snmpgo.V3,
		UserName:        "myName",
		SecurityLevel:   snmpgo.AuthPriv,
		AuthPassword:    "aaaaaaaa",
		AuthProtocol:    snmpgo.Md5,
		PrivPassword:    "bbbbbbbb",
		PrivProtocol:    snmpgo.Des,
		ContextEngineId: hex.EncodeToString(expCtxId),
		ContextName:     expCtxName,
	}
	mp := snmpgo.NewMessageProcessing(args.Version)
	sec := snmpgo.NewSecurity(args)
	usm := snmpgo.ToUsm(sec)
	usm.AuthEngineId = expEngId
	usm.AuthKey = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	usm.PrivKey = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	pdu := snmpgo.NewPdu(snmpgo.V3, snmpgo.GetRequest)

	msg, err := mp.PrepareOutgoingMessage(sec, pdu, args)
	if err != nil {
		t.Errorf("PrepareOutgoingMessage() - has error %v", err)
	}
	if len(msg.PduBytes()) == 0 {
		t.Error("PrepareOutgoingMessage() - pdu bytes")
	}
	p := pdu.(*snmpgo.ScopedPdu)
	if p.RequestId() == 0 {
		t.Error("PrepareOutgoingMessage() - request id")
	}
	if !bytes.Equal(p.ContextEngineId, expCtxId) {
		t.Errorf("PrepareOutgoingMessage() - expected [%s], actual [%s]",
			snmpgo.ToHexStr(expCtxId, ""), snmpgo.ToHexStr(p.ContextEngineId, ""))
	}
	if string(p.ContextName) != expCtxName {
		t.Errorf("GenerateRequestMessage() - expected [%s], actual [%s]",
			expCtxName, string(p.ContextName))
	}
	msgv3 := snmpgo.ToMessageV3(msg)
	if msgv3.MessageId == 0 {
		t.Error("PrepareOutgoingMessage() - message id")
	}
	if !msgv3.Reportable() || !msgv3.Authentication() || !msgv3.Privacy() {
		t.Error("PrepareOutgoingMessage() - security flag")
	}
	msgv3.SetAuthentication(false)
	msgv3.SetPrivacy(false)
	msgv3.AuthEngineId = []byte{0, 0, 0, 0, 0}
	requestId := pdu.RequestId()
	messageId := msgv3.MessageId

	_, err = mp.PrepareDataElements(sec, msg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - pdu type check")
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.GetResponse)
	rmsg := snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.AuthEngineId = []byte{0, 0, 0, 0, 0}
	rmsg.UserName = []byte("myName")
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - message id check")
	}

	rmsg.MessageId = messageId
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - security model check")
	}

	pduBytes, _ := pdu.Marshal()
	rmsg.SetPduBytes(pduBytes)
	rmsg.SecurityModel = 3
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Error("PrepareDataElements() - request id check")
	}

	pdu.SetRequestId(requestId)
	pduBytes, _ = pdu.Marshal()
	rmsg.SetPduBytes(pduBytes)
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Errorf("PrepareDataElements() - contextEngineId check")
	}

	pdu.(*snmpgo.ScopedPdu).ContextEngineId = expCtxId
	pduBytes, _ = pdu.Marshal()
	rmsg.SetPduBytes(pduBytes)
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Errorf("PrepareDataElements() - contextName check")
	}

	pdu.(*snmpgo.ScopedPdu).ContextName = []byte(expCtxName)
	pduBytes, _ = pdu.Marshal()
	rmsg.SetPduBytes(pduBytes)

	msgv3.SetAuthentication(true)
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err == nil {
		t.Errorf("PrepareDataElements() - response authenticate check")
	}

	msgv3.SetAuthentication(false)
	_, err = mp.PrepareDataElements(sec, rmsg, msg)
	if err != nil {
		t.Errorf("PrepareDataElements() - has error %v", err)
	}
}

func TestMessageProcessingV3Receive(t *testing.T) {
	secEngId := []byte{0x80, 0x00, 0x00, 0x00, 0x01}
	args := &snmpgo.SNMPArguments{
		Version:          snmpgo.V3,
		UserName:         "myName",
		SecurityEngineId: hex.EncodeToString(secEngId),
	}
	mp := snmpgo.NewMessageProcessing(args.Version)
	sec := snmpgo.NewSecurity(args)
	usm := snmpgo.ToUsm(sec)
	usm.SetAuthEngineId(secEngId)
	usm.DiscoveryStatus = 3 // remoteReference

	pdu := snmpgo.NewPdu(snmpgo.V3, snmpgo.GetResponse)
	pduBytes, _ := pdu.Marshal()
	rmsg := snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.AuthEngineId = secEngId
	rmsg.UserName = []byte("myName")
	rmsg.SecurityModel = 3 // securityUsm
	rmsg.SetPduBytes(pduBytes)
	_, err := mp.PrepareDataElements(sec, rmsg, nil)
	if err == nil {
		t.Error("PrepareDataElements() - pdu type check")
	}

	pdu = snmpgo.NewPdu(snmpgo.V3, snmpgo.SNMPTrapV2)
	pduBytes, _ = pdu.Marshal()
	rmsg = snmpgo.ToMessageV3(snmpgo.NewMessageWithPdu(snmpgo.V3, pdu))
	rmsg.AuthEngineId = secEngId
	rmsg.UserName = []byte("myName")
	rmsg.SecurityModel = 3 // securityUsm
	rmsg.SetPduBytes(pduBytes)
	_, err = mp.PrepareDataElements(sec, rmsg, nil)
	if err != nil {
		t.Errorf("PrepareDataElements() - has error %v", err)
	}
}
