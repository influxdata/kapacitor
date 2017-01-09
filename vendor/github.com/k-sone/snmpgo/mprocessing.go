package snmpgo

import (
	"bytes"
	"fmt"
)

type messageProcessing interface {
	Version() SNMPVersion
	PrepareOutgoingMessage(security, Pdu, *SNMPArguments) (message, error)
	PrepareResponseMessage(security, Pdu, message) (message, error)
	PrepareDataElements(security, message, message) (Pdu, error)
}

type messageProcessingV1 struct {
	version SNMPVersion
}

func (mp *messageProcessingV1) Version() SNMPVersion {
	return mp.version
}

func (mp *messageProcessingV1) PrepareOutgoingMessage(
	sec security, pdu Pdu, args *SNMPArguments) (message, error) {

	_, ok := pdu.(*PduV1)
	if !ok {
		return nil, &ArgumentError{
			Value:   pdu,
			Message: "Type of Pdu is not PduV1",
		}
	}
	pdu.SetRequestId(genRequestId())
	msg := newMessageWithPdu(mp.Version(), pdu)

	if err := sec.GenerateRequestMessage(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (mp *messageProcessingV1) PrepareResponseMessage(
	sec security, pdu Pdu, recvMsg message) (message, error) {

	_, ok := pdu.(*PduV1)
	if !ok {
		return nil, &ArgumentError{
			Value:   pdu,
			Message: "Type of Pdu is not PduV1",
		}
	}
	pdu.SetRequestId(recvMsg.Pdu().RequestId())
	msg := newMessageWithPdu(mp.Version(), pdu)

	if err := sec.GenerateResponseMessage(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (mp *messageProcessingV1) PrepareDataElements(
	sec security, recvMsg, sendMsg message) (Pdu, error) {

	if sendMsg != nil && sendMsg.Version() != recvMsg.Version() {
		return nil, &MessageError{
			Message: fmt.Sprintf(
				"SNMPVersion mismatch - expected [%v], actual [%v]",
				sendMsg.Version(), recvMsg.Version()),
			Detail: fmt.Sprintf("%s vs %s", sendMsg, recvMsg),
		}
	}

	if err := sec.ProcessIncomingMessage(recvMsg); err != nil {
		return nil, err
	}

	pdu := recvMsg.Pdu()
	if sendMsg != nil {
		if pdu.PduType() != GetResponse {
			return nil, &MessageError{
				Message: fmt.Sprintf("Illegal PduType - expected [%s], actual [%v]",
					GetResponse, pdu.PduType()),
			}
		}
		if sendMsg.Pdu().RequestId() != pdu.RequestId() {
			return nil, &MessageError{
				Message: fmt.Sprintf("RequestId mismatch - expected [%d], actual [%d]",
					sendMsg.Pdu().RequestId(), pdu.RequestId()),
				Detail: fmt.Sprintf("%s vs %s", sendMsg, recvMsg),
			}
		}
	} else {
		if t := pdu.PduType(); !confirmedType(t) && t != SNMPTrapV2 {
			return nil, &MessageError{
				Message: fmt.Sprintf("Illegal PduType - received [%v]", t),
			}
		}
	}

	return pdu, nil
}

type messageProcessingV3 struct {
	version SNMPVersion
}

func (mp *messageProcessingV3) Version() SNMPVersion {
	return mp.version
}

func (mp *messageProcessingV3) PrepareOutgoingMessage(
	sec security, pdu Pdu, args *SNMPArguments) (message, error) {

	p, ok := pdu.(*ScopedPdu)
	if !ok {
		return nil, &ArgumentError{
			Value:   pdu,
			Message: "Type of Pdu is not ScopedPdu",
		}
	}
	p.SetRequestId(genRequestId())
	if args.ContextEngineId != "" {
		p.ContextEngineId, _ = engineIdToBytes(args.ContextEngineId)
	} else {
		p.ContextEngineId = sec.(*usm).AuthEngineId
	}
	if args.ContextName != "" {
		p.ContextName = []byte(args.ContextName)
	}

	msg := newMessageWithPdu(mp.Version(), pdu)
	m := msg.(*messageV3)
	m.MessageId = genMessageId()
	m.MessageMaxSize = args.MessageMaxSize
	m.SecurityModel = securityUsm
	m.SetReportable(confirmedType(pdu.PduType()))
	if args.SecurityLevel >= AuthNoPriv {
		m.SetAuthentication(true)
		if args.SecurityLevel >= AuthPriv {
			m.SetPrivacy(true)
		}
	}
	// set boots & time from arguments
	// (there is possibility to be overwritten with the usm)
	m.AuthEngineBoots = int64(args.authEngineBoots)
	m.AuthEngineTime = int64(args.authEngineTime)

	if err := sec.GenerateRequestMessage(msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (mp *messageProcessingV3) PrepareResponseMessage(
	sec security, pdu Pdu, recvMsg message) (message, error) {

	// TODO support for response message of v3
	return nil, nil
}

func (mp *messageProcessingV3) PrepareDataElements(
	sec security, recvMsg, sendMsg message) (Pdu, error) {

	sm, _ := sendMsg.(*messageV3)
	rm := recvMsg.(*messageV3)
	if sm != nil {
		if sm.Version() != rm.Version() {
			return nil, &MessageError{
				Message: fmt.Sprintf(
					"SNMPVersion mismatch - expected [%v], actual [%v]",
					sm.Version(), rm.Version()),
				Detail: fmt.Sprintf("%s vs %s", sm, rm),
			}
		}
		if sm.MessageId != rm.MessageId {
			return nil, &MessageError{
				Message: fmt.Sprintf(
					"MessageId mismatch - expected [%d], actual [%d]",
					sm.MessageId, rm.MessageId),
				Detail: fmt.Sprintf("%s vs %s", sm, rm),
			}
		}
	}
	if rm.SecurityModel != securityUsm {
		return nil, &MessageError{
			Message: fmt.Sprintf("Unknown SecurityModel, value [%d]", rm.SecurityModel),
		}
	}

	if err := sec.ProcessIncomingMessage(recvMsg); err != nil {
		return nil, err
	}

	pdu, _ := recvMsg.Pdu().(*ScopedPdu)
	if sm != nil {
		switch t := pdu.PduType(); t {
		case GetResponse:
			if sm.Pdu().RequestId() != pdu.RequestId() {
				return nil, &MessageError{
					Message: fmt.Sprintf("RequestId mismatch - expected [%d], actual [%d]",
						sm.Pdu().RequestId(), pdu.RequestId()),
					Detail: fmt.Sprintf("%s vs %s", sm, rm),
				}
			}

			sPdu := sm.Pdu().(*ScopedPdu)
			if !bytes.Equal(sPdu.ContextEngineId, pdu.ContextEngineId) {
				return nil, &MessageError{
					Message: fmt.Sprintf("ContextEngineId mismatch - expected [%s], actual [%s]",
						toHexStr(sPdu.ContextEngineId, ""), toHexStr(pdu.ContextEngineId, "")),
				}
			}

			if !bytes.Equal(sPdu.ContextName, pdu.ContextName) {
				return nil, &MessageError{
					Message: fmt.Sprintf("ContextName mismatch - expected [%s], actual [%s]",
						toHexStr(sPdu.ContextName, ""), toHexStr(pdu.ContextName, "")),
				}
			}

			if sm.Authentication() && !rm.Authentication() {
				return nil, &MessageError{
					Message: "Response message is not authenticated",
				}
			}
		case Report:
			if sm.Reportable() {
				break
			}
			fallthrough
		default:
			return nil, &MessageError{
				Message: fmt.Sprintf("Illegal PduType - expected [%s], actual [%v]",
					GetResponse, t),
			}
		}
	} else {
		if t := pdu.PduType(); !confirmedType(t) && t != SNMPTrapV2 {
			return nil, &MessageError{
				Message: fmt.Sprintf("Illegal PduType - received [%v]", t),
			}
		}
	}

	return pdu, nil
}

func newMessageProcessing(ver SNMPVersion) (mp messageProcessing) {
	switch ver {
	case V1, V2c:
		mp = &messageProcessingV1{version: ver}
	case V3:
		mp = &messageProcessingV3{version: ver}
	}
	return
}
