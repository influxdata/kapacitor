package snmpgo

import (
	"fmt"
	"net"
	"time"
)

type snmpEngine struct {
	mp  messageProcessing
	sec security
}

func (e *snmpEngine) SendPdu(pdu Pdu, conn net.Conn, args *SNMPArguments) (result Pdu, err error) {
	size := args.MessageMaxSize
	if size < recvBufferSize {
		size = recvBufferSize
	}

	var sendMsg message
	sendMsg, err = e.mp.PrepareOutgoingMessage(e.sec, pdu, args)
	if err != nil {
		return
	}

	var buf []byte
	buf, err = sendMsg.Marshal()
	if err != nil {
		return
	}

	if err = conn.SetDeadline(time.Now().Add(args.Timeout)); err != nil {
		return
	}
	_, err = conn.Write(buf)
	if !confirmedType(pdu.PduType()) || err != nil {
		return
	}

	buf = make([]byte, size)
	_, err = conn.Read(buf)
	if err != nil {
		return
	}

	var recvMsg message
	if recvMsg, _, err = unmarshalMessage(buf); err != nil {
		return nil, &MessageError{
			Cause:   err,
			Message: "Failed to Unmarshal message",
			Detail:  fmt.Sprintf("message Bytes - [%s]", toHexStr(buf, " ")),
		}
	}

	result, err = e.mp.PrepareDataElements(e.sec, recvMsg, sendMsg)
	if result != nil && len(pdu.VarBinds()) > 0 {
		if err = e.checkPdu(result, args); err != nil {
			result = nil
		}
	}
	return
}

func (e *snmpEngine) checkPdu(pdu Pdu, args *SNMPArguments) (err error) {
	varBinds := pdu.VarBinds()
	if args.Version == V3 && pdu.PduType() == Report && len(varBinds) > 0 {
		oid := varBinds[0].Oid.String()
		rep := reportStatusOid(oid)
		err = &MessageError{
			Message: fmt.Sprintf("Received a report from the agent - %s(%s)", rep, oid),
			Detail:  fmt.Sprintf("Pdu - %s", pdu),
		}
		// perhaps the agent has rebooted after the previous communication
		if rep == usmStatsNotInTimeWindows {
			err = &notInTimeWindowError{err}
		}
	}
	return
}

func (e *snmpEngine) Discover(snmp *SNMP) error {
	return e.sec.Discover(snmp)
}

func (e *snmpEngine) String() string {
	return fmt.Sprintf(`{"sec": %s}`, e.sec.String())
}

func newSNMPEngine(args *SNMPArguments) *snmpEngine {
	return &snmpEngine{
		mp:  newMessageProcessing(args.Version),
		sec: newSecurity(args),
	}
}
