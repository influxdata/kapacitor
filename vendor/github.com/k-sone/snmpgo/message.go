package snmpgo

import (
	"encoding/asn1"
	"fmt"

	"github.com/geoffgarside/ber"
)

type message interface {
	Version() SNMPVersion
	Pdu() Pdu
	PduBytes() []byte
	SetPduBytes([]byte)
	Marshal() ([]byte, error)
	Unmarshal([]byte) ([]byte, error)
	String() string
}

type messageV1 struct {
	version   SNMPVersion
	Community []byte
	pduBytes  []byte
	pdu       Pdu
}

func (msg *messageV1) Version() SNMPVersion {
	return msg.version
}

func (msg *messageV1) Pdu() Pdu {
	return msg.pdu
}

func (msg *messageV1) PduBytes() []byte {
	return msg.pduBytes
}

func (msg *messageV1) SetPduBytes(b []byte) {
	msg.pduBytes = b
}

func (msg *messageV1) Marshal() (b []byte, err error) {
	var buf []byte
	raw := asn1.RawValue{Class: classUniversal, Tag: tagSequence, IsCompound: true}

	buf, err = asn1.Marshal(msg.version)
	if err != nil {
		return
	}
	raw.Bytes = append(raw.Bytes, buf...)

	buf, err = asn1.Marshal(msg.Community)
	if err != nil {
		return
	}
	raw.Bytes = append(raw.Bytes, buf...)

	raw.Bytes = append(raw.Bytes, msg.pduBytes...)
	return asn1.Marshal(raw)
}

func (msg *messageV1) Unmarshal(b []byte) ([]byte, error) {
	ver, rest, next, err := unmarshalMessageVersion(b)
	if err != nil {
		return nil, err
	}

	err = msg.unmarshalInner(next)
	if err != nil {
		return nil, err
	}

	msg.version = ver
	return rest, nil
}

func (msg *messageV1) unmarshalInner(b []byte) error {
	var community []byte
	next, err := ber.Unmarshal(b, &community)
	if err != nil {
		return err
	}

	msg.Community = community
	msg.pduBytes = next
	return nil
}

func (msg *messageV1) String() string {
	return fmt.Sprintf(
		`{"Version": "%s", "Community": "%s", "Pdu": %s}`,
		msg.version, msg.Community, msg.pdu.String())
}

type globalDataV3 struct {
	MessageId      int
	MessageMaxSize int
	MessageFlags   []byte
	SecurityModel  securityModel
}

func (h *globalDataV3) Marshal() (b []byte, err error) {
	return asn1.Marshal(*h)
}

func (h *globalDataV3) Unmarshal(b []byte) (rest []byte, err error) {
	return ber.Unmarshal(b, h)
}

func (h *globalDataV3) initFlags() {
	if len(h.MessageFlags) == 0 {
		h.MessageFlags = append(h.MessageFlags, 0)
	}
}

func (h *globalDataV3) SetReportable(b bool) {
	h.initFlags()
	if b {
		h.MessageFlags[0] |= 0x04
	} else {
		h.MessageFlags[0] &= 0xfb
	}
}

func (h *globalDataV3) Reportable() bool {
	h.initFlags()
	if h.MessageFlags[0]&0x04 == 0 {
		return false
	}
	return true
}

func (h *globalDataV3) SetPrivacy(b bool) {
	h.initFlags()
	if b {
		h.MessageFlags[0] |= 0x02
	} else {
		h.MessageFlags[0] &= 0xfd
	}
}

func (h *globalDataV3) Privacy() bool {
	h.initFlags()
	if h.MessageFlags[0]&0x02 == 0 {
		return false
	}
	return true
}

func (h *globalDataV3) SetAuthentication(b bool) {
	h.initFlags()
	if b {
		h.MessageFlags[0] |= 0x01
	} else {
		h.MessageFlags[0] &= 0xfe
	}
}

func (h *globalDataV3) Authentication() bool {
	h.initFlags()
	if h.MessageFlags[0]&0x01 == 0 {
		return false
	}
	return true
}

func (h *globalDataV3) String() string {
	var flags string
	if h.Authentication() {
		flags += "a"
	}
	if h.Privacy() {
		flags += "p"
	}
	if h.Reportable() {
		flags += "r"
	}

	return fmt.Sprintf(
		`{"MessageId": "%d", "MessageMaxSize": "%d", "MessageFlags": "%s", `+
			`"SecurityModel": "%s"}`,
		h.MessageId, h.MessageMaxSize, flags, h.SecurityModel)
}

type securityParameterV3 struct {
	AuthEngineId    []byte
	AuthEngineBoots int64
	AuthEngineTime  int64
	UserName        []byte
	AuthParameter   []byte
	PrivParameter   []byte
}

func (sec *securityParameterV3) Marshal() ([]byte, error) {
	raw := asn1.RawValue{Class: classUniversal, Tag: tagOctetString, IsCompound: false}

	buf, err := asn1.Marshal(*sec)
	if err != nil {
		return nil, err
	}
	raw.Bytes = buf

	return asn1.Marshal(raw)
}

func (sec *securityParameterV3) Unmarshal(b []byte) (rest []byte, err error) {
	var raw asn1.RawValue
	rest, err = ber.Unmarshal(b, &raw)
	if err != nil {
		return
	}

	if raw.Class != classUniversal || raw.Tag != tagOctetString || raw.IsCompound {
		return nil, asn1.StructuralError{fmt.Sprintf(
			"Invalid SecurityParameter object - Class [%02x], Tag [%02x] : [%s]",
			raw.Class, raw.Tag, toHexStr(b, " "))}
	}

	_, err = ber.Unmarshal(raw.Bytes, sec)
	return
}

func (sec *securityParameterV3) String() string {
	return fmt.Sprintf(
		`{"AuthEngineId": "%s", "AuthEngineBoots": "%d", "AuthEngineTime": "%d", `+
			`"UserName": "%s", "AuthParameter": "%s", "PrivParameter": "%s"}`,
		toHexStr(sec.AuthEngineId, ""), sec.AuthEngineBoots, sec.AuthEngineTime, sec.UserName,
		toHexStr(sec.AuthParameter, ":"), toHexStr(sec.PrivParameter, ":"))
}

type messageV3 struct {
	globalDataV3
	securityParameterV3
	messageV1
}

func (msg *messageV3) Marshal() (b []byte, err error) {
	var buf []byte
	raw := asn1.RawValue{Class: classUniversal, Tag: tagSequence, IsCompound: true}

	buf, err = asn1.Marshal(msg.version)
	if err != nil {
		return
	}
	raw.Bytes = buf

	buf, err = msg.globalDataV3.Marshal()
	if err != nil {
		return
	}
	raw.Bytes = append(raw.Bytes, buf...)

	buf, err = msg.securityParameterV3.Marshal()
	if err != nil {
		return
	}
	raw.Bytes = append(raw.Bytes, buf...)

	raw.Bytes = append(raw.Bytes, msg.pduBytes...)
	return asn1.Marshal(raw)
}

func (msg *messageV3) Unmarshal(b []byte) ([]byte, error) {
	ver, rest, next, err := unmarshalMessageVersion(b)
	if err != nil {
		return nil, err
	}

	err = msg.unmarshalInner(next)
	if err != nil {
		return nil, err
	}

	msg.version = ver
	return rest, nil
}

func (msg *messageV3) unmarshalInner(b []byte) error {
	next, err := msg.globalDataV3.Unmarshal(b)
	if err != nil {
		return err
	}

	next, err = msg.securityParameterV3.Unmarshal(next)
	if err != nil {
		return err
	}

	msg.pduBytes = next
	return nil
}

func (msg *messageV3) String() string {
	return fmt.Sprintf(
		`{"Version": "%s", "GlobalData": %s, "SecurityParameter": %s, "Pdu": %s}`,
		msg.version, msg.globalDataV3.String(), msg.securityParameterV3.String(),
		msg.pdu.String())
}

func newMessage(ver SNMPVersion) (msg message) {
	switch ver {
	case V1, V2c:
		msg = newMessageWithPdu(ver, &PduV1{})
	case V3:
		msg = newMessageWithPdu(ver, &ScopedPdu{})
	}
	return
}

func newMessageWithPdu(ver SNMPVersion, pdu Pdu) (msg message) {
	m := messageV1{
		version: ver,
		pdu:     pdu,
	}
	switch ver {
	case V1, V2c:
		msg = &m
	case V3:
		msg = &messageV3{
			messageV1:    m,
			globalDataV3: globalDataV3{MessageFlags: []byte{0}},
		}
	}
	return
}

func unmarshalMessage(b []byte) (message, []byte, error) {
	ver, rest, next, err := unmarshalMessageVersion(b)
	if err != nil {
		return nil, nil, err
	}

	msg := newMessage(ver)
	switch m := msg.(type) {
	case *messageV1:
		err = m.unmarshalInner(next)
	case *messageV3:
		err = m.unmarshalInner(next)
	default:
		err = &MessageError{
			Message: fmt.Sprintf("Invalid SNMP Version - %v(%d)", ver, ver),
		}
	}
	if err != nil {
		return nil, nil, err
	}

	return msg, rest, nil
}

func unmarshalMessageVersion(b []byte) (SNMPVersion, []byte, []byte, error) {

	var raw asn1.RawValue
	rest, err := ber.Unmarshal(b, &raw)
	if err != nil {
		return 0, nil, nil, err
	}
	if raw.Class != classUniversal || raw.Tag != tagSequence || !raw.IsCompound {
		return 0, nil, nil, asn1.StructuralError{fmt.Sprintf(
			"Invalid message object - Class [%02x], Tag [%02x] : [%s]",
			raw.Class, raw.Tag, toHexStr(b, " "))}
	}

	var version int
	next, err := ber.Unmarshal(raw.Bytes, &version)
	if err != nil {
		return 0, nil, nil, err
	}

	return SNMPVersion(version), rest, next, nil
}
