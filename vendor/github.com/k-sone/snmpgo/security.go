package snmpgo

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/asn1"
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"sync"
	"time"

	"github.com/geoffgarside/ber"
)

type security interface {
	Identifier() string
	GenerateRequestMessage(message) error
	GenerateResponseMessage(message) error
	ProcessIncomingMessage(message) error
	Discover(*SNMP) error
	String() string
}

type community struct {
	Community []byte
}

func (c *community) Identifier() string {
	return string(c.Community)
}

func (c *community) GenerateRequestMessage(sendMsg message) (err error) {
	m := sendMsg.(*messageV1)
	m.Community = c.Community

	b, err := m.Pdu().Marshal()
	if err != nil {
		return
	}
	m.SetPduBytes(b)

	return
}

func (c *community) GenerateResponseMessage(sendMsg message) (err error) {
	return c.GenerateRequestMessage(sendMsg)
}

func (c *community) ProcessIncomingMessage(recvMsg message) (err error) {
	rm := recvMsg.(*messageV1)

	if !bytes.Equal(c.Community, rm.Community) {
		return &MessageError{
			Message: fmt.Sprintf(
				"Community mismatch - expected [%s], actual [%s]",
				toHexStr(c.Community, ""), toHexStr(rm.Community, "")),
			Detail: fmt.Sprintf("Message - [%s]", rm),
		}
	}

	_, err = rm.Pdu().Unmarshal(rm.PduBytes())
	if err != nil {
		return &MessageError{
			Cause:   err,
			Message: "Failed to Unmarshal Pdu",
			Detail:  fmt.Sprintf("Pdu Bytes - [%s]", toHexStr(rm.PduBytes(), " ")),
		}
	}
	return
}

func (c *community) Discover(snmp *SNMP) error {
	return nil
}

func (c *community) String() string {
	return fmt.Sprintf(`{"Community": "%s"}`, toHexStr(c.Community, ""))
}

type discoveryStatus int

const (
	// for client side
	noDiscovered discoveryStatus = iota
	noSynchronized
	discovered

	// for server side
	remoteReference
)

func (d discoveryStatus) String() string {
	switch d {
	case noDiscovered:
		return "noDiscovered"
	case noSynchronized:
		return "noSynchronized"
	case discovered:
		return "discovered"
	case remoteReference:
		return "remoteReference"
	default:
		return "Unknown"
	}
}

type usm struct {
	UserName        []byte
	DiscoveryStatus discoveryStatus
	AuthEngineId    []byte
	AuthEngineBoots int64
	AuthEngineTime  int64
	UpdatedTime     time.Time
	AuthKey         []byte
	AuthPassword    string
	AuthProtocol    AuthProtocol
	PrivKey         []byte
	PrivPassword    string
	PrivProtocol    PrivProtocol
}

func (u *usm) Identifier() string {
	id := string(u.AuthEngineId) + ":" + string(u.UserName)
	if len(u.AuthPassword) > 0 {
		id += ":auth"
	}
	return id
}

func (u *usm) GenerateRequestMessage(sendMsg message) (err error) {
	// setup message
	m := sendMsg.(*messageV3)

	if u.DiscoveryStatus > noDiscovered {
		m.UserName = u.UserName
		m.AuthEngineId = u.AuthEngineId
	}
	if u.DiscoveryStatus > noSynchronized {
		err = u.UpdateEngineBootsTime()
		if err != nil {
			return
		}
		m.AuthEngineBoots = u.AuthEngineBoots
		m.AuthEngineTime = u.AuthEngineTime
	}

	// setup Pdu
	pduBytes, err := sendMsg.Pdu().Marshal()
	if err != nil {
		return
	}
	m.SetPduBytes(pduBytes)

	if m.Authentication() {
		// encrypt Pdu
		if m.Privacy() {
			err = encrypt(m, u.PrivProtocol, u.PrivKey)
			if err != nil {
				return
			}
		}

		// get digest of whole message
		digest, e := mac(m, u.AuthProtocol, u.AuthKey)
		if e != nil {
			return e
		}
		m.AuthParameter = digest
	}

	return
}

func (u *usm) GenerateResponseMessage(sendMsg message) (err error) {
	return u.GenerateRequestMessage(sendMsg)
}

func (u *usm) ProcessIncomingMessage(recvMsg message) (err error) {
	rm := recvMsg.(*messageV3)

	// RFC3411 Section 5
	if l := len(rm.AuthEngineId); l < 5 || l > 32 {
		return &MessageError{
			Message: fmt.Sprintf("AuthEngineId length is range 5..32, value [%s]",
				toHexStr(rm.AuthEngineId, "")),
		}
	}
	if rm.AuthEngineBoots < 0 || rm.AuthEngineBoots > math.MaxInt32 {
		return &MessageError{
			Message: fmt.Sprintf("AuthEngineBoots is range %d..%d, value [%d]",
				0, math.MaxInt32, rm.AuthEngineBoots),
		}
	}
	if rm.AuthEngineTime < 0 || rm.AuthEngineTime > math.MaxInt32 {
		return &MessageError{
			Message: fmt.Sprintf("AuthEngineTime is range %d..%d, value [%d]",
				0, math.MaxInt32, rm.AuthEngineTime),
		}
	}
	if u.DiscoveryStatus > noDiscovered {
		if !bytes.Equal(u.AuthEngineId, rm.AuthEngineId) {
			return &MessageError{
				Message: fmt.Sprintf(
					"AuthEngineId mismatch - expected [%s], actual [%s]",
					toHexStr(u.AuthEngineId, ""), toHexStr(rm.AuthEngineId, "")),
				Detail: fmt.Sprintf("Message - [%s]", rm),
			}
		}
		if !bytes.Equal(u.UserName, rm.UserName) {
			return &MessageError{
				Message: fmt.Sprintf(
					"UserName mismatch - expected [%s], actual [%s]",
					toHexStr(u.UserName, ""), toHexStr(rm.UserName, "")),
				Detail: fmt.Sprintf("Message - [%s]", rm),
			}
		}
	}

	if rm.Authentication() {
		// get & check digest of whole message
		digest, e := mac(rm, u.AuthProtocol, u.AuthKey)
		if e != nil {
			return &MessageError{
				Cause:   e,
				Message: "Can't get a message digest",
			}
		}
		if !hmac.Equal(rm.AuthParameter, digest) {
			return &MessageError{
				Message: fmt.Sprintf("Failed to authenticate - expected [%s], actual [%s]",
					toHexStr(rm.AuthParameter, ""), toHexStr(digest, "")),
			}
		}

		// decrypt Pdu
		if rm.Privacy() {
			e := decrypt(rm, u.PrivProtocol, u.PrivKey, rm.PrivParameter)
			if e != nil {
				return &MessageError{
					Cause:   e,
					Message: "Can't decrypt a message",
				}
			}
		}
	}

	// update boots & time
	switch u.DiscoveryStatus {
	case remoteReference:
		if rm.Authentication() {
			if err = u.CheckTimeliness(rm.AuthEngineBoots, rm.AuthEngineTime); err != nil {
				return
			}
			u.SynchronizeEngineBootsTime(rm.AuthEngineBoots, rm.AuthEngineTime)
		}
	case discovered:
		if rm.Authentication() {
			err = u.CheckTimeliness(rm.AuthEngineBoots, rm.AuthEngineTime)
			if err != nil {
				u.SynchronizeEngineBootsTime(0, 0)
				u.DiscoveryStatus = noSynchronized
				return
			}
		}
		fallthrough
	case noSynchronized:
		if rm.Authentication() {
			u.SynchronizeEngineBootsTime(rm.AuthEngineBoots, rm.AuthEngineTime)
			u.DiscoveryStatus = discovered
		}
	case noDiscovered:
		u.SetAuthEngineId(rm.AuthEngineId)
		u.DiscoveryStatus = noSynchronized
	}

	_, err = rm.Pdu().Unmarshal(rm.PduBytes())
	if err != nil {
		var note string
		if rm.Privacy() {
			note = " (probably Pdu was unable to decrypt)"
		}
		return &MessageError{
			Cause:   err,
			Message: fmt.Sprintf("Failed to Unmarshal Pdu%s", note),
			Detail:  fmt.Sprintf("Pdu Bytes - [%s]", toHexStr(rm.PduBytes(), " ")),
		}
	}
	return
}

func (u *usm) Discover(snmp *SNMP) (err error) {
	if snmp.args.SecurityEngineId != "" {
		securityEngineId, _ := engineIdToBytes(snmp.args.SecurityEngineId)
		u.SetAuthEngineId(securityEngineId)
		u.DiscoveryStatus = noSynchronized
		return
	}

	if u.DiscoveryStatus == noDiscovered {
		// Send an empty Pdu with the NoAuthNoPriv
		orgSecLevel := snmp.args.SecurityLevel
		snmp.args.SecurityLevel = NoAuthNoPriv

		pdu := NewPdu(snmp.args.Version, GetRequest)
		_, err = snmp.sendPdu(pdu)

		snmp.args.SecurityLevel = orgSecLevel
		if err != nil {
			return
		}
	}

	if u.DiscoveryStatus == noSynchronized && snmp.args.SecurityLevel > NoAuthNoPriv {
		// Send an empty Pdu
		pdu := NewPdu(snmp.args.Version, GetRequest)
		_, err = snmp.sendPdu(pdu)
		if err != nil {
			return
		}
	}

	return
}

func (u *usm) SetAuthEngineId(authEngineId []byte) {
	u.AuthEngineId = authEngineId
	if len(u.AuthPassword) > 0 {
		u.AuthKey = passwordToKey(u.AuthProtocol, u.AuthPassword, authEngineId)
	}
	if len(u.PrivPassword) > 0 {
		u.PrivKey = passwordToKey(u.AuthProtocol, u.PrivPassword, authEngineId)
	}
}

func (u *usm) UpdateEngineBootsTime() error {
	now := time.Now()
	u.AuthEngineTime += int64(now.Sub(u.UpdatedTime).Seconds())
	if u.AuthEngineTime > math.MaxInt32 {
		u.AuthEngineBoots++
		// RFC3414 2.2.2
		if u.AuthEngineBoots == math.MaxInt32 {
			return fmt.Errorf("EngineBoots reached the max value, [%d]", math.MaxInt32)
		}
		u.AuthEngineTime -= math.MaxInt32
	}
	u.UpdatedTime = now
	return nil
}

func (u *usm) SynchronizeEngineBootsTime(engineBoots, engineTime int64) {
	u.AuthEngineBoots = engineBoots
	u.AuthEngineTime = engineTime
	u.UpdatedTime = time.Now()
}

func (u *usm) CheckTimeliness(engineBoots, engineTime int64) error {
	// RFC3414 Section 3.2 7) b)
	if engineBoots == math.MaxInt32 ||
		engineBoots < u.AuthEngineBoots ||
		(engineBoots == u.AuthEngineBoots && u.AuthEngineTime-engineTime > 150) {
		return &MessageError{
			Message: fmt.Sprintf(
				"The message is not in the time window - local [%d/%d], remote [%d/%d]",
				engineBoots, engineTime, u.AuthEngineBoots, u.AuthEngineTime),
		}
	}
	return nil
}

func (u *usm) String() string {
	return fmt.Sprintf(
		`{"UserName": "%s", "DiscoveryStatus": "%s", "AuthEngineId": "%s", `+
			`"AuthEngineBoots": "%d", "AuthEngineTime": "%d", "UpdatedTime": "%s", `+
			`"AuthKey": "%s", "AuthProtocol": "%s", "PrivKey": "%s", "PrivProtocol": "%s"}`,
		toHexStr(u.UserName, ""), u.DiscoveryStatus, toHexStr(u.AuthEngineId, ""),
		u.AuthEngineBoots, u.AuthEngineTime, u.UpdatedTime,
		toHexStr(u.AuthKey, ""), u.AuthProtocol, toHexStr(u.PrivKey, ""), u.PrivProtocol)
}

func mac(msg *messageV3, proto AuthProtocol, key []byte) ([]byte, error) {
	tmp := msg.AuthParameter
	msg.AuthParameter = padding([]byte{}, 12)
	msgBytes, err := msg.Marshal()
	msg.AuthParameter = tmp
	if err != nil {
		return nil, err
	}

	var h hash.Hash
	switch proto {
	case Md5:
		h = hmac.New(md5.New, key)
	case Sha:
		h = hmac.New(sha1.New, key)
	}
	h.Write(msgBytes)
	return h.Sum(nil)[:12], nil
}

func encrypt(msg *messageV3, proto PrivProtocol, key []byte) (err error) {
	var dst, priv []byte
	src := msg.PduBytes()

	switch proto {
	case Des:
		dst, priv, err = encryptDES(src, key, int32(msg.AuthEngineBoots), genSalt32())
	case Aes:
		dst, priv, err = encryptAES(
			src, key, int32(msg.AuthEngineBoots), int32(msg.AuthEngineTime), genSalt64())
	}
	if err != nil {
		return
	}

	raw := asn1.RawValue{Class: classUniversal, Tag: tagOctetString, IsCompound: false}
	raw.Bytes = dst
	dst, err = asn1.Marshal(raw)
	if err == nil {
		msg.SetPduBytes(dst)
		msg.PrivParameter = priv
	}
	return
}

func decrypt(msg *messageV3, proto PrivProtocol, key, privParam []byte) (err error) {
	var raw asn1.RawValue
	_, err = ber.Unmarshal(msg.PduBytes(), &raw)
	if err != nil {
		return
	}
	if raw.Class != classUniversal || raw.Tag != tagOctetString || raw.IsCompound {
		return asn1.StructuralError{fmt.Sprintf(
			"Invalid encrypted Pdu object - Class [%02x], Tag [%02x] : [%s]",
			raw.Class, raw.Tag, toHexStr(msg.PduBytes(), " "))}
	}

	var dst []byte
	switch proto {
	case Des:
		dst, err = decryptDES(raw.Bytes, key, privParam)
	case Aes:
		dst, err = decryptAES(
			raw.Bytes, key, privParam, int32(msg.AuthEngineBoots), int32(msg.AuthEngineTime))
	}

	if err == nil {
		msg.SetPduBytes(dst)
	}
	return
}

func encryptDES(src, key []byte, engineBoots, salt int32) (dst, privParam []byte, err error) {
	block, err := des.NewCipher(key[:8])
	if err != nil {
		return
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, engineBoots)
	binary.Write(&buf, binary.BigEndian, salt)
	privParam = buf.Bytes()
	iv := xor(key[8:16], privParam)

	src = padding(src, des.BlockSize)
	dst = make([]byte, len(src))

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(dst, src)
	return
}

func decryptDES(src, key, privParam []byte) (dst []byte, err error) {
	if len(src)%des.BlockSize != 0 {
		err = &ArgumentError{
			Value:   len(src),
			Message: "Invalid DES cipher length",
		}
		return
	}
	if len(privParam) != 8 {
		err = &ArgumentError{
			Value:   len(privParam),
			Message: "Invalid DES PrivParameter length",
		}
		return
	}

	block, err := des.NewCipher(key[:8])
	if err != nil {
		return
	}

	iv := xor(key[8:16], privParam)
	dst = make([]byte, len(src))

	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(dst, src)
	return
}

func encryptAES(src, key []byte, engineBoots, engineTime int32, salt int64) (
	dst, privParam []byte, err error) {

	block, err := aes.NewCipher(key[:16])
	if err != nil {
		return
	}

	var buf1, buf2 bytes.Buffer
	binary.Write(&buf1, binary.BigEndian, salt)
	privParam = buf1.Bytes()

	binary.Write(&buf2, binary.BigEndian, engineBoots)
	binary.Write(&buf2, binary.BigEndian, engineTime)
	iv := append(buf2.Bytes(), privParam...)

	src = padding(src, aes.BlockSize)
	dst = make([]byte, len(src))

	mode := cipher.NewCFBEncrypter(block, iv)
	mode.XORKeyStream(dst, src)
	return
}

func decryptAES(src, key, privParam []byte, engineBoots, engineTime int32) (
	dst []byte, err error) {

	if len(privParam) != 8 {
		err = &ArgumentError{
			Value:   len(privParam),
			Message: "Invalid AES PrivParameter length",
		}
		return
	}

	block, err := aes.NewCipher(key[:16])
	if err != nil {
		return
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, engineBoots)
	binary.Write(&buf, binary.BigEndian, engineTime)
	iv := append(buf.Bytes(), privParam...)

	dst = make([]byte, len(src))

	mode := cipher.NewCFBDecrypter(block, iv)
	mode.XORKeyStream(dst, src)
	return
}

func passwordToKey(proto AuthProtocol, password string, engineId []byte) []byte {
	var h hash.Hash
	switch proto {
	case Md5:
		h = md5.New()
	case Sha:
		h = sha1.New()
	}

	pass := []byte(password)
	plen := len(pass)
	for i := mega / plen; i > 0; i-- {
		h.Write(pass)
	}
	remain := mega % plen
	if remain > 0 {
		h.Write(pass[:remain])
	}
	ku := h.Sum(nil)

	h.Reset()
	h.Write(ku)
	h.Write(engineId)
	h.Write(ku)
	return h.Sum(nil)
}

func newSecurity(args *SNMPArguments) security {
	switch args.Version {
	case V1, V2c:
		return &community{
			Community: []byte(args.Community),
		}
	case V3:
		sec := &usm{
			UserName: []byte(args.UserName),
		}
		switch args.SecurityLevel {
		case AuthPriv:
			sec.PrivPassword = args.PrivPassword
			sec.PrivProtocol = args.PrivProtocol
			fallthrough
		case AuthNoPriv:
			sec.AuthPassword = args.AuthPassword
			sec.AuthProtocol = args.AuthProtocol
		}
		return sec
	default:
		return nil
	}
}

func newSecurityFromEntry(entry *SecurityEntry) security {
	switch entry.Version {
	case V1, V2c:
		return &community{
			Community: []byte(entry.Community),
		}
	case V3:
		sec := &usm{
			UserName: []byte(entry.UserName),
		}
		switch entry.SecurityLevel {
		case AuthPriv:
			sec.PrivPassword = entry.PrivPassword
			sec.PrivProtocol = entry.PrivProtocol
			fallthrough
		case AuthNoPriv:
			sec.AuthPassword = entry.AuthPassword
			sec.AuthProtocol = entry.AuthProtocol
		}
		if len(entry.SecurityEngineId) > 0 {
			authEngineId, _ := engineIdToBytes(entry.SecurityEngineId)
			sec.SetAuthEngineId(authEngineId)
			sec.DiscoveryStatus = remoteReference
		}
		return sec
	default:
		return nil
	}
}

type securityMap struct {
	lock *sync.RWMutex
	objs map[string]security
}

func (m *securityMap) Set(sec security) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.objs[sec.Identifier()] = sec
}

func (m *securityMap) Lookup(msg message) security {
	var id string
	switch mm := msg.(type) {
	case *messageV1:
		id = string(mm.Community)
	case *messageV3:
		id = string(mm.AuthEngineId) + ":" + string(mm.UserName)
		if mm.Authentication() {
			id += ":auth"
		}
	}

	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.objs[id]
}

func (m *securityMap) List() []security {
	ret := make([]security, 0, len(m.objs))

	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, v := range m.objs {
		ret = append(ret, v)
	}
	return ret
}

func (m *securityMap) Delete(sec security) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.objs, sec.Identifier())
}

func newSecurityMap() *securityMap {
	return &securityMap{
		lock: new(sync.RWMutex),
		objs: map[string]security{},
	}
}
