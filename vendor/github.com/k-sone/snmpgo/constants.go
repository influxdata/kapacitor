package snmpgo

import (
	"time"
)

type SNMPVersion int

const (
	V1  SNMPVersion = 0
	V2c SNMPVersion = 1
	V3  SNMPVersion = 3
)

func (s SNMPVersion) String() string {
	switch s {
	case V1:
		return "1"
	case V2c:
		return "2c"
	case V3:
		return "3"
	default:
		return "Unknown"
	}
}

type PduType int

const (
	GetRequest PduType = iota
	GetNextRequest
	GetResponse
	SetRequest
	Trap
	GetBulkRequest
	InformRequest
	SNMPTrapV2
	Report
)

func (t PduType) String() string {
	switch t {
	case GetRequest:
		return "GetRequest"
	case GetNextRequest:
		return "GetNextRequest"
	case GetResponse:
		return "GetResponse"
	case SetRequest:
		return "SetRequest"
	case Trap:
		return "Trap"
	case GetBulkRequest:
		return "GetBulkRequest"
	case InformRequest:
		return "InformRequest"
	case SNMPTrapV2:
		return "SNMPTrapV2"
	case Report:
		return "Report"
	default:
		return "Unknown"
	}
}

type ErrorStatus int

const (
	NoError ErrorStatus = iota
	TooBig
	NoSuchName
	BadValue
	ReadOnly
	GenError
	NoAccess
	WrongType
	WrongLength
	WrongEncoding
	WrongValue
	NoCreation
	InconsistentValue
	ResourceUnavailable
	CommitFailed
	UndoFailed
	AuthorizationError
	NotWritable
	InconsistentName
)

func (e ErrorStatus) String() string {
	switch e {
	case NoError:
		return "NoError"
	case TooBig:
		return "TooBig"
	case NoSuchName:
		return "NoSuchName"
	case BadValue:
		return "BadValue"
	case ReadOnly:
		return "ReadOnly"
	case GenError:
		return "GenError"
	case NoAccess:
		return "NoAccess"
	case WrongType:
		return "WrongType"
	case WrongLength:
		return "WrongLength"
	case WrongEncoding:
		return "WrongEncoding"
	case WrongValue:
		return "WrongValue"
	case NoCreation:
		return "NoCreation"
	case InconsistentValue:
		return "InconsistentValue"
	case ResourceUnavailable:
		return "ResourceUnavailable"
	case CommitFailed:
		return "CommitFailed"
	case UndoFailed:
		return "UndoFailed"
	case AuthorizationError:
		return "AuthorizationError"
	case NotWritable:
		return "NotWritable"
	case InconsistentName:
		return "InconsistentName"
	default:
		return "Unknown"
	}
}

type SecurityLevel int

const (
	NoAuthNoPriv SecurityLevel = iota
	AuthNoPriv
	AuthPriv
)

func (s SecurityLevel) String() string {
	switch s {
	case NoAuthNoPriv:
		return "NoAuthNoPriv"
	case AuthNoPriv:
		return "AuthNoPriv"
	case AuthPriv:
		return "AuthPriv"
	default:
		return "Unknown"
	}
}

type AuthProtocol string

const (
	Md5 AuthProtocol = "MD5"
	Sha AuthProtocol = "SHA"
)

type PrivProtocol string

const (
	Des PrivProtocol = "DES"
	Aes PrivProtocol = "AES"
)

type securityModel int

const (
	// RFC 3411 Section 6.1
	securitySNMPv1 = iota + 1
	securitySNMPv2c
	securityUsm
)

func (s securityModel) String() string {
	switch s {
	case securitySNMPv1:
		return "SNMPv1"
	case securitySNMPv2c:
		return "SNMPv2c"
	case securityUsm:
		return "USM"
	default:
		return "Unknown"
	}
}

const (
	timeoutDefault = 5 * time.Second
	recvBufferSize = 1 << 11
	msgSizeDefault = 1400
	msgSizeMinimum = 484
	tagMask        = 0x1f
	mega           = 1 << 20
)

// ASN.1 Class
const (
	classUniversal = iota
	classApplication
	classContextSpecific
	classPrivate
)

// ASN.1 Tag
const (
	tagInteger          = 0x02
	tagOctetString      = 0x04
	tagNull             = 0x05
	tagObjectIdentifier = 0x06
	tagSequence         = 0x10
	tagIpaddress        = 0x40
	tagCounter32        = 0x41
	tagGauge32          = 0x42
	tagTimeTicks        = 0x43
	tagOpaque           = 0x44
	tagCounter64        = 0x46
	tagNoSucheObject    = 0x80
	tagNoSucheInstance  = 0x81
	tagEndOfMibView     = 0x82
)

type reportStatusOid string

const (
	// RFC 3412 Section 5
	snmpUnknownSecurityModels reportStatusOid = "1.3.6.1.6.3.11.2.1.1.0"
	snmpInvalidMsgs           reportStatusOid = "1.3.6.1.6.3.11.2.1.2.0"
	snmpUnknownPDUHandlers    reportStatusOid = "1.3.6.1.6.3.11.2.1.3.0"
	// RFC 3413 Section 4.1.2
	snmpUnavailableContexts reportStatusOid = "1.3.6.1.6.3.12.1.4.0"
	snmpUnknownContexts     reportStatusOid = "1.3.6.1.6.3.12.1.5.0"
	// RFC 3414 Section 5
	usmStatsUnsupportedSecLevels reportStatusOid = "1.3.6.1.6.3.15.1.1.1.0"
	usmStatsNotInTimeWindows     reportStatusOid = "1.3.6.1.6.3.15.1.1.2.0"
	usmStatsUnknownUserNames     reportStatusOid = "1.3.6.1.6.3.15.1.1.3.0"
	usmStatsUnknownEngineIDs     reportStatusOid = "1.3.6.1.6.3.15.1.1.4.0"
	usmStatsWrongDigests         reportStatusOid = "1.3.6.1.6.3.15.1.1.5.0"
	usmStatsDecryptionErrors     reportStatusOid = "1.3.6.1.6.3.15.1.1.6.0"
)

func (r reportStatusOid) String() string {
	switch r {
	case snmpUnknownSecurityModels:
		return "SnmpUnknownSecurityModels"
	case snmpInvalidMsgs:
		return "SnmpInvalidMsgs"
	case snmpUnknownPDUHandlers:
		return "SnmpUnknownPDUHandlers"
	case snmpUnavailableContexts:
		return "SnmpUnavailableContexts"
	case snmpUnknownContexts:
		return "SnmpUnknownContexts"
	case usmStatsUnsupportedSecLevels:
		return "UsmStatsUnsupportedSecLevels"
	case usmStatsNotInTimeWindows:
		return "UsmStatsNotInTimeWindows"
	case usmStatsUnknownUserNames:
		return "UsmStatsUnknownUserNames"
	case usmStatsUnknownEngineIDs:
		return "UsmStatsUnknownEngineIDs"
	case usmStatsWrongDigests:
		return "UsmStatsWrongDigests"
	case usmStatsDecryptionErrors:
		return "UsmStatsDecryptionErrors"
	default:
		return "Unknown"
	}
}

var (
	OidSysUpTime = MustNewOid("1.3.6.1.2.1.1.3.0")
	OidSnmpTrap  = MustNewOid("1.3.6.1.6.3.1.1.4.1.0")
)
