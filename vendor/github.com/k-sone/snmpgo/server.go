package snmpgo

import (
	"fmt"
	"log"
	"math"
	"net"
	"runtime"
	"sync"
	"time"
)

const (
	maxTrapSize = 2 << 11 // 2048 bytes
)

// An argument for creating a Server Object
type ServerArguments struct {
	Network        string        // "udp", "udp4", "udp6" (The default is `udp`)
	LocalAddr      string        // See net.Dial parameter
	WriteTimeout   time.Duration // Timeout for writing a response (The default is 5sec)
	MessageMaxSize int           // Maximum size of a SNMP message (The default is 2048)
}

func (a *ServerArguments) setDefault() {
	if a.Network == "" {
		a.Network = "udp"
	}
	if a.WriteTimeout <= 0 {
		a.WriteTimeout = timeoutDefault
	}
	if a.MessageMaxSize == 0 {
		a.MessageMaxSize = maxTrapSize
	}
}

func (a *ServerArguments) validate() error {
	switch a.Network {
	case "", "udp", "udp4", "udp6":
	default:
		return &ArgumentError{
			Value:   a.Network,
			Message: fmt.Sprintf("Unsupported Network: %s", a.Network),
		}
	}
	if m := a.MessageMaxSize; (m != 0 && m < msgSizeMinimum) || m > math.MaxInt32 {
		return &ArgumentError{
			Value: m,
			Message: fmt.Sprintf("MessageMaxSize is range %d..%d",
				msgSizeMinimum, math.MaxInt32),
		}
	}

	return nil
}

func (a *ServerArguments) String() string {
	return escape(a)
}

// SecurityEntry is used for authentication of the received SNMP message
type SecurityEntry struct {
	Version          SNMPVersion   // SNMP version to use (V2c or V3)
	Community        string        // Community (V2c specific)
	UserName         string        // Security name (V3 specific)
	SecurityLevel    SecurityLevel // Security level (V3 specific)
	AuthPassword     string        // Authentication protocol pass phrase (V3 specific)
	AuthProtocol     AuthProtocol  // Authentication protocol (V3 specific)
	PrivPassword     string        // Privacy protocol pass phrase (V3 specific)
	PrivProtocol     PrivProtocol  // Privacy protocol (V3 specific)
	SecurityEngineId string        // Security engine ID (V3 Trap specific)
}

func (a *SecurityEntry) validate() error {
	if v := a.Version; v != V2c && v != V3 {
		return &ArgumentError{
			Value:   a.Version,
			Message: "Unsupported SNMP Version",
		}
	}
	// TODO Refactor(this copied from `SNMPArguments.validate()`)
	if a.Version == V3 {
		// RFC3414 Section 5
		if l := len(a.UserName); l < 1 || l > 32 {
			return &ArgumentError{
				Value:   a.UserName,
				Message: "UserName length is range 1..32",
			}
		}
		if a.SecurityLevel > NoAuthNoPriv {
			// RFC3414 Section 11.2
			if len(a.AuthPassword) < 8 {
				return &ArgumentError{
					Value:   a.AuthPassword,
					Message: "AuthPassword is at least 8 characters in length",
				}
			}
			if p := a.AuthProtocol; p != Md5 && p != Sha {
				return &ArgumentError{
					Value:   a.AuthProtocol,
					Message: "Illegal AuthProtocol",
				}
			}
		}
		if a.SecurityLevel > AuthNoPriv {
			// RFC3414 Section 11.2
			if len(a.PrivPassword) < 8 {
				return &ArgumentError{
					Value:   a.PrivPassword,
					Message: "PrivPassword is at least 8 characters in length",
				}
			}
			if p := a.PrivProtocol; p != Des && p != Aes {
				return &ArgumentError{
					Value:   a.PrivProtocol,
					Message: "Illegal PrivProtocol",
				}
			}
		}
		if a.SecurityEngineId != "" {
			a.SecurityEngineId = stripHexPrefix(a.SecurityEngineId)
			_, err := engineIdToBytes(a.SecurityEngineId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *SecurityEntry) String() string {
	return escape(a)
}

// TrapListener defines method that need to be implemented by Trap listeners.
// If OnTRAP panics, the server (caller of OnTRAP) assumes that affect of the panic
// is temporary and recovers by the panic and logs trace to the error log.
type TrapListener interface {
	OnTRAP(trap *TrapRequest)
}

// TrapRequest is representing trap request that is send from the network element.
type TrapRequest struct {
	// The received PDU
	Pdu Pdu

	// The source address of trap
	Source net.Addr

	// Error is an optional field used to indicate
	// errors which may occur during the decoding
	// of the received packet
	Error error
}

// A TrapServer defines parameters for running of TRAP daemon that listens for incoming
// trap messages.
type TrapServer struct {
	args      *ServerArguments
	mps       map[SNMPVersion]messageProcessing
	secs      map[SNMPVersion]*securityMap
	transport transport
	servingMu sync.RWMutex
	serving   bool

	// Error Logger which will be used for logging of default errors
	ErrorLog StdLogger
}

func (s *TrapServer) AddSecurity(entry *SecurityEntry) error {
	if err := entry.validate(); err != nil {
		return err
	}
	s.secs[entry.Version].Set(newSecurityFromEntry(entry))
	return nil
}

func (s *TrapServer) DeleteSecurity(entry *SecurityEntry) error {
	if err := entry.validate(); err != nil {
		return err
	}
	s.secs[entry.Version].Delete(newSecurityFromEntry(entry))
	return nil
}

// Serve starts the SNMP trap receiver.
// Serve blocks, the caller should call Close when finished, to shut it down.
func (s *TrapServer) Serve(listener TrapListener) error {
	if listener == nil {
		return &ArgumentError{Message: "listener is nil"}
	}
	s.servingMu.Lock()
	s.serving = true
	s.servingMu.Unlock()
	size := s.args.MessageMaxSize
	if size < recvBufferSize {
		size = recvBufferSize
	}

	for {
		conn, err := s.transport.Listen()
		s.servingMu.RLock()
		serving := s.serving
		s.servingMu.RUnlock()
		if !serving {
			return nil
		}
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				continue
			}
			return err
		}

		go func(conn interface{}) {
			defer s.transport.Close(conn)
			buf := make([]byte, size)
			for {
				_, src, msg, err := s.transport.Read(conn, buf)
				if _, ok := err.(net.Error); ok {
					s.servingMu.RLock()
					serving := s.serving
					s.servingMu.RUnlock()
					if serving {
						s.logf("trap: failed to read packet: %v", err)
					}
					return
				}

				go s.handle(listener, conn, msg, src, err)
			}
		}(conn)
	}
}

// Close shuts down the server.
func (s *TrapServer) Close() error {
	s.servingMu.Lock()
	s.serving = false
	s.servingMu.Unlock()
	return s.transport.Close(nil)
}

// handle a newly received trap
func (s *TrapServer) handle(listener TrapListener, conn interface{}, msg message, src net.Addr, err error) {
	defer func() {
		if err := recover(); err != nil {
			const size = 64 << 10
			logBuf := make([]byte, size)
			logBuf = logBuf[:runtime.Stack(logBuf, false)]
			s.logf("trap: panic while receiving %v: %v\n%s", src, err, logBuf)

		}
	}()

	var pdu Pdu
	var mp messageProcessing
	var sec security
	if msg != nil {
		var ok bool
		v := msg.Version()
		if mp, ok = s.mps[v]; ok {
			if sec = s.secs[v].Lookup(msg); sec != nil {
				pdu, err = mp.PrepareDataElements(sec, msg, nil)
			} else {
				err = &MessageError{
					Message: "Authentication failure",
					Detail:  fmt.Sprintf("Message - [%s]", msg),
				}
			}
		} else {
			err = &MessageError{
				Message: fmt.Sprintf("Unsupported SNMP version: %s", v),
				Detail:  fmt.Sprintf("Message - [%s]", msg),
			}
		}
	}

	if pdu != nil {
		switch t := pdu.PduType(); t {
		case SNMPTrapV2, InformRequest:
		default:
			err = &MessageError{
				Message: fmt.Sprintf("Invalid PduType: %s ", t),
				Detail:  fmt.Sprintf("Message - [%s]", msg),
			}
			pdu = nil
		}
	}

	listener.OnTRAP(&TrapRequest{Pdu: pdu, Source: src, Error: err})

	if pdu != nil && pdu.PduType() == InformRequest {
		if err = s.informResponse(conn, src, mp, sec, msg); err != nil && s.serving {
			s.logf("trap: failed to send response %v: %v", src, err)
		}
	}
}

func (s *TrapServer) informResponse(
	conn interface{}, src net.Addr, mp messageProcessing, sec security, msg message) error {

	respPdu := NewPduWithVarBinds(msg.Version(), GetResponse, msg.Pdu().VarBinds())
	respMsg, err := mp.PrepareResponseMessage(sec, respPdu, msg)
	if err != nil {
		return err
	}
	pkt, err := respMsg.Marshal()
	if err != nil {
		return err
	}
	return s.transport.Write(conn, pkt, src)
}

func (s *TrapServer) logf(format string, args ...interface{}) {
	if l := s.ErrorLog; l != nil {
		l.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// NewTrapServer returns a new Server and is using server arguments for configuration.
func NewTrapServer(args ServerArguments) (*TrapServer, error) {
	if err := args.validate(); err != nil {
		return nil, err
	}
	args.setDefault()

	return &TrapServer{
		args: &args,
		mps: map[SNMPVersion]messageProcessing{
			V2c: newMessageProcessing(V2c),
			V3:  newMessageProcessing(V3),
		},
		secs: map[SNMPVersion]*securityMap{
			V2c: newSecurityMap(),
			V3:  newSecurityMap(),
		},
		transport: newTransport(&args),
	}, nil
}
