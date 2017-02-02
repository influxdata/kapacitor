package network // import "collectd.org/network"

import (
	"context"
	"log"
	"net"

	"collectd.org/api"
)

// ListenAndWrite listens on the provided UDP address, parses the received
// packets and writes them to the provided api.Writer.
// This is a convenience function for a minimally configured server. If you
// need more control, see the "Server" type below.
func ListenAndWrite(ctx context.Context, address string, d api.Writer) error {
	srv := &Server{
		Addr:   address,
		Writer: d,
	}

	return srv.ListenAndWrite(ctx)
}

// Server holds parameters for running a collectd server.
type Server struct {
	Conn           *net.UDPConn   // UDP connection the server listens on.
	Addr           string         // Address to listen on if Conn is nil.
	Writer         api.Writer     // Object used to send incoming ValueLists to.
	BufferSize     uint16         // Maximum packet size to accept.
	PasswordLookup PasswordLookup // User to password lookup.
	SecurityLevel  SecurityLevel  // Minimal required security level.
	TypesDB        *api.TypesDB   // TypesDB for looking up DS names and verify data source types.
	// Interface is the name of the interface to use when subscribing to a
	// multicast group. Has no effect when using unicast.
	Interface string
}

// ListenAndWrite listens on the provided UDP connection (or creates one using
// Addr if Conn is nil), parses the received packets and writes them to the
// provided api.Writer.
func (srv *Server) ListenAndWrite(ctx context.Context) error {
	if srv.Conn == nil {
		addr := srv.Addr
		if addr == "" {
			addr = ":" + DefaultService
		}

		laddr, err := net.ResolveUDPAddr("udp", srv.Addr)
		if err != nil {
			return err
		}

		if laddr.IP != nil && laddr.IP.IsMulticast() {
			var ifi *net.Interface
			if srv.Interface != "" {
				if ifi, err = net.InterfaceByName(srv.Interface); err != nil {
					return err
				}
			}
			srv.Conn, err = net.ListenMulticastUDP("udp", ifi, laddr)
		} else {
			srv.Conn, err = net.ListenUDP("udp", laddr)
		}
		if err != nil {
			return err
		}
		defer func() {
			srv.Conn.Close()
			srv.Conn = nil
		}()
	}

	if srv.BufferSize <= 0 {
		srv.BufferSize = DefaultBufferSize
	}
	buf := make([]byte, srv.BufferSize)

	popts := ParseOpts{
		PasswordLookup: srv.PasswordLookup,
		SecurityLevel:  srv.SecurityLevel,
		TypesDB:        srv.TypesDB,
	}

	for {
		n, err := srv.Conn.Read(buf)
		if err != nil {
			return err
		}

		valueLists, err := Parse(buf[:n], popts)
		if err != nil {
			log.Printf("error while parsing: %v", err)
			continue
		}

		go dispatch(ctx, valueLists, srv.Writer)
	}
}

func dispatch(ctx context.Context, valueLists []*api.ValueList, d api.Writer) {
	for _, vl := range valueLists {
		if err := d.Write(ctx, vl); err != nil {
			log.Printf("error while dispatching: %v", err)
		}
	}
}
