package snmpgo

import (
	"net"
	"sync"
	"time"
)

type transport interface {
	Listen() (interface{}, error)
	Read(interface{}, []byte) (int, net.Addr, message, error)
	Write(interface{}, []byte, net.Addr) error
	Close(interface{}) error
}

type packetTransport struct {
	conn         net.PacketConn
	lock         *sync.Mutex
	anchor       chan struct{}
	network      string
	localAddr    string
	writeTimeout time.Duration
}

func (t *packetTransport) Listen() (interface{}, error) {
	t.lock.Lock()
	c := t.conn
	t.lock.Unlock()
	if c != nil {
		<-t.anchor
		return nil, nil
	}

	c, err := net.ListenPacket(t.network, t.localAddr)
	t.lock.Lock()
	t.conn = c
	t.lock.Unlock()
	return c, err
}

func (t *packetTransport) Read(conn interface{}, buf []byte) (num int, src net.Addr, msg message, err error) {
	c := conn.(net.PacketConn)
	for {
		num, src, err = c.ReadFrom(buf)
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				continue
			}
			return
		}

		pkt := make([]byte, num)
		copy(pkt, buf)
		msg, _, err = unmarshalMessage(pkt)
		return
	}
}

func (t *packetTransport) Write(conn interface{}, pkt []byte, dst net.Addr) error {
	c := conn.(net.PacketConn)
	if err := c.SetWriteDeadline(time.Now().Add(t.writeTimeout)); err != nil {
		return err
	}

	for {
		if _, err := c.WriteTo(pkt, dst); err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() && !e.Timeout() {
				continue
			}
			return err
		}
		return nil
	}
}

func (t *packetTransport) Close(_ interface{}) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if c := t.conn; c != nil {
		t.conn = nil
		t.anchor <- struct{}{}
		return c.Close()
	}
	return nil
}

func newTransport(args *ServerArguments) transport {
	switch args.Network {
	case "udp", "udp4", "udp6":
		return &packetTransport{
			lock:         new(sync.Mutex),
			anchor:       make(chan struct{}, 0),
			network:      args.Network,
			localAddr:    args.LocalAddr,
			writeTimeout: args.WriteTimeout,
		}
	default:
		return nil
	}
}
