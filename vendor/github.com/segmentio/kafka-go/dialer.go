package kafka

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"time"
)

// The Dialer type mirrors the net.Dialer API but is designed to open kafka
// connections instead of raw network connections.
type Dialer struct {
	// Unique identifier for client connections established by this Dialer.
	ClientID string

	// Timeout is the maximum amount of time a dial will wait for a connect to
	// complete. If Deadline is also set, it may fail earlier.
	//
	// The default is no timeout.
	//
	// When dialing a name with multiple IP addresses, the timeout may be
	// divided between them.
	//
	// With or without a timeout, the operating system may impose its own
	// earlier timeout. For instance, TCP timeouts are often around 3 minutes.
	Timeout time.Duration

	// Deadline is the absolute point in time after which dials will fail.
	// If Timeout is set, it may fail earlier.
	// Zero means no deadline, or dependent on the operating system as with the
	// Timeout option.
	Deadline time.Time

	// LocalAddr is the local address to use when dialing an address.
	// The address must be of a compatible type for the network being dialed.
	// If nil, a local address is automatically chosen.
	LocalAddr net.Addr

	// DualStack enables RFC 6555-compliant "Happy Eyeballs" dialing when the
	// network is "tcp" and the destination is a host name with both IPv4 and
	// IPv6 addresses. This allows a client to tolerate networks where one
	// address family is silently broken.
	DualStack bool

	// FallbackDelay specifies the length of time to wait before spawning a
	// fallback connection, when DualStack is enabled.
	// If zero, a default delay of 300ms is used.
	FallbackDelay time.Duration

	// KeepAlive specifies the keep-alive period for an active network
	// connection.
	// If zero, keep-alives are not enabled. Network protocols that do not
	// support keep-alives ignore this field.
	KeepAlive time.Duration

	// Resolver optionally specifies an alternate resolver to use.
	Resolver Resolver

	// TLS enables Dialer to open secure connections.  If nil, standard net.Conn
	// will be used.
	TLS *tls.Config
}

// Dial connects to the address on the named network.
func (d *Dialer) Dial(network string, address string) (*Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext connects to the address on the named network using the provided
// context.
//
// The provided Context must be non-nil. If the context expires before the
// connection is complete, an error is returned. Once successfully connected,
// any expiration of the context will not affect the connection.
//
// When using TCP, and the host in the address parameter resolves to multiple
// network addresses, any dial timeout (from d.Timeout or ctx) is spread over
// each consecutive dial, such that each is given an appropriate fraction of the
// time to connect. For example, if a host has 4 IP addresses and the timeout is
// 1 minute, the connect to each single address will be given 15 seconds to
// complete before trying the next one.
func (d *Dialer) DialContext(ctx context.Context, network string, address string) (*Conn, error) {
	if d.Timeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}

	if !d.Deadline.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, d.Deadline)
		defer cancel()
	}

	c, err := d.dialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewConnWith(c, ConnConfig{ClientID: d.ClientID}), nil
}

// DialLeader opens a connection to the leader of the partition for a given
// topic.
//
// The address given to the DialContext method may not be the one that the
// connection will end up being established to, because the dialer will lookup
// the partition leader for the topic and return a connection to that server.
// The original address is only used as a mechanism to discover the
// configuration of the kafka cluster that we're connecting to.
func (d *Dialer) DialLeader(ctx context.Context, network string, address string, topic string, partition int) (*Conn, error) {
	b, err := d.LookupLeader(ctx, network, address, topic, partition)
	if err != nil {
		return nil, err
	}

	c, err := d.dialContext(ctx, network, net.JoinHostPort(b.Host, strconv.Itoa(b.Port)))
	if err != nil {
		return nil, err
	}

	return NewConnWith(c, ConnConfig{
		ClientID:  d.ClientID,
		Topic:     topic,
		Partition: partition,
	}), nil
}

// LookupLeader searches for the kafka broker that is the leader of the
// partition for a given topic, returning a Broker value representing it.
func (d *Dialer) LookupLeader(ctx context.Context, network string, address string, topic string, partition int) (Broker, error) {
	c, err := d.DialContext(ctx, network, address)
	if err != nil {
		return Broker{}, err
	}
	defer c.Close()

	brkch := make(chan Broker, 1)
	errch := make(chan error, 1)

	go func() {
		for attempt := 0; true; attempt++ {
			if attempt != 0 {
				sleep(ctx, backoff(attempt, 100*time.Millisecond, 10*time.Second))
			}

			partitions, err := c.ReadPartitions(topic)
			if err != nil {
				if isTemporary(err) {
					continue
				}
				errch <- err
				return
			}

			for _, p := range partitions {
				if p.ID == partition {
					brkch <- p.Leader
					return
				}
			}
		}

		errch <- UnknownTopicOrPartition
	}()

	var brk Broker
	select {
	case brk = <-brkch:
	case err = <-errch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return brk, err
}

// LookupPartitions returns the list of partitions that exist for the given topic.
func (d *Dialer) LookupPartitions(ctx context.Context, network string, address string, topic string) ([]Partition, error) {
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	prtch := make(chan []Partition, 1)
	errch := make(chan error, 1)

	go func() {
		if prt, err := conn.ReadPartitions(topic); err != nil {
			errch <- err
		} else {
			prtch <- prt
		}
	}()

	var prt []Partition
	select {
	case prt = <-prtch:
	case err = <-errch:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return prt, err
}

// connectTLS returns a tls.Conn that has already completed the Handshake
func (d *Dialer) connectTLS(ctx context.Context, conn net.Conn) (tlsConn *tls.Conn, err error) {
	tlsConn = tls.Client(conn, d.TLS)
	errch := make(chan error)

	go func() {
		defer close(errch)
		errch <- tlsConn.Handshake()
	}()

	select {
	case <-ctx.Done():
		conn.Close()
		tlsConn.Close()
		<-errch // ignore possible error from Handshake
		err = ctx.Err()

	case err = <-errch:
	}

	return
}

func (d *Dialer) dialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	if r := d.Resolver; r != nil {
		host, port := splitHostPort(address)
		addrs, err := r.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addrs) != 0 {
			address = addrs[0]
		}
		if len(port) != 0 {
			address, _ = splitHostPort(address)
			address = net.JoinHostPort(address, port)
		}
	}

	conn, err := (&net.Dialer{
		LocalAddr:     d.LocalAddr,
		DualStack:     d.DualStack,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
	}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if d.TLS != nil {
		return d.connectTLS(ctx, conn)
	}

	return conn, nil
}

// DefaultDialer is the default dialer used when none is specified.
var DefaultDialer = &Dialer{
	Timeout:   10 * time.Second,
	DualStack: true,
}

// Dial is a convenience wrapper for DefaultDialer.Dial.
func Dial(network string, address string) (*Conn, error) {
	return DefaultDialer.Dial(network, address)
}

// DialContext is a convenience wrapper for DefaultDialer.DialContext.
func DialContext(ctx context.Context, network string, address string) (*Conn, error) {
	return DefaultDialer.DialContext(ctx, network, address)
}

// DialLeader is a convenience wrapper for DefaultDialer.DialLeader.
func DialLeader(ctx context.Context, network string, address string, topic string, partition int) (*Conn, error) {
	return DefaultDialer.DialLeader(ctx, network, address, topic, partition)
}

// The Resolver interface is used as an abstraction to provide service discovery
// of the hosts of a kafka cluster.
type Resolver interface {
	// LookupHost looks up the given host using the local resolver.
	// It returns a slice of that host's addresses.
	LookupHost(ctx context.Context, host string) (addrs []string, err error)
}

func sleep(ctx context.Context, duration time.Duration) bool {
	if duration == 0 {
		select {
		default:
			return true
		case <-ctx.Done():
			return false
		}
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func backoff(attempt int, min time.Duration, max time.Duration) time.Duration {
	d := time.Duration(attempt*attempt) * min
	if d > max {
		d = max
	}
	return d
}

func splitHostPort(s string) (host string, port string) {
	host, port, _ = net.SplitHostPort(s)
	if len(host) == 0 && len(port) == 0 {
		host = s
	}
	return
}
