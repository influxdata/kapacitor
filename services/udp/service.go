package udp

import (
	"errors"
	"net"
	"strings"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/diagnostic"
)

const (
	UDPPacketSize = 65536
)

// statistics gathered by the UDP package.
const (
	statPointsReceived    = "points_rx"
	statBytesReceived     = "bytes_rx"
	statPointsParseFail   = "points_parse_fail"
	statReadFail          = "read_fail"
	statPointsTransmitted = "points_tx"
	statTransmitFail      = "tx_fail"
)

//
// Service represents here an UDP service
// that will listen for incoming packets
// formatted with the inline protocol
//
type Service struct {
	conn    *net.UDPConn
	addr    *net.UDPAddr
	wg      sync.WaitGroup
	done    chan struct{}
	packets chan []byte

	config Config

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	Diagnostic diagnostic.Diagnostic
	statMap    *expvar.Map
	statKey    string
}

func NewService(c Config, d diagnostic.Diagnostic) *Service {
	cfg := *c.WithDefaults()
	return &Service{
		config:     cfg,
		done:       make(chan struct{}),
		Diagnostic: d,
	}
}

func (s *Service) Open() (err error) {

	if s.config.BindAddress == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	s.addr, err = net.ResolveUDPAddr("udp", s.config.BindAddress)
	if err != nil {
		s.Diagnostic.Diag(
			"level", "error",
			"msg", "failed to resolve UDP address",
			"address", s.config.BindAddress,
			"error", err,
		)
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Diagnostic.Diag(
			"level", "error",
			"msg", "failed to st up UDP listener",
			"address", s.addr,
			"error", err,
		)
		return err
	}

	//save fully resolved and bound addr. Useful if port given was '0'.
	s.addr = s.conn.LocalAddr().(*net.UDPAddr)

	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	tags := map[string]string{"bind": s.addr.String()}
	s.statKey, s.statMap = vars.NewStatistic("udp", tags)

	if s.config.ReadBuffer != 0 {
		err = s.conn.SetReadBuffer(s.config.ReadBuffer)
		if err != nil {
			s.Diagnostic.Diag(
				"level", "error",
				"msg", "failed to set up UDP read buffer",
				"buffer", s.config.ReadBuffer,
				"error", err,
			)
			return err
		}
	}

	s.Diagnostic.Diag(
		"level", "info",
		"msg", "started listening on UDP",
		"address", s.addr.String(),
	)

	// Start reading and processing packets
	s.packets = make(chan []byte, s.config.Buffer)
	s.wg.Add(1)
	go s.serve()
	s.wg.Add(1)
	go s.processPackets()

	return nil
}

func (s *Service) serve() {
	defer s.wg.Done()
	defer close(s.packets)

	buf := make([]byte, UDPPacketSize)
	for {

		select {
		case <-s.done:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
		}

		n, _, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				s.statMap.Add(statReadFail, 1)
				s.Diagnostic.Diag(
					"level", "error",
					"msg", "failed to read UDP message",
					"error", err,
				)
			}
			continue
		}
		s.statMap.Add(statBytesReceived, int64(n))
		p := make([]byte, n)
		copy(p, buf[:n])
		s.packets <- p
	}
}

func (s *Service) processPackets() {
	defer s.wg.Done()

	for p := range s.packets {
		points, err := models.ParsePoints(p)
		if err != nil {
			s.statMap.Add(statPointsParseFail, 1)
			s.Diagnostic.Diag(
				"level", "error",
				"msg", "failed to parse points",
				"error", err,
			)
			continue
		}

		if err := s.PointsWriter.WritePoints(
			s.config.Database,
			s.config.RetentionPolicy,
			models.ConsistencyLevelAll,
			points,
		); err == nil {
			s.statMap.Add(statPointsTransmitted, int64(len(points)))
		} else {
			s.Diagnostic.Diag(
				"level", "error",
				"msg", "failed to write points to database",
				"database", s.config.Database,
				"error", err,
			)
			s.statMap.Add(statTransmitFail, 1)
		}

		s.statMap.Add(statPointsReceived, int64(len(points)))
	}
}

func (s *Service) Close() error {
	if s.conn == nil {
		return errors.New("Service already closed")
	}
	vars.DeleteStatistic(s.statKey)

	close(s.done)
	s.conn.Close()
	s.wg.Wait()

	// Release all remaining resources.
	s.done = nil
	s.conn = nil
	s.packets = nil

	s.Diagnostic.Diag(
		"level", "info",
		"msg", "service closed",
	)

	return nil
}

func (s *Service) Addr() *net.UDPAddr {
	return s.addr
}
