package udp // import "github.com/influxdata/influxdb/services/udp"

import (
	"errors"
	"expvar"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

const (
	// Arbitrary, testing indicated that this doesn't typically get over 10
	parserChanLen = 1000

	MAX_UDP_PAYLOAD = 64 * 1024
)

// statistics gathered by the UDP package.
const (
	statPointsReceived      = "pointsRx"
	statBytesReceived       = "bytesRx"
	statPointsParseFail     = "pointsParseFail"
	statReadFail            = "readFail"
	statBatchesTrasmitted   = "batchesTx"
	statPointsTransmitted   = "pointsTx"
	statBatchesTransmitFail = "batchesTxFail"
)

//
// Service represents here an UDP service
// that will listen for incoming packets
// formatted with the inline protocol
//
type Service struct {
	conn *net.UDPConn
	addr *net.UDPAddr
	wg   sync.WaitGroup
	done chan struct{}

	parserChan chan []byte
	batcher    *tsdb.PointBatcher
	config     Config

	PointsWriter interface {
		WritePoints(database, retentionPolicy string, consistencyLevel models.ConsistencyLevel, points []models.Point) error
	}

	MetaClient interface {
		CreateDatabase(name string) (*meta.DatabaseInfo, error)
	}

	Logger  *log.Logger
	statMap *expvar.Map
}

// NewService returns a new instance of Service.
func NewService(c Config) *Service {
	d := *c.WithDefaults()
	return &Service{
		config:     d,
		done:       make(chan struct{}),
		parserChan: make(chan []byte, parserChanLen),
		batcher:    tsdb.NewPointBatcher(d.BatchSize, d.BatchPending, time.Duration(d.BatchTimeout)),
		Logger:     log.New(os.Stderr, "[udp] ", log.LstdFlags),
	}
}

// Open starts the service
func (s *Service) Open() (err error) {
	// Configure expvar monitoring. It's OK to do this even if the service fails to open and
	// should be done before any data could arrive for the service.
	key := strings.Join([]string{"udp", s.config.BindAddress}, ":")
	tags := map[string]string{"bind": s.config.BindAddress}
	s.statMap = influxdb.NewStatistics(key, "udp", tags)

	if s.config.BindAddress == "" {
		return errors.New("bind address has to be specified in config")
	}
	if s.config.Database == "" {
		return errors.New("database has to be specified in config")
	}

	if _, err := s.MetaClient.CreateDatabase(s.config.Database); err != nil {
		return errors.New("Failed to ensure target database exists")
	}

	s.addr, err = net.ResolveUDPAddr("udp", s.config.BindAddress)
	if err != nil {
		s.Logger.Printf("Failed to resolve UDP address %s: %s", s.config.BindAddress, err)
		return err
	}

	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		s.Logger.Printf("Failed to set up UDP listener at address %s: %s", s.addr, err)
		return err
	}

	if s.config.ReadBuffer != 0 {
		err = s.conn.SetReadBuffer(s.config.ReadBuffer)
		if err != nil {
			s.Logger.Printf("Failed to set UDP read buffer to %d: %s",
				s.config.ReadBuffer, err)
			return err
		}
	}

	s.Logger.Printf("Started listening on UDP: %s", s.config.BindAddress)

	s.wg.Add(3)
	go s.serve()
	go s.parser()
	go s.writer()

	return nil
}

func (s *Service) writer() {
	defer s.wg.Done()

	for {
		select {
		case batch := <-s.batcher.Out():
			if err := s.PointsWriter.WritePoints(s.config.Database, s.config.RetentionPolicy, models.ConsistencyLevelAny, batch); err == nil {
				s.statMap.Add(statBatchesTrasmitted, 1)
				s.statMap.Add(statPointsTransmitted, int64(len(batch)))
			} else {
				s.Logger.Printf("failed to write point batch to database %q: %s", s.config.Database, err)
				s.statMap.Add(statBatchesTransmitFail, 1)
			}

		case <-s.done:
			return
		}
	}
}

func (s *Service) serve() {
	defer s.wg.Done()

	buf := make([]byte, MAX_UDP_PAYLOAD)
	s.batcher.Start()
	for {

		select {
		case <-s.done:
			// We closed the connection, time to go.
			return
		default:
			// Keep processing.
			n, _, err := s.conn.ReadFromUDP(buf)
			if err != nil {
				s.statMap.Add(statReadFail, 1)
				s.Logger.Printf("Failed to read UDP message: %s", err)
				continue
			}
			s.statMap.Add(statBytesReceived, int64(n))

			bufCopy := make([]byte, n)
			copy(bufCopy, buf[:n])
			s.parserChan <- bufCopy
		}
	}
}

func (s *Service) parser() {
	defer s.wg.Done()

	for {
		select {
		case <-s.done:
			return
		case buf := <-s.parserChan:
			points, err := models.ParsePointsWithPrecision(buf, time.Now().UTC(), s.config.Precision)
			if err != nil {
				s.statMap.Add(statPointsParseFail, 1)
				s.Logger.Printf("Failed to parse points: %s", err)
				continue
			}

			for _, point := range points {
				s.batcher.In() <- point
			}
			s.statMap.Add(statPointsReceived, int64(len(points)))
		}
	}
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	if s.conn == nil {
		return errors.New("Service already closed")
	}

	s.conn.Close()
	s.batcher.Flush()
	close(s.done)
	s.wg.Wait()

	// Release all remaining resources.
	s.done = nil
	s.conn = nil

	s.Logger.Print("Service closed")

	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[udp] ", log.LstdFlags)
}

// Addr returns the listener's address
func (s *Service) Addr() net.Addr {
	return s.addr
}
