package udf

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/models"
)

var ErrServerStopped = errors.New("server already stopped")

// Server provides an implementation for the core communication with UDFs.
// The Server provides only a partial implementation of udf.Interface as
// it is expected that setup and teardown will be necessary to create a Server.
// As such the Open and Close methods are not implemented.
//
// Once a Server is created and started the owner can send points or batches
// to the UDF by writing them to the PointIn or BatchIn channels respectively,
// and according to the type of UDF created.
//
// The Server may be Aborted at anytime for various reasons. It is the owner's responsibility
// via the abortCallback to stop writing to the *In channels since no more selects on the channels
// will be performed.
//
// Calling Stop on the Server should only be done once the owner has stopped writing to the *In channel,
// at which point the remaining data will be processed and the UDF will be allowed to clean up.
//
// Callling Info returns information about available options the UDF has.
//
// Calling Init is required to process data.
// The behavior is undefined if you send points/batches to the Server without calling Init.
type Server struct {

	// If the processes is Aborted (via Keepalive timeout, etc.)
	// then no more data will be read off the *In channels.
	//
	// Optional callback if the process aborts.
	// It is the owners response
	abortCallback func()
	abortOnce     sync.Once

	// If abort fails after sometime this will be called
	killCallback func()

	pointIn chan models.Point
	batchIn chan models.Batch

	pointOut chan models.Point
	batchOut chan models.Batch

	stopped  bool
	stopping chan struct{}
	aborted  bool
	aborting chan struct{}
	// The first error that occurred or nil
	err   error
	errMu sync.Mutex

	requests      chan *Request
	requestsGroup sync.WaitGroup

	keepalive        chan int64
	keepaliveTimeout time.Duration

	in  ByteReadReader
	out io.WriteCloser

	// Group for waiting on read/write goroutines
	ioGroup sync.WaitGroup

	mu     sync.Mutex
	logger *log.Logger

	responseBuf []byte

	infoResponse     chan *Response
	initResponse     chan *Response
	snapshotResponse chan *Response
	restoreResponse  chan *Response

	batch *models.Batch
}

func NewServer(
	in ByteReadReader,
	out io.WriteCloser,
	l *log.Logger,
	timeout time.Duration,
	abortCallback func(),
	killCallback func(),
) *Server {
	s := &Server{
		in:               in,
		out:              out,
		logger:           l,
		requests:         make(chan *Request),
		keepalive:        make(chan int64, 1),
		keepaliveTimeout: timeout,
		abortCallback:    abortCallback,
		killCallback:     killCallback,
		pointIn:          make(chan models.Point),
		batchIn:          make(chan models.Batch),
		pointOut:         make(chan models.Point),
		batchOut:         make(chan models.Batch),
		infoResponse:     make(chan *Response, 1),
		initResponse:     make(chan *Response, 1),
		snapshotResponse: make(chan *Response, 1),
		restoreResponse:  make(chan *Response, 1),
	}

	return s
}

func (s *Server) PointIn() chan<- models.Point {
	return s.pointIn
}
func (s *Server) BatchIn() chan<- models.Batch {
	return s.batchIn
}
func (s *Server) PointOut() <-chan models.Point {
	return s.pointOut
}
func (s *Server) BatchOut() <-chan models.Batch {
	return s.batchOut
}

func (s *Server) setError(err error) {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	if s.err == nil {
		s.err = err
	}
}

// Start the Server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopped = false
	s.stopping = make(chan struct{})
	s.aborted = false
	s.aborting = make(chan struct{})

	s.ioGroup.Add(1)
	go func() {
		err := s.writeData()
		if err != nil {
			s.setError(err)
			defer s.abort()
		}
		s.ioGroup.Done()
	}()
	s.ioGroup.Add(1)
	go func() {
		err := s.readData()
		if err != nil {
			s.setError(err)
			defer s.abort()
		}
		s.ioGroup.Done()
	}()

	s.requestsGroup.Add(2)
	go s.runKeepalive()
	go s.watchKeepalive()

	return nil
}

// Abort the server.
// Data in-flight will not be processed.
// Give a reason for aborting via the err parameter.
func (s *Server) Abort(err error) {
	s.setError(err)
	s.abort()
}

func (s *Server) abort() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.aborted {
		return
	}
	s.aborted = true
	close(s.aborting)
	if s.abortCallback != nil {
		s.abortOnce.Do(s.abortCallback)
	}
	_ = s.stop()
}

// Wait for all IO to stop on the in/out objects.
func (s *Server) WaitIO() {
	s.ioGroup.Wait()
}

// Stop the Server cleanly.
//
// Calling Stop should only be done once the owner has stopped writing to the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stop()
}

// internal stop function you must acquire the lock before calling
func (s *Server) stop() error {

	if s.stopped {
		return s.err
	}
	s.stopped = true

	close(s.stopping)

	s.requestsGroup.Wait()

	close(s.requests)

	close(s.pointIn)
	close(s.batchIn)

	s.ioGroup.Wait()

	// Return the error that occurred first
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

type Info struct {
	Wants    EdgeType
	Provides EdgeType
	Options  map[string]*OptionInfo
}

// Get information about the process, available options etc.
// Info need not be called every time a process is started.
func (s *Server) Info() (Info, error) {
	info := Info{}
	req := &Request{Message: &Request_Info{
		Info: &InfoRequest{},
	}}

	resp, err := s.doRequestResponse(req, s.infoResponse)
	if err != nil {
		return info, err
	}
	ri := resp.Message.(*Response_Info).Info
	info.Options = ri.Options
	info.Wants = ri.Wants
	info.Provides = ri.Provides

	return info, nil
}

// Initialize the process with a set of Options.
// Calling Init is required even if you do not have any specific Options, just pass nil
func (s *Server) Init(options []*Option) error {
	req := &Request{Message: &Request_Init{
		Init: &InitRequest{
			Options: options,
		},
	}}
	resp, err := s.doRequestResponse(req, s.initResponse)
	if err != nil {
		return err
	}

	init := resp.Message.(*Response_Init).Init
	if !init.Success {
		return fmt.Errorf("failed to initialize processes %s", init.Error)
	}
	return nil
}

// Request a snapshot from the process.
func (s *Server) Snapshot() ([]byte, error) {
	req := &Request{Message: &Request_Snapshot{
		Snapshot: &SnapshotRequest{},
	}}
	resp, err := s.doRequestResponse(req, s.snapshotResponse)
	if err != nil {
		return nil, err
	}

	snapshot := resp.Message.(*Response_Snapshot).Snapshot.Snapshot
	return snapshot, nil
}

// Request to restore a snapshot.
func (s *Server) Restore(snapshot []byte) error {
	req := &Request{Message: &Request_Restore{
		Restore: &RestoreRequest{snapshot},
	}}
	resp, err := s.doRequestResponse(req, s.restoreResponse)
	if err != nil {
		return err
	}

	restore := resp.Message.(*Response_Restore).Restore
	if !restore.Success {
		return fmt.Errorf("error restoring snapshot: %s", restore.Error)
	}
	return nil
}

func (s *Server) doRequestResponse(req *Request, respC chan *Response) (*Response, error) {
	err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.stopped {
			if s.err != nil {
				return s.err
			}
			return ErrServerStopped
		}
		s.requestsGroup.Add(1)
		return nil
	}()
	if err != nil {
		return nil, err
	}
	defer s.requestsGroup.Done()

	select {
	case <-s.aborting:
		return nil, s.err
	case s.requests <- req:
	}

	select {
	case <-s.aborting:
		return nil, s.err
	case res := <-respC:
		return res, nil
	}
}

func (s *Server) doResponse(response *Response, respC chan *Response) {
	select {
	case respC <- response:
	default:
		s.logger.Printf("E! received %T without requesting it", response.Message)
	}
}

// send KeepaliveRequest on the specified interval
func (s *Server) runKeepalive() {
	defer s.requestsGroup.Done()
	if s.keepaliveTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(s.keepaliveTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			req := &Request{Message: &Request_Keepalive{
				Keepalive: &KeepaliveRequest{
					Time: time.Now().UnixNano(),
				},
			}}
			select {
			case s.requests <- req:
			case <-s.aborting:
			}
		case <-s.stopping:
			return
		}
	}
}

// Abort the process if a keepalive timeout is reached.
func (s *Server) watchKeepalive() {
	// Defer functions are called LIFO.
	// We need to call p.abort after p.requestsGroup.Done so we just set a flag.
	var err error
	defer func() {
		if err != nil {
			s.setError(err)
			aborted := make(chan struct{})
			go func() {
				timeout := s.keepaliveTimeout * 2
				if timeout <= 0 {
					timeout = time.Second
				}
				time.Sleep(timeout)
				select {
				case <-aborted:
					// We cleanly aborted process is stopped
				default:
					// We failed to abort just kill it.
					if s.killCallback != nil {
						s.logger.Println("E! process not responding! killing")
						s.killCallback()
					}
				}
			}()
			s.abort()
			close(aborted)
		}
	}()
	defer s.requestsGroup.Done()
	// If timeout is <= 0 then we don't ever timeout from keepalive,
	// but we need to receive from p.keepalive or handleResponse will block.
	// So we set a long timeout and then ignore it if its reached.
	timeout := s.keepaliveTimeout
	if timeout <= 0 {
		timeout = time.Hour
	}
	last := time.Now().UnixNano()
	for {
		select {
		case last = <-s.keepalive:
		case <-time.After(timeout):
			// Ignore invalid timeout
			if s.keepaliveTimeout <= 0 {
				break
			}
			err = fmt.Errorf("keepalive timedout, last keepalive received was: %s", time.Unix(0, last))
			s.logger.Println("E!", err)
			return
		case <-s.stopping:
			return
		}
	}
}

// Write Requests
func (s *Server) writeData() error {
	defer s.out.Close()
	for {
		select {
		case pt, ok := <-s.pointIn:
			if ok {
				err := s.writePoint(pt)
				if err != nil {
					return err
				}
			} else {
				s.pointIn = nil
			}
		case bt, ok := <-s.batchIn:
			if ok {
				err := s.writeBatch(bt)
				if err != nil {
					return err
				}
			} else {
				s.batchIn = nil
			}
		case req, ok := <-s.requests:
			if ok {
				err := s.writeRequest(req)
				if err != nil {
					return err
				}
			} else {
				s.requests = nil
			}
		case <-s.aborting:
			return s.err
		}
		if s.pointIn == nil && s.batchIn == nil && s.requests == nil {
			break
		}
	}
	return nil
}

func (s *Server) writePoint(pt models.Point) error {
	strs, floats, ints := s.fieldsToTypedMaps(pt.Fields)
	udfPoint := &Point{
		Time:            pt.Time.UnixNano(),
		Name:            pt.Name,
		Database:        pt.Database,
		RetentionPolicy: pt.RetentionPolicy,
		Group:           string(pt.Group),
		Dimensions:      pt.Dimensions.TagNames,
		ByName:          pt.Dimensions.ByName,
		Tags:            pt.Tags,
		FieldsDouble:    floats,
		FieldsInt:       ints,
		FieldsString:    strs,
	}
	req := &Request{
		Message: &Request_Point{udfPoint},
	}
	return s.writeRequest(req)
}

func (s *Server) fieldsToTypedMaps(fields models.Fields) (
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) {
	for k, v := range fields {
		switch value := v.(type) {
		case string:
			if strs == nil {
				strs = make(map[string]string)
			}
			strs[k] = value
		case float64:
			if floats == nil {
				floats = make(map[string]float64)
			}
			floats[k] = value
		case int64:
			if ints == nil {
				ints = make(map[string]int64)
			}
			ints[k] = value
		default:
			panic("unsupported field value type")
		}
	}
	return
}

func (s *Server) typeMapsToFields(
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
) models.Fields {
	fields := make(models.Fields)
	for k, v := range strs {
		fields[k] = v
	}
	for k, v := range ints {
		fields[k] = v
	}
	for k, v := range floats {
		fields[k] = v
	}
	return fields
}

func (s *Server) writeBatch(b models.Batch) error {
	req := &Request{
		Message: &Request_Begin{&BeginBatch{
			Name:   b.Name,
			Group:  string(b.Group),
			Tags:   b.Tags,
			Size:   int64(len(b.Points)),
			ByName: b.ByName,
		}},
	}
	err := s.writeRequest(req)
	if err != nil {
		return err
	}
	rp := &Request_Point{}
	req.Message = rp
	for _, pt := range b.Points {
		strs, floats, ints := s.fieldsToTypedMaps(pt.Fields)
		udfPoint := &Point{
			Time:         pt.Time.UnixNano(),
			Group:        string(b.Group),
			Tags:         pt.Tags,
			FieldsDouble: floats,
			FieldsInt:    ints,
			FieldsString: strs,
		}
		rp.Point = udfPoint
		err := s.writeRequest(req)
		if err != nil {
			return err
		}
	}

	req.Message = &Request_End{
		&EndBatch{
			Name:  b.Name,
			Group: string(b.Group),
			Tmax:  b.TMax.UnixNano(),
			Tags:  b.Tags,
		},
	}
	return s.writeRequest(req)
}

func (s *Server) writeRequest(req *Request) error {
	err := WriteMessage(req, s.out)
	if err != nil {
		err = fmt.Errorf("write error: %s", err)
	}
	return err
}

// Read Responses from STDOUT of the process.
func (s *Server) readData() error {
	defer func() {
		close(s.pointOut)
		close(s.batchOut)
	}()
	for {
		response, err := s.readResponse()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			err = fmt.Errorf("read error: %s", err)
			return err
		}
		err = s.handleResponse(response)
		if err != nil {
			return err
		}
	}
}

func (s *Server) readResponse() (*Response, error) {
	response := new(Response)
	err := ReadMessage(&s.responseBuf, s.in, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Server) handleResponse(response *Response) error {
	// Always reset the keepalive timer since we received a response
	select {
	case s.keepalive <- time.Now().UnixNano():
	case <-s.stopping:
		// No one is watching the keepalive anymore so we don't need to feed it,
		// but we still want to handle the response
	case <-s.aborting:
		return s.err
	}
	// handle response
	switch msg := response.Message.(type) {
	case *Response_Keepalive:
		// Noop we already reset the keepalive timer
	case *Response_Info:
		s.doResponse(response, s.infoResponse)
	case *Response_Init:
		s.doResponse(response, s.initResponse)
	case *Response_Snapshot:
		s.doResponse(response, s.snapshotResponse)
	case *Response_Restore:
		s.doResponse(response, s.restoreResponse)
	case *Response_Error:
		s.logger.Println("E!", msg.Error.Error)
		return errors.New(msg.Error.Error)
	case *Response_Begin:
		s.batch = &models.Batch{
			ByName: msg.Begin.ByName,
			Points: make([]models.BatchPoint, 0, msg.Begin.Size),
		}
	case *Response_Point:
		if s.batch != nil {
			pt := models.BatchPoint{
				Time: time.Unix(0, msg.Point.Time).UTC(),
				Tags: msg.Point.Tags,
				Fields: s.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			s.batch.Points = append(s.batch.Points, pt)
		} else {
			pt := models.Point{
				Time:            time.Unix(0, msg.Point.Time).UTC(),
				Name:            msg.Point.Name,
				Database:        msg.Point.Database,
				RetentionPolicy: msg.Point.RetentionPolicy,
				Group:           models.GroupID(msg.Point.Group),
				Dimensions:      models.Dimensions{ByName: msg.Point.ByName, TagNames: msg.Point.Dimensions},
				Tags:            msg.Point.Tags,
				Fields: s.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			select {
			case s.pointOut <- pt:
			case <-s.aborting:
				return s.err
			}
		}
	case *Response_End:
		s.batch.Name = msg.End.Name
		s.batch.TMax = time.Unix(0, msg.End.Tmax).UTC()
		s.batch.Group = models.GroupID(msg.End.Group)
		s.batch.Tags = msg.End.Tags
		select {
		case s.batchOut <- *s.batch:
		case <-s.aborting:
			return s.err
		}
		s.batch = nil
	default:
		panic(fmt.Sprintf("unexpected response message %T", msg))
	}
	return nil
}
