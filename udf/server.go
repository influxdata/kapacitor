package udf

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/udf/agent"
)

var ErrServerStopped = errors.New("server already stopped")

type Diagnostic interface {
	Error(msg string, err error, ctx ...keyvalue.T)

	UDFLog(msg string)
}

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

	inMsg chan edge.Message

	outMsg chan edge.Message

	stopped  bool
	stopping chan struct{}
	aborted  bool
	aborting chan struct{}
	// The first error that occurred or nil
	err   error
	errMu sync.Mutex

	requests      chan *agent.Request
	requestsGroup sync.WaitGroup

	keepalive        chan int64
	keepaliveTimeout time.Duration

	taskID string
	nodeID string

	in  agent.ByteReadReader
	out io.WriteCloser

	// Group for waiting on read/write goroutines
	ioGroup sync.WaitGroup

	mu   sync.Mutex
	diag Diagnostic

	responseBuf []byte

	infoResponse     chan *agent.Response
	initResponse     chan *agent.Response
	snapshotResponse chan *agent.Response
	restoreResponse  chan *agent.Response

	// Buffer up batch messages
	begin  *agent.BeginBatch
	points []edge.BatchPointMessage
}

func NewServer(
	taskID, nodeID string,
	in agent.ByteReadReader,
	out io.WriteCloser,
	d Diagnostic,
	timeout time.Duration,
	abortCallback func(),
	killCallback func(),
) *Server {
	s := &Server{
		taskID:           taskID,
		nodeID:           nodeID,
		in:               in,
		out:              out,
		diag:             d,
		requests:         make(chan *agent.Request),
		keepalive:        make(chan int64, 1),
		keepaliveTimeout: timeout,
		abortCallback:    abortCallback,
		killCallback:     killCallback,
		inMsg:            make(chan edge.Message),
		outMsg:           make(chan edge.Message),
		infoResponse:     make(chan *agent.Response, 1),
		initResponse:     make(chan *agent.Response, 1),
		snapshotResponse: make(chan *agent.Response, 1),
		restoreResponse:  make(chan *agent.Response, 1),
	}

	return s
}

func (s *Server) In() chan<- edge.Message {
	return s.inMsg
}
func (s *Server) Out() <-chan edge.Message {
	return s.outMsg
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

	close(s.inMsg)

	s.ioGroup.Wait()

	// Return the error that occurred first
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

type Info struct {
	Wants    agent.EdgeType
	Provides agent.EdgeType
	Options  map[string]*agent.OptionInfo
}

// Get information about the process, available options etc.
// Info need not be called every time a process is started.
func (s *Server) Info() (Info, error) {
	info := Info{}
	req := &agent.Request{Message: &agent.Request_Info{
		Info: &agent.InfoRequest{},
	}}

	resp, err := s.doRequestResponse(req, s.infoResponse)
	if err != nil {
		return info, err
	}
	ri := resp.Message.(*agent.Response_Info).Info
	info.Options = ri.Options
	info.Wants = ri.Wants
	info.Provides = ri.Provides

	return info, nil
}

// Initialize the process with a set of Options.
// Calling Init is required even if you do not have any specific Options, just pass nil
func (s *Server) Init(options []*agent.Option) error {
	req := &agent.Request{Message: &agent.Request_Init{
		Init: &agent.InitRequest{
			Options: options,
			TaskID:  s.taskID,
			NodeID:  s.nodeID,
		},
	}}
	resp, err := s.doRequestResponse(req, s.initResponse)
	if err != nil {
		return err
	}

	init := resp.Message.(*agent.Response_Init).Init
	if !init.Success {
		return fmt.Errorf("failed to initialize processes %s", init.Error)
	}
	return nil
}

// Request a snapshot from the process.
func (s *Server) Snapshot() ([]byte, error) {
	req := &agent.Request{Message: &agent.Request_Snapshot{
		Snapshot: &agent.SnapshotRequest{},
	}}
	resp, err := s.doRequestResponse(req, s.snapshotResponse)
	if err != nil {
		return nil, err
	}

	snapshot := resp.Message.(*agent.Response_Snapshot).Snapshot.Snapshot
	return snapshot, nil
}

// Request to restore a snapshot.
func (s *Server) Restore(snapshot []byte) error {
	req := &agent.Request{Message: &agent.Request_Restore{
		Restore: &agent.RestoreRequest{snapshot},
	}}
	resp, err := s.doRequestResponse(req, s.restoreResponse)
	if err != nil {
		return err
	}

	restore := resp.Message.(*agent.Response_Restore).Restore
	if !restore.Success {
		return fmt.Errorf("error restoring snapshot: %s", restore.Error)
	}
	return nil
}

func (s *Server) doRequestResponse(req *agent.Request, respC chan *agent.Response) (*agent.Response, error) {
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

func (s *Server) doResponse(response *agent.Response, respC chan *agent.Response) {
	select {
	case respC <- response:
	default:
		s.diag.Error("received message without requesting it", fmt.Errorf("did not expect %T message", response.Message))
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
			req := &agent.Request{Message: &agent.Request_Keepalive{
				Keepalive: &agent.KeepaliveRequest{
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
						s.diag.Error("killing process", errors.New("process not responding"))
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
			s.diag.Error("encountered error", err)
			return
		case <-s.stopping:
			return
		}
	}
}

// Write Requests
func (s *Server) writeData() error {
	defer s.out.Close()
	var begin edge.BeginBatchMessage
	for {
		select {
		case m, ok := <-s.inMsg:
			if !ok {
				s.inMsg = nil
			}
			switch msg := m.(type) {
			case edge.PointMessage:
				err := s.writePoint(msg)
				if err != nil {
					return err
				}
			case edge.BeginBatchMessage:
				begin = msg
				err := s.writeBeginBatch(msg)
				if err != nil {
					return err
				}
			case edge.BatchPointMessage:
				err := s.writeBatchPoint(begin.GroupID(), msg)
				if err != nil {
					return err
				}
			case edge.EndBatchMessage:
				err := s.writeEndBatch(begin.Name(), begin.Time(), begin.GroupInfo(), msg)
				if err != nil {
					return err
				}
			case edge.BufferedBatchMessage:
				err := s.writeBufferedBatch(msg)
				if err != nil {
					return err
				}
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
		if s.inMsg == nil && s.requests == nil {
			break
		}
	}
	return nil
}

func (s *Server) writePoint(p edge.PointMessage) error {
	strs, floats, ints, bools := s.fieldsToTypedMaps(p.Fields())
	udfPoint := &agent.Point{
		Time:            p.Time().UnixNano(),
		Name:            p.Name(),
		Database:        p.Database(),
		RetentionPolicy: p.RetentionPolicy(),
		Group:           string(p.GroupID()),
		Dimensions:      p.Dimensions().TagNames,
		ByName:          p.Dimensions().ByName,
		Tags:            p.Tags(),
		FieldsDouble:    floats,
		FieldsInt:       ints,
		FieldsString:    strs,
		FieldsBool:      bools,
	}
	req := &agent.Request{
		Message: &agent.Request_Point{Point: udfPoint},
	}
	return s.writeRequest(req)
}

func (s *Server) fieldsToTypedMaps(fields models.Fields) (
	strs map[string]string,
	floats map[string]float64,
	ints map[string]int64,
	bools map[string]bool,
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
		case bool:
			if bools == nil {
				bools = make(map[string]bool)
			}
			bools[k] = value
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
	bools map[string]bool,
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
	for k, v := range bools {
		fields[k] = v
	}
	return fields
}

func (s *Server) writeBeginBatch(begin edge.BeginBatchMessage) error {
	req := &agent.Request{
		Message: &agent.Request_Begin{
			Begin: &agent.BeginBatch{
				Name:   begin.Name(),
				Group:  string(begin.GroupID()),
				Tags:   begin.Tags(),
				Size:   int64(begin.SizeHint()),
				ByName: begin.Dimensions().ByName,
			}},
	}
	return s.writeRequest(req)
}

func (s *Server) writeBatchPoint(group models.GroupID, bp edge.BatchPointMessage) error {
	strs, floats, ints, bools := s.fieldsToTypedMaps(bp.Fields())
	req := &agent.Request{
		Message: &agent.Request_Point{
			Point: &agent.Point{
				Time:         bp.Time().UnixNano(),
				Group:        string(group),
				Tags:         bp.Tags(),
				FieldsDouble: floats,
				FieldsInt:    ints,
				FieldsString: strs,
				FieldsBool:   bools,
			},
		},
	}
	return s.writeRequest(req)
}

func (s *Server) writeEndBatch(name string, tmax time.Time, groupInfo edge.GroupInfo, end edge.EndBatchMessage) error {
	req := &agent.Request{
		Message: &agent.Request_End{
			End: &agent.EndBatch{
				Name:  name,
				Group: string(groupInfo.ID),
				Tmax:  tmax.UnixNano(),
				Tags:  groupInfo.Tags,
			},
		},
	}
	return s.writeRequest(req)
}

func (s *Server) writeBufferedBatch(batch edge.BufferedBatchMessage) error {
	if err := s.writeBeginBatch(batch.Begin()); err != nil {
		return err
	}
	for _, bp := range batch.Points() {
		if err := s.writeBatchPoint(batch.GroupID(), bp); err != nil {
			return err
		}
	}
	return s.writeEndBatch(batch.Name(), batch.Time(), batch.GroupInfo(), batch.End())
}

func (s *Server) writeRequest(req *agent.Request) error {
	err := agent.WriteMessage(req, s.out)
	if err != nil {
		err = fmt.Errorf("write error: %s", err)
	}
	return err
}

// Read Responses from STDOUT of the process.
func (s *Server) readData() error {
	defer func() {
		close(s.outMsg)
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

func (s *Server) readResponse() (*agent.Response, error) {
	response := new(agent.Response)
	err := agent.ReadMessage(&s.responseBuf, s.in, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (s *Server) handleResponse(response *agent.Response) error {
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
	case *agent.Response_Keepalive:
		// Noop we already reset the keepalive timer
	case *agent.Response_Info:
		s.doResponse(response, s.infoResponse)
	case *agent.Response_Init:
		s.doResponse(response, s.initResponse)
	case *agent.Response_Snapshot:
		s.doResponse(response, s.snapshotResponse)
	case *agent.Response_Restore:
		s.doResponse(response, s.restoreResponse)
	case *agent.Response_Error:
		s.diag.Error("received error message", errors.New(msg.Error.Error))
		return errors.New(msg.Error.Error)
	case *agent.Response_Begin:
		s.begin = msg.Begin
		s.points = make([]edge.BatchPointMessage, 0, msg.Begin.Size)
	case *agent.Response_Point:
		if s.points != nil {
			bp := edge.NewBatchPointMessage(
				s.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
					msg.Point.FieldsBool,
				),
				msg.Point.Tags,
				time.Unix(0, msg.Point.Time).UTC(),
			)
			s.points = append(s.points, bp)
		} else {
			p := edge.NewPointMessage(
				msg.Point.Name,
				msg.Point.Database,
				msg.Point.RetentionPolicy,
				models.Dimensions{ByName: msg.Point.ByName, TagNames: msg.Point.Dimensions},
				s.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
					msg.Point.FieldsBool,
				),
				msg.Point.Tags,
				time.Unix(0, msg.Point.Time).UTC(),
			)
			select {
			case s.outMsg <- p:
			case <-s.aborting:
				return s.err
			}
		}
	case *agent.Response_End:
		begin := edge.NewBeginBatchMessage(
			msg.End.Name,
			msg.End.Tags,
			s.begin.ByName,
			time.Unix(0, msg.End.Tmax).UTC(),
			len(s.points),
		)
		bufferedBatch := edge.NewBufferedBatchMessage(
			begin,
			s.points,
			edge.NewEndBatchMessage(),
		)
		select {
		case s.outMsg <- bufferedBatch:
		case <-s.aborting:
			return s.err
		}
		s.begin = nil
		s.points = nil
	default:
		panic(fmt.Sprintf("unexpected response message %T", msg))
	}
	return nil
}
