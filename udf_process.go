package kapacitor

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf"
)

var ErrUDFProcessStopped = errors.New("process already stopped")

type byteReadCloser struct {
	*bufio.Reader
	io.Closer
}

// Wraps an external process and sends and receives data
// over STDIN and STDOUT. Lines received over STDERR are logged
// via normal Kapacitor logging.
//
// Once a UDFProcess is created and started the owner can send points or batches
// to the subprocess by writing them to the PointIn or BatchIn channels respectively,
// and according to the type of process created.
//
// The UDFProcess may be Aborted at anytime for various reasons. It is the owner's responsibility
// via the abortCallback to stop writing to the *In channels since no more selects on the channels
// will be performed.
//
// Calling Stop on the process should only be done once the owner has stopped writing to the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
//
// Callling Info returns information about available options the process has.
//
// Calling Init is required to process data.
// The behavior is undefined if you send points/batches to the process without calling Init.
type UDFProcess struct {

	// If the processes is Aborted (via Keepalive timeout, etc.)
	// then no more data will be read off the *In channels.
	//
	// Optional callback if the process aborts.
	// It is the owners response
	abortCallback func()
	abortOnce     sync.Once

	pointIn chan models.Point
	PointIn chan<- models.Point
	batchIn chan models.Batch
	BatchIn chan<- models.Batch

	pointOut chan models.Point
	PointOut <-chan models.Point
	batchOut chan models.Batch
	BatchOut <-chan models.Batch

	stopped  bool
	stopping chan struct{}
	aborted  bool
	aborting chan struct{}
	// The first error that occurred or nil
	err   error
	errMu sync.Mutex

	requests      chan *udf.Request
	requestsGroup sync.WaitGroup

	keepalive        chan int64
	keepaliveTimeout time.Duration

	commander command.Commander
	cmd       command.Command
	stdin     io.WriteCloser
	stdout    byteReadCloser
	stderr    io.ReadCloser

	// Group for waiting on read/write goroutines
	pipeWaitGroup sync.WaitGroup
	// Group for waiting on the process itself
	processWaitGroup sync.WaitGroup

	mu     sync.Mutex
	logger *log.Logger

	responseBuf []byte
	response    *udf.Response

	requestResponse  sync.Mutex
	infoResponse     chan *udf.Response
	initResponse     chan *udf.Response
	snapshotResponse chan *udf.Response
	restoreResponse  chan *udf.Response

	batch *models.Batch
}

func NewUDFProcess(
	commander command.Commander,
	l *log.Logger,
	timeout time.Duration,
	abortCallback func(),
) *UDFProcess {
	p := &UDFProcess{
		commander:        commander,
		logger:           l,
		requests:         make(chan *udf.Request),
		keepalive:        make(chan int64, 1),
		keepaliveTimeout: timeout,
		abortCallback:    abortCallback,
		response:         &udf.Response{},
	}

	p.pointIn = make(chan models.Point)
	p.PointIn = p.pointIn
	p.batchIn = make(chan models.Batch)
	p.BatchIn = p.batchIn

	p.pointOut = make(chan models.Point)
	p.PointOut = p.pointOut
	p.batchOut = make(chan models.Batch)
	p.BatchOut = p.batchOut
	return p
}

func (p *UDFProcess) setError(err error) {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	if p.err == nil {
		p.err = err
	}
}

// Start the UDFProcess
func (p *UDFProcess) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = false
	p.stopping = make(chan struct{})
	p.aborted = false
	p.aborting = make(chan struct{})

	cmd := p.commander.NewCommand()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	p.stdin = stdin

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	brc := byteReadCloser{
		bufio.NewReader(stdout),
		stdout,
	}
	p.stdout = brc

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	p.stderr = stderr

	err = cmd.Start()
	if err != nil {
		return err
	}
	p.cmd = cmd

	p.pipeWaitGroup.Add(1)
	go func() {
		err := p.writeData()
		if err != nil {
			p.setError(err)
			defer p.abort()
		}
		p.pipeWaitGroup.Done()
	}()
	p.pipeWaitGroup.Add(1)
	go func() {
		err := p.readData()
		if err != nil {
			p.setError(err)
			defer p.abort()
		}
		p.pipeWaitGroup.Done()
	}()

	// Wait for process to terminate
	p.processWaitGroup.Add(1)
	go func() {
		// First wait for the pipe read writes to finish
		p.pipeWaitGroup.Wait()
		err := cmd.Wait()
		if err != nil {
			err = fmt.Errorf("process exited unexpectedly: %v", err)
			p.setError(err)
			defer p.abort()
		}
		p.processWaitGroup.Done()
	}()

	p.requestsGroup.Add(2)
	go p.runKeepalive()
	go p.watchKeepalive()

	p.pipeWaitGroup.Add(1)
	go p.logStdErr()

	return nil
}

// Abort the process.
// Data in-flight will not be processed.
func (p *UDFProcess) Abort(err error) {
	p.setError(err)
	p.abort()
}

func (p *UDFProcess) abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.aborted {
		return
	}
	p.aborted = true
	close(p.aborting)
	if p.abortCallback != nil {
		p.abortOnce.Do(p.abortCallback)
	}
	p.stop()
}

// Send the kill signal to the process
// it hasn't shutdown cleanly
func (p *UDFProcess) kill() {
	p.cmd.Kill()
}

// Stop the UDFProcess cleanly.
//
// Calling Stop should only be done once the owner has stopped writing to the *In channel,
// at which point the remaining data will be processed and the subprocess will be allowed to exit cleanly.
func (p *UDFProcess) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stop()
}

// internal stop function you must acquire the lock before calling
func (p *UDFProcess) stop() error {

	if p.stopped {
		return p.err
	}
	p.stopped = true

	close(p.stopping)

	p.requestsGroup.Wait()

	close(p.requests)

	close(p.pointIn)
	close(p.batchIn)

	p.pipeWaitGroup.Wait()
	p.processWaitGroup.Wait()

	// Return the error that occurred first
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.err
}

type UDFProcessInfo struct {
	Commander command.Commander
	Timeout   time.Duration
	Wants     pipeline.EdgeType
	Provides  pipeline.EdgeType
	Options   map[string]*udf.OptionInfo
}

// Get information about the process, available options etc.
// Info need not be called every time a process is started.
func (p *UDFProcess) Info() (UDFProcessInfo, error) {
	info := UDFProcessInfo{}
	req := &udf.Request{Message: &udf.Request_Info{
		Info: &udf.InfoRequest{},
	}}

	resp, err := p.doRequestResponse(req, &p.infoResponse)
	if err != nil {
		return info, err
	}
	ri := resp.Message.(*udf.Response_Info).Info
	info.Commander = p.commander
	info.Timeout = p.keepaliveTimeout
	info.Options = ri.Options

	switch ri.Wants {
	case udf.EdgeType_STREAM:
		info.Wants = pipeline.StreamEdge
	case udf.EdgeType_BATCH:
		info.Wants = pipeline.BatchEdge
	}
	switch ri.Provides {
	case udf.EdgeType_STREAM:
		info.Provides = pipeline.StreamEdge
	case udf.EdgeType_BATCH:
		info.Provides = pipeline.BatchEdge
	}
	return info, nil

}

// Initialize the process with a set of Options.
// Calling Init is required even if you do not have any specific Options, just pass nil
func (p *UDFProcess) Init(options []*udf.Option) error {
	req := &udf.Request{Message: &udf.Request_Init{
		Init: &udf.InitRequest{
			Options: options,
		},
	}}
	resp, err := p.doRequestResponse(req, &p.initResponse)
	if err != nil {
		return err
	}

	init := resp.Message.(*udf.Response_Init).Init
	if !init.Success {
		return fmt.Errorf("failed to initialize processes %s", init.Error)
	}
	return nil
}

// Request a snapshot from the process.
func (p *UDFProcess) Snapshot() ([]byte, error) {
	req := &udf.Request{Message: &udf.Request_Snapshot{
		Snapshot: &udf.SnapshotRequest{},
	}}
	resp, err := p.doRequestResponse(req, &p.snapshotResponse)
	if err != nil {
		return nil, err
	}
	snapshot := resp.Message.(*udf.Response_Snapshot).Snapshot.Snapshot
	return snapshot, nil
}

// Request to restore a snapshot.
func (p *UDFProcess) Restore(snapshot []byte) error {

	req := &udf.Request{Message: &udf.Request_Restore{
		Restore: &udf.RestoreRequest{snapshot},
	}}
	resp, err := p.doRequestResponse(req, &p.restoreResponse)
	if err != nil {
		return err
	}

	restore := resp.Message.(*udf.Response_Restore).Restore
	if !restore.Success {
		return fmt.Errorf("error restoring snapshot: %s", restore.Error)
	}
	return nil
}

// Request a snapshot from the process.
func (p *UDFProcess) doRequestResponse(req *udf.Request, respC *chan *udf.Response) (*udf.Response, error) {
	err := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.stopped {
			if p.err != nil {
				return p.err
			}
			return ErrUDFProcessStopped
		}
		p.requestsGroup.Add(1)

		p.requestResponse.Lock()
		defer p.requestResponse.Unlock()
		*respC = make(chan *udf.Response, 1)
		return nil
	}()
	if err != nil {
		return nil, err
	}
	defer p.requestsGroup.Done()
	defer func() {
		p.requestResponse.Lock()
		defer p.requestResponse.Unlock()
		*respC = nil
	}()

	select {
	case <-p.aborting:
		return nil, p.err
	case p.requests <- req:
	}

	select {
	case <-p.aborting:
		return nil, p.err
	case res := <-(*respC):
		return res, nil
	}
}

func (p *UDFProcess) doResponse(response *udf.Response, respC *chan *udf.Response) {
	p.requestResponse.Lock()
	if *respC != nil {
		p.requestResponse.Unlock()
		*respC <- response
	} else {
		p.requestResponse.Unlock()
		p.logger.Printf("E! received %T without requesting it", response.Message)
	}
}

// send KeepaliveRequest on the specified interval
func (p *UDFProcess) runKeepalive() {
	defer p.requestsGroup.Done()
	if p.keepaliveTimeout <= 0 {
		return
	}
	ticker := time.NewTicker(p.keepaliveTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			req := &udf.Request{Message: &udf.Request_Keepalive{
				Keepalive: &udf.KeepaliveRequest{
					Time: time.Now().UnixNano(),
				},
			}}
			select {
			case p.requests <- req:
			case <-p.aborting:
			}
		case <-p.stopping:
			return
		}
	}
}

// Abort the process if a keepalive timeout is reached.
func (p *UDFProcess) watchKeepalive() {
	// Defer functions are called LIFO.
	// We need to call p.abort after p.requestsGroup.Done so we just set a flag.
	var err error
	defer func() {
		if err != nil {
			p.setError(err)
			aborted := make(chan struct{})
			go func() {
				timeout := p.keepaliveTimeout * 2
				if timeout <= 0 {
					timeout = time.Second
				}
				time.Sleep(timeout)
				select {
				case <-aborted:
					// We cleanly aborted process is stopped
				default:
					// We failed to abort just kill it.
					p.logger.Println("E! process not responding! killing")
					p.kill()
				}
			}()
			p.abort()
			close(aborted)
		}
	}()
	defer p.requestsGroup.Done()
	// If timeout is <= 0 then we don't ever timeout from keepalive,
	// but we need to receive from p.keepalive or handleResponse will block.
	// So we set a long timeout and then ignore it if its reached.
	timeout := p.keepaliveTimeout
	if timeout <= 0 {
		timeout = time.Hour
	}
	last := time.Now().UnixNano()
	for {
		select {
		case last = <-p.keepalive:
		case <-time.After(timeout):
			// Ignore invalid timeout
			if p.keepaliveTimeout <= 0 {
				break
			}
			err = fmt.Errorf("keepalive timedout, last keepalive received was: %s", time.Unix(0, last))
			p.logger.Println("E!", err)
			return
		case <-p.stopping:
			return
		}
	}
}

// Write Requests to the STDIN of the process.
func (p *UDFProcess) writeData() error {
	defer p.stdin.Close()
	for {
		select {
		case pt, ok := <-p.pointIn:
			if ok {
				err := p.writePoint(pt)
				if err != nil {
					return err
				}
			} else {
				p.pointIn = nil
			}
		case bt, ok := <-p.batchIn:
			if ok {
				err := p.writeBatch(bt)
				if err != nil {
					return err
				}
			} else {
				p.batchIn = nil
			}
		case req, ok := <-p.requests:
			if ok {
				err := p.writeRequest(req)
				if err != nil {
					return err
				}
			} else {
				p.requests = nil
			}
		case <-p.aborting:
			return p.err
		}
		if p.pointIn == nil && p.batchIn == nil && p.requests == nil {
			break
		}
	}
	return nil
}

func (p *UDFProcess) writePoint(pt models.Point) error {
	strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
	udfPoint := &udf.Point{
		Time:            pt.Time.UnixNano(),
		Name:            pt.Name,
		Database:        pt.Database,
		RetentionPolicy: pt.RetentionPolicy,
		Group:           string(pt.Group),
		Dimensions:      pt.Dimensions,
		Tags:            pt.Tags,
		FieldsDouble:    floats,
		FieldsInt:       ints,
		FieldsString:    strs,
	}
	req := &udf.Request{
		Message: &udf.Request_Point{udfPoint},
	}
	return p.writeRequest(req)
}

func (p *UDFProcess) fieldsToTypedMaps(fields models.Fields) (
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

func (p *UDFProcess) typeMapsToFields(
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

func (p *UDFProcess) writeBatch(b models.Batch) error {
	req := &udf.Request{
		Message: &udf.Request_Begin{&udf.BeginBatch{
			Name:  b.Name,
			Group: string(b.Group),
			Tags:  b.Tags,
		}},
	}
	err := p.writeRequest(req)
	if err != nil {
		return err
	}
	rp := &udf.Request_Point{}
	req.Message = rp
	for _, pt := range b.Points {
		strs, floats, ints := p.fieldsToTypedMaps(pt.Fields)
		udfPoint := &udf.Point{
			Time:         pt.Time.UnixNano(),
			Group:        string(b.Group),
			Tags:         pt.Tags,
			FieldsDouble: floats,
			FieldsInt:    ints,
			FieldsString: strs,
		}
		rp.Point = udfPoint
		err := p.writeRequest(req)
		if err != nil {
			return err
		}
	}

	req.Message = &udf.Request_End{
		&udf.EndBatch{
			Name:  b.Name,
			Group: string(b.Group),
			Tmax:  b.TMax.UnixNano(),
			Tags:  b.Tags,
		},
	}
	return p.writeRequest(req)
}

func (p *UDFProcess) writeRequest(req *udf.Request) error {
	err := udf.WriteMessage(req, p.stdin)
	if err != nil {
		err = fmt.Errorf("write error: %s", err)
	}
	return err
}

// Read Responses from STDOUT of the process.
func (p *UDFProcess) readData() error {
	defer func() {
		close(p.pointOut)
		close(p.batchOut)
	}()
	for {
		response, err := p.readResponse()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			err = fmt.Errorf("read error: %s", err)
			return err
		}
		err = p.handleResponse(response)
		if err != nil {
			return err
		}
	}
}

func (p *UDFProcess) readResponse() (*udf.Response, error) {
	err := udf.ReadMessage(&p.responseBuf, p.stdout, p.response)
	if err != nil {
		return nil, err
	}
	return p.response, nil
}

func (p *UDFProcess) handleResponse(response *udf.Response) error {
	// Always reset the keepalive timer since we received a response
	select {
	case p.keepalive <- time.Now().UnixNano():
	case <-p.stopping:
		// No one is watching the keepalive anymore so we don't need to feed it,
		// but we still want to handle the response
	case <-p.aborting:
		return p.err
	}
	// handle response
	switch msg := response.Message.(type) {
	case *udf.Response_Keepalive:
		// Noop we already reset the keepalive timer
	case *udf.Response_Info:
		p.doResponse(response, &p.infoResponse)
	case *udf.Response_Init:
		p.doResponse(response, &p.initResponse)
	case *udf.Response_Snapshot:
		p.doResponse(response, &p.snapshotResponse)
	case *udf.Response_Restore:
		p.doResponse(response, &p.restoreResponse)
	case *udf.Response_Error:
		p.logger.Println("E!", msg.Error.Error)
		return errors.New(msg.Error.Error)
	case *udf.Response_Begin:
		p.batch = &models.Batch{}
	case *udf.Response_Point:
		if p.batch != nil {
			pt := models.BatchPoint{
				Time: time.Unix(0, msg.Point.Time).UTC(),
				Tags: msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			p.batch.Points = append(p.batch.Points, pt)
		} else {
			pt := models.Point{
				Time:            time.Unix(0, msg.Point.Time).UTC(),
				Name:            msg.Point.Name,
				Database:        msg.Point.Database,
				RetentionPolicy: msg.Point.RetentionPolicy,
				Group:           models.GroupID(msg.Point.Group),
				Dimensions:      msg.Point.Dimensions,
				Tags:            msg.Point.Tags,
				Fields: p.typeMapsToFields(
					msg.Point.FieldsString,
					msg.Point.FieldsDouble,
					msg.Point.FieldsInt,
				),
			}
			select {
			case p.pointOut <- pt:
			case <-p.aborting:
				return p.err
			}
		}
	case *udf.Response_End:
		p.batch.Name = msg.End.Name
		p.batch.TMax = time.Unix(0, msg.End.Tmax).UTC()
		p.batch.Group = models.GroupID(msg.End.Group)
		p.batch.Tags = msg.End.Tags
		select {
		case p.batchOut <- *p.batch:
		case <-p.aborting:
			return p.err
		}
		p.batch = nil
	default:
		panic(fmt.Sprintf("unexpected response message %T", msg))
	}
	return nil
}

// Replay any lines from STDERR of the process to the Kapacitor log.
func (p *UDFProcess) logStdErr() {
	defer p.pipeWaitGroup.Done()
	scanner := bufio.NewScanner(p.stderr)
	for scanner.Scan() {
		p.logger.Println("I!P", scanner.Text())
	}
}
