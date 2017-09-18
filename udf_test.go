package kapacitor_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
	udf_test "github.com/influxdata/kapacitor/udf/test"
)

var diagService *diagnostic.Service

var kapacitorDiag kapacitor.Diagnostic

func init() {
	diagService = diagnostic.NewService(diagnostic.NewConfig(), ioutil.Discard, ioutil.Discard)
	diagService.Open()
	kapacitorDiag = diagService.NewKapacitorHandler()
}

func newUDFSocket(name string) (*kapacitor.UDFSocket, *udf_test.IO) {
	uio := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext(name)
	u := kapacitor.NewUDFSocket(name, "testNode", newTestSocket(uio), d, 0, nil)
	return u, uio
}

func newUDFProcess(name string) (*kapacitor.UDFProcess, *udf_test.IO) {
	uio := udf_test.NewIO()
	cmd := newTestCommander(uio)
	d := kapacitorDiag.WithNodeContext(name)
	u := kapacitor.NewUDFProcess(name, "testNode", cmd, command.Spec{}, d, 0, nil)
	return u, uio
}

func TestUDFSocket_OpenClose(t *testing.T) {
	u, uio := newUDFSocket("OpenClose")
	testUDF_OpenClose(u, uio, t)
}
func TestUDFProcess_OpenClose(t *testing.T) {
	u, uio := newUDFProcess("OpenClose")
	testUDF_OpenClose(u, uio, t)
}

func testUDF_OpenClose(u udf.Interface, uio *udf_test.IO, t *testing.T) {
	u.Open()

	close(uio.Responses)
	u.Close()
	// read all requests and wait till the chan is closed
	for range uio.Requests {
	}
	if err := <-uio.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDFSocket_WritePoint(t *testing.T) {
	u, uio := newUDFSocket("WritePoint")
	testUDF_WritePoint(u, uio, t)
}

func TestUDFProcess_WritePoint(t *testing.T) {
	u, uio := newUDFProcess("WritePoint")
	testUDF_WritePoint(u, uio, t)
}

func testUDF_WritePoint(u udf.Interface, uio *udf_test.IO, t *testing.T) {
	go func() {
		req := <-uio.Requests
		_, ok := req.Message.(*agent.Request_Init)
		if !ok {
			t.Errorf("expected init message got %T", req.Message)
		}
		res := &agent.Response{
			Message: &agent.Response_Init{
				Init: &agent.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- res
		req = <-uio.Requests
		pt, ok := req.Message.(*agent.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Point{
				Point: pt.Point,
			},
		}
		uio.Responses <- res
		close(uio.Responses)
	}()

	err := u.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = u.Init(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write point to server
	p := edge.NewPointMessage(
		"test",
		"db",
		"rp",
		models.Dimensions{},
		models.Fields{"f1": 1.0, "f2": 2.0},
		models.Tags{"t1": "v1", "t2": "v2"},
		time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	u.In() <- p
	rp := <-u.Out()
	if !reflect.DeepEqual(rp, p) {
		t.Errorf("unexpected returned point got: %v exp %v", rp, p)
	}

	u.Close()
	// read all requests and wait till the chan is closed
	for range uio.Requests {
	}
	if err := <-uio.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDFSocket_WriteBatch(t *testing.T) {
	u, uio := newUDFSocket("WriteBatch")
	testUDF_WriteBatch(u, uio, t)
}

func TestUDFProcess_WriteBatch(t *testing.T) {
	u, uio := newUDFProcess("WriteBatch")
	testUDF_WriteBatch(u, uio, t)
}

func testUDF_WriteBatch(u udf.Interface, uio *udf_test.IO, t *testing.T) {
	go func() {
		req := <-uio.Requests
		_, ok := req.Message.(*agent.Request_Init)
		if !ok {
			t.Errorf("expected init message got %T", req.Message)
		}
		res := &agent.Response{
			Message: &agent.Response_Init{
				Init: &agent.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- res
		// Begin batch
		req = <-uio.Requests
		bb, ok := req.Message.(*agent.Request_Begin)
		if !ok {
			t.Errorf("expected begin message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Begin{
				Begin: bb.Begin,
			},
		}
		uio.Responses <- res

		// Point
		req = <-uio.Requests
		pt, ok := req.Message.(*agent.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Point{
				Point: pt.Point,
			},
		}
		uio.Responses <- res

		// End batch
		req = <-uio.Requests
		eb, ok := req.Message.(*agent.Request_End)
		if !ok {
			t.Errorf("expected end message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_End{
				End: eb.End,
			},
		}
		uio.Responses <- res
		close(uio.Responses)
	}()

	err := u.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = u.Init(nil)
	if err != nil {
		t.Fatal(err)
	}

	// Write point to server
	b := edge.NewBufferedBatchMessage(
		edge.NewBeginBatchMessage(
			"test",
			models.Tags{"t1": "v1"},
			false,
			time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			1,
		),
		[]edge.BatchPointMessage{
			edge.NewBatchPointMessage(
				models.Fields{"f1": 1.0, "f2": 2.0, "f3": int64(1), "f4": "str"},
				models.Tags{"t1": "v1", "t2": "v2"},
				time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			),
		},
		edge.NewEndBatchMessage(),
	)
	u.In() <- b
	rb := <-u.Out()
	if !reflect.DeepEqual(b, rb) {
		t.Errorf("unexpected returned batch got: %v exp %v", rb, b)
	}

	u.Close()
	// read all requests and wait till the chan is closed
	for range uio.Requests {
	}
	if err := <-uio.ErrC; err != nil {
		t.Error(err)
	}
}

type testCommander struct {
	uio *udf_test.IO
}

func newTestCommander(uio *udf_test.IO) command.Commander {
	return &testCommander{
		uio: uio,
	}
}

func (c *testCommander) NewCommand(command.Spec) command.Command {
	return c
}

func (c *testCommander) Start() error { return nil }

func (c *testCommander) Wait() error { return nil }

func (c *testCommander) Stdin(io.Reader)  {}
func (c *testCommander) Stdout(io.Writer) {}
func (c *testCommander) Stderr(io.Writer) {}

func (c *testCommander) StdinPipe() (io.WriteCloser, error) {
	return c.uio.In(), nil
}

func (c *testCommander) StdoutPipe() (io.Reader, error) {
	return c.uio.Out(), nil
}

func (c *testCommander) StderrPipe() (io.Reader, error) {
	return &bytes.Buffer{}, nil
}

func (c *testCommander) Kill() {
	c.uio.Kill()
}

type testSocket struct {
	uio *udf_test.IO
}

func newTestSocket(uio *udf_test.IO) kapacitor.Socket {
	return &testSocket{
		uio: uio,
	}
}
func (s *testSocket) Open() error {
	return nil
}

func (s *testSocket) Close() error {
	return nil
}

func (s *testSocket) In() io.WriteCloser {
	return s.uio.In()
}

func (s *testSocket) Out() io.Reader {
	return s.uio.Out()
}
