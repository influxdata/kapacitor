package kapacitor_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/udf"
	udf_test "github.com/influxdata/kapacitor/udf/test"
)

func newUDFSocket(name string) (*kapacitor.UDFSocket, *udf_test.IO) {
	uio := udf_test.NewIO()
	l := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags)
	u := kapacitor.NewUDFSocket(newTestSocket(uio), l, 0, nil)
	return u, uio
}

func newUDFProcess(name string) (*kapacitor.UDFProcess, *udf_test.IO) {
	uio := udf_test.NewIO()
	cmd := newTestCommander(uio)
	l := log.New(os.Stderr, fmt.Sprintf("[%s] ", name), log.LstdFlags)
	u := kapacitor.NewUDFProcess(cmd, command.Spec{}, l, 0, nil)
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
		_, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Errorf("expected init message got %T", req.Message)
		}
		res := &udf.Response{
			Message: &udf.Response_Init{
				Init: &udf.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- res
		req = <-uio.Requests
		pt, ok := req.Message.(*udf.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Point{
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
	pt := models.Point{
		Name:            "test",
		Database:        "db",
		RetentionPolicy: "rp",
		Tags:            models.Tags{"t1": "v1", "t2": "v2"},
		Fields:          models.Fields{"f1": 1.0, "f2": 2.0},
		Time:            time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	u.PointIn() <- pt
	rpt := <-u.PointOut()
	if !reflect.DeepEqual(rpt, pt) {
		t.Errorf("unexpected returned point got: %v exp %v", rpt, pt)
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
		_, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Errorf("expected init message got %T", req.Message)
		}
		res := &udf.Response{
			Message: &udf.Response_Init{
				Init: &udf.InitResponse{
					Success: true,
				},
			},
		}
		uio.Responses <- res
		// Begin batch
		req = <-uio.Requests
		bb, ok := req.Message.(*udf.Request_Begin)
		if !ok {
			t.Errorf("expected begin message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Begin{
				Begin: bb.Begin,
			},
		}
		uio.Responses <- res

		// Point
		req = <-uio.Requests
		pt, ok := req.Message.(*udf.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Point{
				Point: pt.Point,
			},
		}
		uio.Responses <- res

		// End batch
		req = <-uio.Requests
		eb, ok := req.Message.(*udf.Request_End)
		if !ok {
			t.Errorf("expected end message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_End{
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
	b := models.Batch{
		Name: "test",
		Tags: models.Tags{"t1": "v1"},
		TMax: time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
		Points: []models.BatchPoint{{
			Fields: models.Fields{"f1": 1.0, "f2": 2.0, "f3": int64(1), "f4": "str"},
			Time:   time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
			Tags:   models.Tags{"t1": "v1", "t2": "v2"},
		}},
	}
	u.BatchIn() <- b
	rb := <-u.BatchOut()
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
