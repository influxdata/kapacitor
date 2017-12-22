package udf_test

import (
	"errors"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
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

func TestUDF_StartStop(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartStop")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)

	s.Start()

	close(u.Responses)
	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_StartInitStop(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartStop")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
		u.Responses <- res
		close(u.Responses)
	}()

	s.Start()
	err := s.Init(nil)
	if err != nil {
		t.Fatal(err)
	}

	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_StartInitAbort(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartInfoAbort")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	s.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*agent.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		s.Abort(expErr)
		close(u.Responses)
	}()
	err := s.Init(nil)
	if err != expErr {
		t.Fatal("expected explicit abort error")
	}
}

func TestUDF_StartInfoStop(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartInfoStop")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*agent.Request_Info)
		if !ok {
			t.Errorf("expected info message got %T", req.Message)
		}
		res := &agent.Response{
			Message: &agent.Response_Info{
				Info: &agent.InfoResponse{
					Wants:    agent.EdgeType_STREAM,
					Provides: agent.EdgeType_BATCH,
				},
			},
		}
		u.Responses <- res
		close(u.Responses)
	}()
	s.Start()
	info, err := s.Info()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := agent.EdgeType_STREAM, info.Wants; got != exp {
		t.Errorf("unexpected info.Wants got %v exp %v", got, exp)
	}
	if exp, got := agent.EdgeType_BATCH, info.Provides; got != exp {
		t.Errorf("unexpected info.Provides got %v exp %v", got, exp)
	}

	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_StartInfoAbort(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartInfoAbort")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	s.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*agent.Request_Info)
		if !ok {
			t.Error("expected info message")
		}
		s.Abort(expErr)
		close(u.Responses)
	}()
	_, err := s.Info()
	if err != expErr {
		t.Fatal("expected ErrUDFProcessAborted")
	}
}

func TestUDF_Keepalive(t *testing.T) {
	t.Parallel()
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_Keepalive")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, time.Millisecond*100, nil, nil)
	s.Start()
	s.Init(nil)
	req := <-u.Requests
	_, ok := req.Message.(*agent.Request_Init)
	if !ok {
		t.Error("expected init message")
	}
	select {
	case req = <-u.Requests:
	case <-time.After(time.Second):
		t.Fatal("expected keepalive message")
	}
	if req == nil {
		t.Fatal("expected keepalive message got nil, u was killed.")
	}
	_, ok = req.Message.(*agent.Request_Keepalive)
	if !ok {
		t.Errorf("expected keepalive message got %T", req.Message)
	}

	close(u.Responses)
	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_MissedKeepalive(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_MissedKeepalive")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, time.Millisecond*100, aborted, nil)
	s.Start()

	// Since the keepalive is missed, the process should abort on its own.
	for range u.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}

	close(u.Responses)
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_KillCallBack(t *testing.T) {
	t.Parallel()
	timeout := time.Millisecond * 100
	abortCalled := make(chan struct{})
	killCalled := make(chan struct{})
	aborted := func() {
		time.Sleep(timeout * 3)
		close(abortCalled)
	}
	kill := func() {
		close(killCalled)
	}

	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_MissedKeepalive")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, timeout, aborted, kill)
	s.Start()

	// Since the keepalive is missed, the process should abort on its own.
	for range u.Requests {
	}

	// Since abort takes a long time killCallback should be called
	select {
	case <-killCalled:
	case <-time.After(time.Second):
		t.Error("expected kill callback to be called")
	}

	close(u.Responses)
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_MissedKeepaliveInit(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_MissedKeepaliveInit")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, time.Millisecond*100, aborted, nil)
	s.Start()
	s.Init(nil)

	// Since the keepalive is missed, the process should abort on its own.
	for range u.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}
	close(u.Responses)
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_MissedKeepaliveInfo(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_MissedKeepaliveInfo")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, time.Millisecond*100, aborted, nil)
	s.Start()
	s.Info()

	// Since the keepalive is missed, the process should abort on its own.
	for range u.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}
	close(u.Responses)
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}

func TestUDF_SnapshotRestore(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_SnapshotRestore")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	go func() {
		// Init
		req := <-u.Requests
		_, ok := req.Message.(*agent.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		u.Responses <- &agent.Response{
			Message: &agent.Response_Init{
				Init: &agent.InitResponse{Success: true},
			},
		}
		// Snapshot
		req = <-u.Requests
		if req == nil {
			t.Fatal("expected snapshot message got nil")
		}
		_, ok = req.Message.(*agent.Request_Snapshot)
		if !ok {
			t.Errorf("expected snapshot message got %T", req.Message)
		}
		data := []byte{42}
		u.Responses <- &agent.Response{
			Message: &agent.Response_Snapshot{
				Snapshot: &agent.SnapshotResponse{Snapshot: data},
			},
		}
		// Restore
		req = <-u.Requests
		if req == nil {
			t.Fatal("expected restore message got nil")
		}
		restore, ok := req.Message.(*agent.Request_Restore)
		if !ok {
			t.Errorf("expected restore message got %T", req.Message)
		}
		if !reflect.DeepEqual(data, restore.Restore.Snapshot) {
			t.Errorf("unexpected restore snapshot got %v exp %v", restore.Restore.Snapshot, data)
		}
		u.Responses <- &agent.Response{
			Message: &agent.Response_Restore{
				Restore: &agent.RestoreResponse{Success: true},
			},
		}
		close(u.Responses)
	}()
	s.Start()
	s.Init(nil)
	snapshot, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	err = s.Restore(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}
func TestUDF_StartInitPointStop(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartPointStop")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
		u.Responses <- res
		req = <-u.Requests
		pt, ok := req.Message.(*agent.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Point{
				Point: pt.Point,
			},
		}
		u.Responses <- res
		close(u.Responses)
	}()

	s.Start()
	err := s.Init(nil)
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
	s.In() <- p
	rp := <-s.Out()
	if !reflect.DeepEqual(rp, p) {
		t.Errorf("unexpected returned point got: %v exp %v", rp, p)
	}

	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}
func TestUDF_StartInitBatchStop(t *testing.T) {
	u := udf_test.NewIO()
	d := kapacitorDiag.WithNodeContext("TestUDF_StartPointStop")
	s := udf.NewServer("testTask", "testNode", u.Out(), u.In(), d, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
		u.Responses <- res
		// Begin batch
		req = <-u.Requests
		bb, ok := req.Message.(*agent.Request_Begin)
		if !ok {
			t.Errorf("expected begin message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Begin{
				Begin: bb.Begin,
			},
		}
		u.Responses <- res

		// Point
		req = <-u.Requests
		pt, ok := req.Message.(*agent.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_Point{
				Point: pt.Point,
			},
		}
		u.Responses <- res

		// End batch
		req = <-u.Requests
		eb, ok := req.Message.(*agent.Request_End)
		if !ok {
			t.Errorf("expected end message got %T", req.Message)
		}
		res = &agent.Response{
			Message: &agent.Response_End{
				End: eb.End,
			},
		}
		u.Responses <- res
		close(u.Responses)
	}()

	s.Start()
	err := s.Init(nil)
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
	s.In() <- b
	rb := <-s.Out()
	if !reflect.DeepEqual(b, rb) {
		t.Errorf("unexpected returned batch got: %v exp %v", rb, b)
	}

	s.Stop()
	// read all requests and wait till the chan is closed
	for range u.Requests {
	}
	if err := <-u.ErrC; err != nil {
		t.Error(err)
	}
}
