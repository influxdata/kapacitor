package udf_test

import (
	"errors"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/udf"
	udf_test "github.com/influxdata/kapacitor/udf/test"
)

func TestUDF_StartStop(t *testing.T) {
	u := udf_test.NewIO()
	l := log.New(os.Stderr, "[TestUDF_StartStop] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)

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
	l := log.New(os.Stderr, "[TestUDF_StartStop] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
	l := log.New(os.Stderr, "[TestUDF_StartInfoAbort] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	s.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*udf.Request_Init)
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
	l := log.New(os.Stderr, "[TestUDF_StartInfoStop] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*udf.Request_Info)
		if !ok {
			t.Errorf("expected info message got %T", req.Message)
		}
		res := &udf.Response{
			Message: &udf.Response_Info{
				Info: &udf.InfoResponse{
					Wants:    udf.EdgeType_STREAM,
					Provides: udf.EdgeType_BATCH,
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
	if exp, got := udf.EdgeType_STREAM, info.Wants; got != exp {
		t.Errorf("unexpected info.Wants got %v exp %v", got, exp)
	}
	if exp, got := udf.EdgeType_BATCH, info.Provides; got != exp {
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
	l := log.New(os.Stderr, "[TestUDF_StartInfoAbort] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	s.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-u.Requests
		_, ok := req.Message.(*udf.Request_Info)
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
	l := log.New(os.Stderr, "[TestUDF_Keepalive] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, time.Millisecond*100, nil, nil)
	s.Start()
	s.Init(nil)
	req := <-u.Requests
	_, ok := req.Message.(*udf.Request_Init)
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
	_, ok = req.Message.(*udf.Request_Keepalive)
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
	l := log.New(os.Stderr, "[TestUDF_MissedKeepalive] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, time.Millisecond*100, aborted, nil)
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
	l := log.New(os.Stderr, "[TestUDF_MissedKeepalive] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, timeout, aborted, kill)
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
	l := log.New(os.Stderr, "[TestUDF_MissedKeepaliveInit] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, time.Millisecond*100, aborted, nil)
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
	l := log.New(os.Stderr, "[TestUDF_MissedKeepaliveInfo] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, time.Millisecond*100, aborted, nil)
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
	l := log.New(os.Stderr, "[TestUDF_SnapshotRestore] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	go func() {
		// Init
		req := <-u.Requests
		_, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		u.Responses <- &udf.Response{
			Message: &udf.Response_Init{
				Init: &udf.InitResponse{Success: true},
			},
		}
		// Snapshot
		req = <-u.Requests
		if req == nil {
			t.Fatal("expected snapshot message got nil")
		}
		_, ok = req.Message.(*udf.Request_Snapshot)
		if !ok {
			t.Errorf("expected snapshot message got %T", req.Message)
		}
		data := []byte{42}
		u.Responses <- &udf.Response{
			Message: &udf.Response_Snapshot{
				Snapshot: &udf.SnapshotResponse{Snapshot: data},
			},
		}
		// Restore
		req = <-u.Requests
		if req == nil {
			t.Fatal("expected restore message got nil")
		}
		restore, ok := req.Message.(*udf.Request_Restore)
		if !ok {
			t.Errorf("expected restore message got %T", req.Message)
		}
		if !reflect.DeepEqual(data, restore.Restore.Snapshot) {
			t.Errorf("unexpected restore snapshot got %v exp %v", restore.Restore.Snapshot, data)
		}
		u.Responses <- &udf.Response{
			Message: &udf.Response_Restore{
				Restore: &udf.RestoreResponse{Success: true},
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
	l := log.New(os.Stderr, "[TestUDF_StartPointStop] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
		u.Responses <- res
		req = <-u.Requests
		pt, ok := req.Message.(*udf.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Point{
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
	pt := models.Point{
		Name:            "test",
		Database:        "db",
		RetentionPolicy: "rp",
		Tags:            models.Tags{"t1": "v1", "t2": "v2"},
		Fields:          models.Fields{"f1": 1.0, "f2": 2.0},
		Time:            time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	s.PointIn() <- pt
	rpt := <-s.PointOut()
	if !reflect.DeepEqual(rpt, pt) {
		t.Errorf("unexpected returned point got: %v exp %v", rpt, pt)
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
	l := log.New(os.Stderr, "[TestUDF_StartPointStop] ", log.LstdFlags)
	s := udf.NewServer(u.Out(), u.In(), l, 0, nil, nil)
	go func() {
		req := <-u.Requests
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
		u.Responses <- res
		// Begin batch
		req = <-u.Requests
		bb, ok := req.Message.(*udf.Request_Begin)
		if !ok {
			t.Errorf("expected begin message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Begin{
				Begin: bb.Begin,
			},
		}
		u.Responses <- res

		// Point
		req = <-u.Requests
		pt, ok := req.Message.(*udf.Request_Point)
		if !ok {
			t.Errorf("expected point message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_Point{
				Point: pt.Point,
			},
		}
		u.Responses <- res

		// End batch
		req = <-u.Requests
		eb, ok := req.Message.(*udf.Request_End)
		if !ok {
			t.Errorf("expected end message got %T", req.Message)
		}
		res = &udf.Response{
			Message: &udf.Response_End{
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
	s.BatchIn() <- b
	rb := <-s.BatchOut()
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
