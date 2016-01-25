package kapacitor_test

import (
	"errors"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/kapacitor"
	cmd_test "github.com/influxdata/kapacitor/command/test"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/udf"
)

func TestProcess_StartStop(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_StartStop] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)

	p.Start()

	close(cmd.Responses)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_StartInitStop(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_StartStop] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	go func() {
		req := <-cmd.Requests
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
		cmd.Responses <- res
		close(cmd.Responses)
	}()

	p.Start()
	err := p.Init(nil)
	if err != nil {
		t.Fatal(err)
	}

	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_StartInitAbort(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_StartInfoAbort] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	p.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-cmd.Requests
		_, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		p.Abort(expErr)
		close(cmd.Responses)
	}()
	err := p.Init(nil)
	if err != expErr {
		t.Fatal("expected explicit abort error")
	}
}

func TestProcess_StartInfoStop(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_StartInfoStop] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	go func() {
		req := <-cmd.Requests
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
		cmd.Responses <- res
		close(cmd.Responses)
	}()
	p.Start()
	info, err := p.Info()
	if err != nil {
		t.Fatal(err)
	}
	if exp, got := pipeline.StreamEdge, info.Wants; got != exp {
		t.Errorf("unexpected info.Wants got %v exp %v", got, exp)
	}
	if exp, got := pipeline.BatchEdge, info.Provides; got != exp {
		t.Errorf("unexpected info.Provides got %v exp %v", got, exp)
	}

	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_StartInfoAbort(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_StartInfoAbort] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	p.Start()
	expErr := errors.New("explicit abort")
	go func() {
		req := <-cmd.Requests
		_, ok := req.Message.(*udf.Request_Info)
		if !ok {
			t.Error("expected info message")
		}
		p.Abort(expErr)
		close(cmd.Responses)
	}()
	_, err := p.Info()
	if err != expErr {
		t.Fatal("expected ErrUDFProcessAborted")
	}
}

func TestProcess_Keepalive(t *testing.T) {
	t.Parallel()
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_Keepalive] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, time.Millisecond*100, nil)
	p.Start()
	p.Init(nil)
	req := <-cmd.Requests
	_, ok := req.Message.(*udf.Request_Init)
	if !ok {
		t.Error("expected init message")
	}
	select {
	case req = <-cmd.Requests:
	case <-time.After(time.Second):
		t.Fatal("expected keepalive message")
	}
	if req == nil {
		t.Fatal("expected keepalive message got nil, cmd was killed.")
	}
	_, ok = req.Message.(*udf.Request_Keepalive)
	if !ok {
		t.Errorf("expected keepalive message got %T", req.Message)
	}

	close(cmd.Responses)
	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_MissedKeepalive(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_MissedKeepalive] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, time.Millisecond*100, aborted)
	p.Start()

	// Since the keepalive is missed, the process should abort on its own.
	for range cmd.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}

	close(cmd.Responses)
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_MissedKeepaliveInit(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_MissedKeepaliveInit] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, time.Millisecond*100, aborted)
	p.Start()
	p.Init(nil)

	// Since the keepalive is missed, the process should abort on its own.
	for range cmd.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}
	close(cmd.Responses)
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_MissedKeepaliveInfo(t *testing.T) {
	t.Parallel()
	abortCalled := make(chan struct{})
	aborted := func() {
		close(abortCalled)
	}

	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_MissedKeepaliveInfo] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, time.Millisecond*100, aborted)
	p.Start()
	p.Info()

	// Since the keepalive is missed, the process should abort on its own.
	for range cmd.Requests {
	}

	select {
	case <-abortCalled:
	case <-time.After(time.Second):
		t.Error("expected abort callback to be called")
	}
	close(cmd.Responses)
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}

func TestProcess_SnapshotRestore(t *testing.T) {
	cmd := cmd_test.NewCommandHelper()
	l := log.New(os.Stderr, "[TestProcess_SnapshotRestore] ", log.LstdFlags)
	p := kapacitor.NewUDFProcess(cmd, l, 0, nil)
	go func() {
		// Init
		req := <-cmd.Requests
		_, ok := req.Message.(*udf.Request_Init)
		if !ok {
			t.Error("expected init message")
		}
		cmd.Responses <- &udf.Response{
			Message: &udf.Response_Init{
				Init: &udf.InitResponse{Success: true},
			},
		}
		// Snapshot
		req = <-cmd.Requests
		if req == nil {
			t.Fatal("expected snapshot message got nil")
		}
		_, ok = req.Message.(*udf.Request_Snapshot)
		if !ok {
			t.Errorf("expected snapshot message got %T", req.Message)
		}
		data := []byte{42}
		cmd.Responses <- &udf.Response{
			Message: &udf.Response_Snapshot{
				Snapshot: &udf.SnapshotResponse{data},
			},
		}
		// Restore
		req = <-cmd.Requests
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
		cmd.Responses <- &udf.Response{
			Message: &udf.Response_Restore{
				Restore: &udf.RestoreResponse{Success: true},
			},
		}
		close(cmd.Responses)
	}()
	p.Start()
	p.Init(nil)
	snapshot, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	err = p.Restore(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	p.Stop()
	// read all requests and wait till the chan is closed
	for range cmd.Requests {
	}
	if err := <-cmd.ErrC; err != nil {
		t.Error(err)
	}
}
