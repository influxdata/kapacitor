package agent

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/influxdata/kapacitor/udf"
)

// The Agent calls the appropriate methods on the Handler as it receives requests over a socket.
//
// Returning an error from any method will cause the Agent to stop and an ErrorResponse to be sent.
// Some *Response objects (like SnapshotResponse) allow for returning their own error within the object itself.
// These types of errors will not stop the Agent and Kapacitor will deal with them appropriately.
//
// The Handler is called from a single goroutine, meaning methods will not be called concurrently.
//
// To write Points/Batches back to the Agent/Kapacitor use the Agent.Responses channel.
type Handler interface {
	// Return the InfoResponse. Describing the properties of this Handler
	Info() (*udf.InfoResponse, error)
	// Initialize the Handler with the provided options.
	Init(*udf.InitRequest) (*udf.InitResponse, error)
	// Create a snapshot of the running state of the handler.
	Snaphost() (*udf.SnapshotResponse, error)
	// Restore a previous snapshot.
	Restore(*udf.RestoreRequest) (*udf.RestoreResponse, error)

	// A batch has begun.
	BeginBatch(*udf.BeginBatch) error
	// A point has arrived.
	Point(*udf.Point) error
	// The batch is complete.
	EndBatch(*udf.EndBatch) error

	// Gracefully stop the Handler.
	// No other methods will be called.
	Stop()
}

// Go implementation of a Kapacitor UDF agent.
// This agent is responsible for reading and writing
// messages over a socket.
//
// The Agent requires a Handler object in order to fulfill requests.
type Agent struct {
	in  io.ReadCloser
	out io.WriteCloser

	outGroup     sync.WaitGroup
	outResponses chan *udf.Response

	responses chan *udf.Response
	// A channel for writing Responses, specifically Batch and Point responses.
	Responses chan<- *udf.Response

	writeErrC chan error
	readErrC  chan error

	// The handler for requests.
	Handler Handler
}

// Create a new Agent is the provided in/out objects.
// To create an Agent that reads from STDIN/STDOUT of the process use New(os.Stdin, os.Stdout)
func New(in io.ReadCloser, out io.WriteCloser) *Agent {
	s := &Agent{
		in:           in,
		out:          out,
		outResponses: make(chan *udf.Response),
		responses:    make(chan *udf.Response),
	}
	s.Responses = s.responses
	return s
}

// Start the Agent, you must set an Handler on the agent before starting.
func (a *Agent) Start() error {
	if a.Handler == nil {
		return errors.New("must set a Handler on the agent before starting")
	}

	a.readErrC = make(chan error, 1)
	a.writeErrC = make(chan error, 1)
	a.outGroup.Add(1)
	go func() {
		defer a.outGroup.Done()
		err := a.readLoop()
		if err != nil {
			a.outResponses <- &udf.Response{
				Message: &udf.Response_Error{
					Error: &udf.ErrorResponse{Error: err.Error()},
				},
			}
		}
		a.readErrC <- err
	}()
	go func() {
		a.writeErrC <- a.writeLoop()
	}()

	a.outGroup.Add(1)
	go func() {
		defer a.outGroup.Done()
		a.forwardResponses()
	}()

	return nil
}

// Wait for the Agent to terminate.
// The Agent will not terminate till the Responses channel is closed.
// You will need to close this channel externally, typically in the Stop method for the Handler.
// The Agent will terminate if the In reader is closed or an error occurs.
func (a *Agent) Wait() error {
	a.outGroup.Wait()
	close(a.outResponses)
	for a.readErrC != nil || a.writeErrC != nil {
		select {
		case err := <-a.readErrC:
			a.readErrC = nil
			if err != nil {
				return fmt.Errorf("read error: %s", err)
			}
		case err := <-a.writeErrC:
			a.writeErrC = nil
			if err != nil {
				return fmt.Errorf("write error: %s", err)
			}
		}
	}
	return nil
}

func (a *Agent) readLoop() error {
	defer a.Handler.Stop()
	defer a.in.Close()
	in := bufio.NewReader(a.in)
	var buf []byte
	request := &udf.Request{}
	for {
		err := udf.ReadMessage(&buf, in, request)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Hand message to handler
		var res *udf.Response
		switch msg := request.Message.(type) {
		case *udf.Request_Info:
			info, err := a.Handler.Info()
			if err != nil {
				return err
			}
			res = &udf.Response{}
			res.Message = &udf.Response_Info{
				Info: info,
			}
		case *udf.Request_Init:
			init, err := a.Handler.Init(msg.Init)
			if err != nil {
				return err
			}
			res = &udf.Response{}
			res.Message = &udf.Response_Init{
				Init: init,
			}
		case *udf.Request_Keepalive:
			res = &udf.Response{
				Message: &udf.Response_Keepalive{
					Keepalive: &udf.KeepaliveResponse{
						Time: msg.Keepalive.Time,
					},
				},
			}
		case *udf.Request_Snapshot:
			snapshot, err := a.Handler.Snaphost()
			if err != nil {
				return err
			}
			res = &udf.Response{}
			res.Message = &udf.Response_Snapshot{
				Snapshot: snapshot,
			}
		case *udf.Request_Restore:
			restore, err := a.Handler.Restore(msg.Restore)
			if err != nil {
				return err
			}
			res = &udf.Response{}
			res.Message = &udf.Response_Restore{
				Restore: restore,
			}
		case *udf.Request_Begin:
			err := a.Handler.BeginBatch(msg.Begin)
			if err != nil {
				return err
			}
		case *udf.Request_Point:
			err := a.Handler.Point(msg.Point)
			if err != nil {
				return err
			}
		case *udf.Request_End:
			err := a.Handler.EndBatch(msg.End)
			if err != nil {
				return err
			}
		}
		if res != nil {
			a.outResponses <- res
		}
	}
	return nil
}

func (a *Agent) writeLoop() error {
	defer a.out.Close()
	for response := range a.outResponses {
		err := udf.WriteMessage(response, a.out)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) forwardResponses() {
	for r := range a.responses {
		a.outResponses <- r
	}
}
