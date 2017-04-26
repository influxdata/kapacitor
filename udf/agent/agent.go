package agent

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sync"
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
	Info() (*InfoResponse, error)
	// Initialize the Handler with the provided options.
	Init(*InitRequest) (*InitResponse, error)
	// Create a snapshot of the running state of the handler.
	Snapshot() (*SnapshotResponse, error)
	// Restore a previous snapshot.
	Restore(*RestoreRequest) (*RestoreResponse, error)

	// A batch has begun.
	BeginBatch(*BeginBatch) error
	// A point has arrived.
	Point(*Point) error
	// The batch is complete.
	EndBatch(*EndBatch) error

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
	outResponses chan *Response

	responses chan *Response
	// A channel for writing Responses, specifically Batch and Point responses.
	Responses chan<- *Response

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
		outResponses: make(chan *Response),
		responses:    make(chan *Response),
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
			a.outResponses <- &Response{
				Message: &Response_Error{
					Error: &ErrorResponse{Error: err.Error()},
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
	request := &Request{}
	for {
		err := ReadMessage(&buf, in, request)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Hand message to handler
		var res *Response
		switch msg := request.Message.(type) {
		case *Request_Info:
			info, err := a.Handler.Info()
			if err != nil {
				return err
			}
			res = &Response{}
			res.Message = &Response_Info{
				Info: info,
			}
		case *Request_Init:
			init, err := a.Handler.Init(msg.Init)
			if err != nil {
				return err
			}
			res = &Response{}
			res.Message = &Response_Init{
				Init: init,
			}
		case *Request_Keepalive:
			res = &Response{
				Message: &Response_Keepalive{
					Keepalive: &KeepaliveResponse{
						Time: msg.Keepalive.Time,
					},
				},
			}
		case *Request_Snapshot:
			snapshot, err := a.Handler.Snapshot()
			if err != nil {
				return err
			}
			res = &Response{}
			res.Message = &Response_Snapshot{
				Snapshot: snapshot,
			}
		case *Request_Restore:
			restore, err := a.Handler.Restore(msg.Restore)
			if err != nil {
				return err
			}
			res = &Response{}
			res.Message = &Response_Restore{
				Restore: restore,
			}
		case *Request_Begin:
			err := a.Handler.BeginBatch(msg.Begin)
			if err != nil {
				return err
			}
		case *Request_Point:
			err := a.Handler.Point(msg.Point)
			if err != nil {
				return err
			}
		case *Request_End:
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
		err := WriteMessage(response, a.out)
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
