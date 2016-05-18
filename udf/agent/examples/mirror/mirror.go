package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"syscall"

	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
)

// Mirrors all points it receives back to Kapacitor
type mirrorHandler struct {
	agent *agent.Agent
}

func newMirrorHandler(agent *agent.Agent) *mirrorHandler {
	return &mirrorHandler{agent: agent}
}

// Return the InfoResponse. Describing the properties of this UDF agent.
func (*mirrorHandler) Info() (*udf.InfoResponse, error) {
	info := &udf.InfoResponse{
		Wants:    udf.EdgeType_STREAM,
		Provides: udf.EdgeType_STREAM,
		Options:  map[string]*udf.OptionInfo{},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (*mirrorHandler) Init(r *udf.InitRequest) (*udf.InitResponse, error) {
	init := &udf.InitResponse{
		Success: true,
		Error:   "",
	}
	return init, nil
}

// Create a snapshot of the running state of the process.
func (*mirrorHandler) Snaphost() (*udf.SnapshotResponse, error) {
	return &udf.SnapshotResponse{}, nil
}

// Restore a previous snapshot.
func (*mirrorHandler) Restore(req *udf.RestoreRequest) (*udf.RestoreResponse, error) {
	return &udf.RestoreResponse{
		Success: true,
	}, nil
}

// Start working with the next batch
func (*mirrorHandler) BeginBatch(begin *udf.BeginBatch) error {
	return errors.New("batching not supported")
}

func (h *mirrorHandler) Point(p *udf.Point) error {
	// Send back the point we just received
	h.agent.Responses <- &udf.Response{
		Message: &udf.Response_Point{
			Point: p,
		},
	}
	return nil
}

func (*mirrorHandler) EndBatch(end *udf.EndBatch) error {
	return nil
}

// Stop the handler gracefully.
func (h *mirrorHandler) Stop() {
	close(h.agent.Responses)
}

type accepter struct {
	count int64
}

// Create a new agent/handler for each new connection.
// Count and log each new connection and termination.
func (acc *accepter) Accept(conn net.Conn) {
	count := acc.count
	acc.count++
	a := agent.New(conn, conn)
	h := newMirrorHandler(a)
	a.Handler = h

	log.Println("Starting agent for connection", count)
	a.Start()
	go func() {
		err := a.Wait()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Agent for connection %d finished", count)
	}()
}

var socketPath = flag.String("socket", "/tmp/mirror.sock", "Where to create the unix socket")

func main() {
	flag.Parse()

	// Create unix socket
	addr, err := net.ResolveUnixAddr("unix", *socketPath)
	if err != nil {
		log.Fatal(err)
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		log.Fatal(err)
	}

	// Create server that listens on the socket
	s := agent.NewServer(l, &accepter{})

	// Setup signal handler to stop Server on various signals
	s.StopOnSignals(os.Interrupt, syscall.SIGTERM)

	log.Println("Server listening on", addr.String())
	err = s.Serve()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Server stopped")
}
