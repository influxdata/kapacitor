package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"os"
	"syscall"

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
func (*mirrorHandler) Info() (*agent.InfoResponse, error) {
	info := &agent.InfoResponse{
		Wants:    agent.EdgeType_STREAM,
		Provides: agent.EdgeType_STREAM,
		Options:  map[string]*agent.OptionInfo{},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (*mirrorHandler) Init(r *agent.InitRequest) (*agent.InitResponse, error) {
	init := &agent.InitResponse{
		Success: true,
		Error:   "",
	}
	return init, nil
}

// Create a snapshot of the running state of the process.
func (*mirrorHandler) Snapshot() (*agent.SnapshotResponse, error) {
	return &agent.SnapshotResponse{}, nil
}

// Restore a previous snapshot.
func (*mirrorHandler) Restore(req *agent.RestoreRequest) (*agent.RestoreResponse, error) {
	return &agent.RestoreResponse{
		Success: true,
	}, nil
}

// Start working with the next batch
func (*mirrorHandler) BeginBatch(begin *agent.BeginBatch) error {
	return errors.New("batching not supported")
}

func (h *mirrorHandler) Point(p *agent.Point) error {
	// Send back the point we just received
	h.agent.Responses <- &agent.Response{
		Message: &agent.Response_Point{
			Point: p,
		},
	}
	return nil
}

func (*mirrorHandler) EndBatch(end *agent.EndBatch) error {
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
