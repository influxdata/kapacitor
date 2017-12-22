package edge

import (
	"errors"
	"sync"

	"github.com/influxdata/kapacitor/pipeline"
)

// Edge represents the connection between two nodes that communicate via messages.
// Edge communication is unidirectional and asynchronous.
// Edges are safe for concurrent use.
type Edge interface {
	// Collect instructs the edge to accept a new message.
	Collect(Message) error
	// Emit blocks until a message is available and returns it or returns false if the edge has been closed or aborted.
	Emit() (Message, bool)
	// Close stops the edge, all messages currently buffered will be processed.
	// Future calls to Collect will panic.
	Close() error
	// Abort immediately stops the edge and all currently buffered messages are dropped.
	// Future calls to Collect return the error ErrAborted.
	Abort()
	// Type indicates whether the edge will emit stream or batch data.
	Type() pipeline.EdgeType
}

type edgeState int

const (
	edgeOpen edgeState = iota
	edgeClosed
	edgeAborted
)

// channelEdge is an implementation of Edge using channels.
type channelEdge struct {
	aborting chan struct{}
	messages chan Message

	typ pipeline.EdgeType

	mu    sync.Mutex
	state edgeState
}

// NewChannelEdge returns a new edge that uses channels as the underlying transport.
func NewChannelEdge(typ pipeline.EdgeType, size int) Edge {
	return &channelEdge{
		aborting: make(chan struct{}),
		messages: make(chan Message, size),
		state:    edgeOpen,
		typ:      typ,
	}
}

func (e *channelEdge) Collect(m Message) error {
	select {
	case e.messages <- m:
		return nil
	case <-e.aborting:
		return ErrAborted
	}
}

func (e *channelEdge) Emit() (m Message, ok bool) {
	select {
	case m, ok = <-e.messages:
	case <-e.aborting:
	}
	return
}

func (e *channelEdge) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != edgeOpen {
		return errors.New("edge not open cannot close")
	}
	close(e.messages)
	e.state = edgeClosed
	return nil
}

func (e *channelEdge) Abort() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == edgeAborted {
		//nothing to do, already aborted
		return
	}
	close(e.aborting)
	e.state = edgeAborted
}

func (e *channelEdge) Type() pipeline.EdgeType {
	return e.typ
}
