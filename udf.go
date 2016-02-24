package kapacitor

import (
	"errors"
	"log"
	"sync"

	"github.com/influxdata/kapacitor/pipeline"
)

// User defined function
type UDFNode struct {
	node
	u       *pipeline.UDFNode
	process *UDFProcess
	aborted chan struct{}

	wg      sync.WaitGroup
	mu      sync.Mutex
	stopped bool
}

// Create a new UDFNode that sends incoming data to child process
func newUDFNode(et *ExecutingTask, n *pipeline.UDFNode, l *log.Logger) (*UDFNode, error) {
	un := &UDFNode{
		node:    node{Node: n, et: et, logger: l},
		u:       n,
		aborted: make(chan struct{}),
	}
	un.process = NewUDFProcess(
		n.Commander,
		l,
		n.Timeout,
		un.abortedCallback,
	)

	un.node.runF = un.runUDF
	un.node.stopF = un.stopUDF
	return un, nil
}

var errNodeAborted = errors.New("node aborted")

func (u *UDFNode) stopUDF() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if !u.stopped {
		u.stopped = true
		u.process.Abort(errNodeAborted)
	}
}

func (u *UDFNode) runUDF(snapshot []byte) (err error) {
	defer func() {
		u.mu.Lock()
		defer u.mu.Unlock()
		//Ignore stopped errors if the process was stopped externally
		if u.stopped && (err == ErrUDFProcessStopped || err == errNodeAborted) {
			err = nil
		}
		u.stopped = true
	}()
	err = u.process.Start()
	if err != nil {
		return
	}
	err = u.process.Init(u.u.Options)
	if err != nil {
		return
	}
	if snapshot != nil {
		err = u.process.Restore(snapshot)
		if err != nil {
			return
		}
	}
	forwardErr := make(chan error, 1)
	go func() {
		switch u.Provides() {
		case pipeline.StreamEdge:
			for p := range u.process.PointOut {
				for _, out := range u.outs {
					err := out.CollectPoint(p)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		case pipeline.BatchEdge:
			for b := range u.process.BatchOut {
				for _, out := range u.outs {
					err := out.CollectBatch(b)
					if err != nil {
						forwardErr <- err
						return
					}
				}
			}
		}
		forwardErr <- nil
	}()

	// The abort callback needs to know when we are done writing
	// so we wrap in a wait group.
	u.wg.Add(1)
	go func() {
		defer u.wg.Done()
		switch u.Wants() {
		case pipeline.StreamEdge:
			for p, ok := u.ins[0].NextPoint(); ok; p, ok = u.ins[0].NextPoint() {
				u.timer.Start()
				select {
				case u.process.PointIn <- p:
				case <-u.aborted:
					return
				}
				u.timer.Stop()
			}
		case pipeline.BatchEdge:
			for b, ok := u.ins[0].NextBatch(); ok; b, ok = u.ins[0].NextBatch() {
				u.timer.Start()
				select {
				case u.process.BatchIn <- b:
				case <-u.aborted:
					return
				}
				u.timer.Stop()
			}
		}
	}()
	// wait till we are done writing
	u.wg.Wait()

	// Stop the process
	err = u.process.Stop()
	if err != nil {
		return
	}
	// Wait/Return any error from the forwarding goroutine
	err = <-forwardErr
	return
}

func (u *UDFNode) abortedCallback() {
	close(u.aborted)
	// wait till we are done writing
	u.wg.Wait()
}

func (u *UDFNode) snapshot() ([]byte, error) {
	return u.process.Snapshot()
}
