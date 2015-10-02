package kapacitor

import (
	"fmt"

	"github.com/influxdb/kapacitor/pipeline"
)

// A node that can be  in an executor.
type Node interface {
	pipeline.Node

	addParentEdge(*Edge)

	// start the node and its children
	start()
	stop()

	// wait for the node to finish processing and return any errors
	Err() error

	// link specified child
	linkChild(c Node) error

	// close children edges
	closeChildEdges()
	closeParentEdges()
}

//implementation of Node
type node struct {
	pipeline.Node
	et       *ExecutingTask
	parents  []Node
	children []Node
	runF     func() error
	stopF    func()
	errCh    chan error
	ins      []*Edge
	outs     []*Edge
}

func (n *node) addParentEdge(e *Edge) {
	n.ins = append(n.ins, e)
}

func (n *node) closeParentEdges() {
	for _, in := range n.ins {
		in.Close()
	}
}

func (n *node) start() {
	n.errCh = make(chan error, 1)
	go func() {
		var err error
		defer func() {
			// Always close children edges
			n.closeChildEdges()
			// Handle panic in runF
			r := recover()
			if r != nil {
				err = fmt.Errorf("%s: %s", n.Name(), r)
			}
			// Propogate error up
			if err != nil {
				n.closeParentEdges()
			}
			n.errCh <- err
		}()
		// Run node
		err = n.runF()
	}()
}

func (n *node) stop() {
	if n.stopF != nil {
		n.stopF()
	}
	n.closeChildEdges()
}

func (n *node) Err() error {
	return <-n.errCh
}

func (n *node) addChild(c Node) (*Edge, error) {
	if n.Provides() != c.Wants() {
		return nil, fmt.Errorf("cannot add child mismatched edges: %s -> %s", n.Provides(), c.Wants())
	}
	n.children = append(n.children, c)

	edge := newEdge(fmt.Sprintf("%s->%s", n.Name(), c.Name()), n.Provides())
	if edge == nil {
		return nil, fmt.Errorf("unknown edge type %s", n.Provides())
	}
	c.addParentEdge(edge)
	return edge, nil
}

func (n *node) linkChild(c Node) error {

	// add child
	edge, err := n.addChild(c)
	if err != nil {
		return err
	}

	// store edge to child
	n.outs = append(n.outs, edge)
	return nil
}

func (n *node) closeChildEdges() {
	for _, child := range n.outs {
		child.Close()
	}
}
