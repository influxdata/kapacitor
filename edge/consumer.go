package edge

import (
	"fmt"
)

// Consumer reads messages off an edge and passes them to a receiver.
type Consumer interface {
	// Consume reads messages off an edge until the edge is closed or aborted.
	// An error is returned if either the edge or receiver errors.
	Consume() error
}

// Receiver handles messages as they arrive via a consumer.
type Receiver interface {
	BeginBatch(begin BeginBatchMessage) error
	BatchPoint(bp BatchPointMessage) error
	EndBatch(end EndBatchMessage) error
	Point(p PointMessage) error
	Barrier(b BarrierMessage) error
	DeleteGroup(d DeleteGroupMessage) error

	// Done is called once the receiver will no longer receive any messages.
	Done()
}

type consumer struct {
	edge Edge
	r    Receiver
}

// NewConsumerWithReceiver creates a new consumer for the edge e and receiver r.
func NewConsumerWithReceiver(e Edge, r Receiver) Consumer {
	return &consumer{
		edge: e,
		r:    r,
	}
}

func (ec *consumer) Consume() error {
	defer ec.r.Done()
	for msg, ok := ec.edge.Emit(); ok; msg, ok = ec.edge.Emit() {
		switch m := msg.(type) {
		case BeginBatchMessage:
			if err := ec.r.BeginBatch(m); err != nil {
				return err
			}
		case BatchPointMessage:
			if err := ec.r.BatchPoint(m); err != nil {
				return err
			}
		case EndBatchMessage:
			if err := ec.r.EndBatch(m); err != nil {
				return err
			}
		case BufferedBatchMessage:
			err := receiveBufferedBatch(ec.r, m)
			if err != nil {
				return err
			}
		case PointMessage:
			if err := ec.r.Point(m); err != nil {
				return err
			}
		case BarrierMessage:
			if err := ec.r.Barrier(m); err != nil {
				return err
			}
		case DeleteGroupMessage:
			if err := ec.r.DeleteGroup(m); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message of type %T", msg)
		}
	}
	return nil
}

func receiveBufferedBatch(r Receiver, batch BufferedBatchMessage) error {
	b, ok := r.(BufferedReceiver)
	// If we have a buffered receiver pass the batch straight through.
	if ok {
		return b.BufferedBatch(batch)
	}

	// Pass the batch non buffered.
	if err := r.BeginBatch(batch.Begin()); err != nil {
		return err
	}
	for _, bp := range batch.Points() {
		if err := r.BatchPoint(bp); err != nil {
			return err
		}
	}
	return r.EndBatch(batch.End())
}

type MultiReceiver interface {
	BufferedBatch(src int, batch BufferedBatchMessage) error
	Point(src int, p PointMessage) error
	Barrier(src int, b BarrierMessage) error
	Finish() error
}

func NewMultiConsumerWithStats(ins []StatsEdge, r MultiReceiver) Consumer {
	edges := make([]Edge, len(ins))
	for i := range ins {
		edges[i] = ins[i]
	}
	return NewMultiConsumer(edges, r)
}

func NewMultiConsumer(ins []Edge, r MultiReceiver) Consumer {
	return &multiConsumer{
		ins:      ins,
		r:        r,
		messages: make(chan srcMessage),
	}
}

type multiConsumer struct {
	ins []Edge

	r MultiReceiver

	messages chan srcMessage
}

type srcMessage struct {
	Src int
	Msg Message
}

func (c *multiConsumer) Consume() error {
	errC := make(chan error, len(c.ins))
	for i, in := range c.ins {
		go func(src int, in Edge) {
			errC <- c.readEdge(src, in)
		}(i, in)
	}

	firstErr := make(chan error, 1)
	go func() {
		for range c.ins {
			err := <-errC
			if err != nil {
				firstErr <- err
			}
		}
		// Close messages now that all readEdge goroutines have finished.
		close(c.messages)
	}()

LOOP:
	for {
		select {
		case err := <-firstErr:
			// One of the parents errored out, return the error.
			return err
		case m, ok := <-c.messages:
			if !ok {
				break LOOP
			}
			switch msg := m.Msg.(type) {
			case BufferedBatchMessage:
				if err := c.r.BufferedBatch(m.Src, msg); err != nil {
					return err
				}
			case PointMessage:
				if err := c.r.Point(m.Src, msg); err != nil {
					return err
				}
			case BarrierMessage:
				if err := c.r.Barrier(m.Src, msg); err != nil {
					return err
				}
			}
		}
	}

	return c.r.Finish()
}

func (c *multiConsumer) readEdge(src int, in Edge) error {
	batchBuffer := new(BatchBuffer)
	for m, ok := in.Emit(); ok; m, ok = in.Emit() {
		switch msg := m.(type) {
		case BeginBatchMessage:
			if err := batchBuffer.BeginBatch(msg); err != nil {
				return err
			}
		case BatchPointMessage:
			if err := batchBuffer.BatchPoint(msg); err != nil {
				return err
			}
		case EndBatchMessage:
			batch := batchBuffer.BufferedBatchMessage(msg)
			c.messages <- srcMessage{
				Src: src,
				Msg: batch,
			}
		default:
			c.messages <- srcMessage{
				Src: src,
				Msg: msg,
			}
		}
	}
	return nil
}
