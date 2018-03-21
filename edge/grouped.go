package edge

import (
	"errors"

	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
)

// GroupedConsumer reads messages off an edge and passes them by group to receivers created from a grouped receiver.
type GroupedConsumer interface {
	Consumer
	// CardinalityVar is an exported var that indicates the current number of groups being managed.
	CardinalityVar() expvar.IntVar
}

// GroupedReceiver creates and deletes receivers as groups are created and deleted.
type GroupedReceiver interface {
	// NewGroup signals that a new group has been discovered in the data.
	// Information on the group and the message that first triggered its creation are provided.
	NewGroup(group GroupInfo, first PointMeta) (Receiver, error)
}

// GroupInfo identifies and contians information about a specific group.
type GroupInfo struct {
	ID         models.GroupID
	Tags       models.Tags
	Dimensions models.Dimensions
}

type groupedConsumer struct {
	consumer    Consumer
	gr          GroupedReceiver
	groups      map[models.GroupID]Receiver
	current     Receiver
	cardinality *expvar.Int
}

// NewGroupedConsumer creates a new grouped consumer for edge e and grouped receiver r.
func NewGroupedConsumer(e Edge, r GroupedReceiver) GroupedConsumer {
	gc := &groupedConsumer{
		gr:          r,
		groups:      make(map[models.GroupID]Receiver),
		cardinality: new(expvar.Int),
	}
	gc.consumer = NewConsumerWithReceiver(e, gc)
	return gc
}

func (c *groupedConsumer) Consume() error {
	return c.consumer.Consume()
}
func (c *groupedConsumer) CardinalityVar() expvar.IntVar {
	return c.cardinality
}

func (c *groupedConsumer) getOrCreateGroup(group GroupInfo, first PointMeta) (Receiver, error) {
	r, ok := c.groups[group.ID]
	if !ok {
		c.cardinality.Add(1)
		recv, err := c.gr.NewGroup(group, first)
		if err != nil {
			return nil, err
		}
		c.groups[group.ID] = recv
		r = recv
	}
	return r, nil
}

func (c *groupedConsumer) BeginBatch(begin BeginBatchMessage) error {
	r, err := c.getOrCreateGroup(begin.GroupInfo(), begin)
	if err != nil {
		return err
	}
	c.current = r
	return r.BeginBatch(begin)
}

func (c *groupedConsumer) BatchPoint(p BatchPointMessage) error {
	if c.current == nil {
		return errors.New("received batch point without batch")
	}
	return c.current.BatchPoint(p)
}

func (c *groupedConsumer) EndBatch(end EndBatchMessage) error {
	err := c.current.EndBatch(end)
	c.current = nil
	return err
}

func (c *groupedConsumer) BufferedBatch(batch BufferedBatchMessage) error {
	begin := batch.Begin()
	r, err := c.getOrCreateGroup(begin.GroupInfo(), begin)
	if err != nil {
		return err
	}
	return receiveBufferedBatch(r, batch)
}

func (c *groupedConsumer) Point(p PointMessage) error {
	r, err := c.getOrCreateGroup(p.GroupInfo(), p)
	if err != nil {
		return err
	}
	return r.Point(p)
}

func (c *groupedConsumer) Barrier(b BarrierMessage) error {
	r, err := c.getOrCreateGroup(b.GroupInfo(), b)
	if err != nil {
		return err
	}
	return r.Barrier(b)
}

func (c *groupedConsumer) DeleteGroup(d DeleteGroupMessage) error {
	id := d.GroupID()
	r, ok := c.groups[id]
	if ok {
		delete(c.groups, id)
		c.cardinality.Add(-1)
		return r.DeleteGroup(d)
	}
	return nil
}
func (c *groupedConsumer) Done() {
	for _, r := range c.groups {
		r.Done()
	}
}
