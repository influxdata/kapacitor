package edge

// ForwardReceiver handles messages as they arrive and can return a message to be forwarded to output edges.
// If a returned messages is nil, no message is forwarded.
type ForwardReceiver interface {
	BeginBatch(begin BeginBatchMessage) (Message, error)
	BatchPoint(bp BatchPointMessage) (Message, error)
	EndBatch(end EndBatchMessage) (Message, error)
	Point(p PointMessage) (Message, error)
	Barrier(b BarrierMessage) (Message, error)
	DeleteGroup(d DeleteGroupMessage) (Message, error)

	// Done is called once the receiver will no longer receive any messages.
	Done()
}

// ForwardBufferedReceiver handles messages as they arrive and can return a message to be forwarded to output edges.
// If a returned messages is nil, no message is forwarded.
type ForwardBufferedReceiver interface {
	ForwardReceiver
	BufferedBatch(batch BufferedBatchMessage) (Message, error)
}

// NewReceiverFromForwardReceiverWithStats creates a new receiver from the provided list of stats edges and forward receiver.
func NewReceiverFromForwardReceiverWithStats(outs []StatsEdge, r ForwardReceiver) Receiver {
	os := make([]Edge, len(outs))
	for i := range outs {
		os[i] = outs[i]
	}
	return NewReceiverFromForwardReceiver(os, r)
}

// NewReceiverFromForwardReceiver creates a new receiver from the provided list of edges and forward receiver.
func NewReceiverFromForwardReceiver(outs []Edge, r ForwardReceiver) Receiver {
	b, ok := r.(ForwardBufferedReceiver)
	if ok {
		return &forwardingBufferedReceiver{
			forwardingReceiver: forwardingReceiver{
				outs: outs,
				r:    r,
			},
			b: b,
		}
	}
	return &forwardingReceiver{
		outs: outs,
		r:    r,
	}
}

type forwardingReceiver struct {
	outs []Edge
	r    ForwardReceiver
}

type forwardingBufferedReceiver struct {
	forwardingReceiver
	b ForwardBufferedReceiver
}

func (fr *forwardingReceiver) BeginBatch(begin BeginBatchMessage) error {
	return fr.forward(fr.r.BeginBatch(begin))
}
func (fr *forwardingReceiver) BatchPoint(bp BatchPointMessage) error {
	return fr.forward(fr.r.BatchPoint(bp))
}
func (fr *forwardingReceiver) EndBatch(end EndBatchMessage) error {
	return fr.forward(fr.r.EndBatch(end))
}

func (fr *forwardingBufferedReceiver) BufferedBatch(batch BufferedBatchMessage) error {
	return fr.forward(fr.b.BufferedBatch(batch))
}

func (fr *forwardingReceiver) Point(p PointMessage) error {
	return fr.forward(fr.r.Point(p))
}
func (fr *forwardingReceiver) Barrier(b BarrierMessage) error {
	return fr.forward(fr.r.Barrier(b))
}
func (fr *forwardingReceiver) DeleteGroup(d DeleteGroupMessage) error {
	return fr.forward(fr.r.DeleteGroup(d))
}
func (fr *forwardingReceiver) Done() {
	fr.r.Done()
}

func (fr *forwardingReceiver) forward(msg Message, err error) error {
	if err != nil {
		return err
	}
	if msg != nil {
		for _, out := range fr.outs {
			if err := out.Collect(msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func Forward(outs []StatsEdge, msg Message) error {
	for _, out := range outs {
		if err := out.Collect(msg); err != nil {
			return err
		}
	}
	return nil
}
