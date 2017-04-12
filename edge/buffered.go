package edge

type BufferedReceiver interface {
	Receiver
	// BufferedBatch processes an entire buffered batch.
	// Do not modify the batch or the slice of Points as it is shared.
	BufferedBatch(batch BufferedBatchMessage) error
}

// BatchBuffer buffers batch messages into a BufferedBatchMessage.
type BatchBuffer struct {
	begin  BeginBatchMessage
	points []BatchPointMessage
}

func (r *BatchBuffer) BeginBatch(begin BeginBatchMessage) error {
	r.begin = begin.ShallowCopy()
	r.points = make([]BatchPointMessage, 0, begin.SizeHint())
	return nil
}

func (r *BatchBuffer) BatchPoint(bp BatchPointMessage) error {
	r.points = append(r.points, bp)
	return nil
}

func (r *BatchBuffer) BufferedBatchMessage(end EndBatchMessage) BufferedBatchMessage {
	r.begin.SetSizeHint(len(r.points))
	return NewBufferedBatchMessage(r.begin, r.points, end)
}
