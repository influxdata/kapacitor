package kapacitor

// CircularQueue defines a circular queue, always use the contructor to create one.
type CircularQueue[T any] struct {
	data []T
	head int
	tail int
	Len  int
	Peek *T
}

func NewCircularQueue[T any](buf ...T) *CircularQueue[T] {
	// if we have a useless buffer, make one that is at least useful
	if cap(buf) < 4 {
		buf = append(make([]T, 0, 4), buf...)
	}
	return &CircularQueue[T]{
		data: buf[:cap(buf)],
		tail: len(buf), // tail is here we insert
		Len:  len(buf),
	}
}

// Enqueue adds an item to the queue.
func Enqueue[T any](q *CircularQueue[T], v T) {
	// if full we must grow and insert together. This is an expensive op
	if cap(q.data) > q.Len { // no need to grow
		if q.tail == len(q.data) {
			q.tail = 0
		}
		q.data[q.tail] = v
	} else { // we need to grow
		buf := make([]T, cap(q.data)*2)
		if q.head < q.tail {
			copy(buf, q.data[q.head:q.tail])
		} else {
			partialWriteLen := copy(buf, q.data[q.head:])
			copy(buf[partialWriteLen:], q.data[:q.tail])
		}
		q.head = 0
		q.tail = cap(q.data)
		buf[q.tail] = v
		q.data = buf
	}
	q.Len++
	q.tail++
	return
}

// Dequeue removes n items from the queue. If n is longer than the number of the items in the queue it will clear them all out.
func (q *CircularQueue[T]) Dequeue(n int) {
	if n <= 0 {
		return
	}
	if q.Len <= n {
		n = q.Len
	}
	ni := n
	var fill T
	if q.head > q.tail {
		for i := q.head; i < len(q.data) && ni > 0; i++ {
			q.data[i] = fill
			ni--
		}
		for i := 0; i < q.tail && ni > 0; i++ {
			q.data[i] = fill
			ni--
		}
	} else {
		for i := q.head; i < q.tail && ni > 0; i++ {
			q.data[i] = fill
			ni--
		}
	}
	q.head += n
	if q.head > len(q.data) {
		q.head -= len(q.data)
	}
	q.Len -= n
	if q.Len == 0 {
		q.head = 0
		q.tail = 0
	}
	return
}

// Peek peeks i ahead of the current head of queue.  It should be used in conjunction with .Len() to prevent a panic.
func Peek[x any](q *CircularQueue[x], i int) x {
	if i < 0 || i >= q.Len {
		panic("peek index is out of bounds")
	}
	p := q.head + i

	if p >= len(q.data) {
		p -= len(q.data)
	}
	return q.data[p]
}
