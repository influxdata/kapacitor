package pipeline

type Stream struct {
	children []*WindowedStream
}

func (s *Stream) Window() *WindowedStream {
	w := NewWindowedStream(s)
	s.children = append(s.children, w)
	return w
}

func (s *Stream) NumChildren() int {
	return len(s.children)
}

func (s *Stream) Children() []*WindowedStream {
	return s.children
}
