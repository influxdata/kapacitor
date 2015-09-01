package pipeline

import (
	"time"
)

type WindowedStream struct {
	stream *Stream
	Period time.Duration
	Every  time.Duration
}

func NewWindowedStream(s *Stream) *WindowedStream {
	return &WindowedStream{
		stream: s,
	}
}

func (w *WindowedStream) Options(period, every time.Duration) *WindowedStream {
	w.Period = period
	w.Every = every
	return w
}
