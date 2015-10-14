package pipeline

// Passes a filtered stream to its children. The stream is filtered based on the From and Where conditions.
type StreamNode struct {
	node
	// Which database retenion policy and measuremnt to require.
	From string
	// An influxql Where condition to further filter the stream.
	Predicate string
}

func newStreamNode() *StreamNode {
	return &StreamNode{
		node: node{
			desc:     "stream",
			wants:    StreamEdge,
			provides: StreamEdge,
		},
	}
}

func (s *StreamNode) Fork() *StreamNode {
	c := newStreamNode()
	s.linkChild(c)
	return c
}

func (s *StreamNode) Where(predicate string) Node {
	s.Predicate = predicate
	return s
}
