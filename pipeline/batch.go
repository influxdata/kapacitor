package pipeline

import (
	"time"
)

type BatchNode struct {
	node
	Query      string
	Period     time.Duration
	Dimensions []interface{}
}

func newBatchNode() *BatchNode {
	return &BatchNode{
		node: node{
			desc:     "batch",
			wants:    BatchEdge,
			provides: BatchEdge,
		},
	}
}

// Group the data by a set of dimensions.
// Adds the dimensions to the query.
func (b *BatchNode) GroupBy(d ...interface{}) Node {
	b.Dimensions = d
	return b
}
