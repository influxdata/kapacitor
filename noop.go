package kapacitor

import (
	"log"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/services/notary"
)

type NoOpNode struct {
	node
}

// Create a new  NoOpNode which does nothing with the data and just passes it through.
func newNoOpNode(et *ExecutingTask, n *pipeline.NoOpNode, l *log.Logger, nt Notary) (*NoOpNode, error) {
	nn := &NoOpNode{
		node: node{Node: n, et: et, logger: l, notary: notary.WithPrefix(nt, "node", "noop")},
	}
	nn.node.runF = nn.runNoOp
	return nn, nil
}

func (n *NoOpNode) runNoOp([]byte) error {
	for m, ok := n.ins[0].Emit(); ok; m, ok = n.ins[0].Emit() {
		if err := edge.Forward(n.outs, m); err != nil {
			return err
		}
	}
	return nil
}
