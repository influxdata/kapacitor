package kapacitor

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type LogNode struct {
	node

	key    string
	level  string
	prefix string
	buf    bytes.Buffer
	enc    *json.Encoder

	batchBuffer *edge.BatchBuffer
}

// Create a new  LogNode which logs all data it receives
func newLogNode(et *ExecutingTask, n *pipeline.LogNode, d NodeDiagnostic) (*LogNode, error) {
	nn := &LogNode{
		node:        node{Node: n, et: et, diag: d},
		level:       strings.ToUpper(n.Level),
		prefix:      n.Prefix,
		batchBuffer: new(edge.BatchBuffer),
	}
	nn.enc = json.NewEncoder(&nn.buf)
	nn.node.runF = nn.runLog
	return nn, nil
}

func (n *LogNode) runLog([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		n.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			n.outs,
			edge.NewTimedForwardReceiver(n.timer, n),
		),
	)
	return consumer.Consume()

}

func (n *LogNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BeginBatch(begin)
}

func (n *LogNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, n.batchBuffer.BatchPoint(bp)
}

func (n *LogNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return n.BufferedBatch(n.batchBuffer.BufferedBatchMessage(end))
}

func (n *LogNode) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	n.diag.LogBatchData(n.level, n.prefix, batch)
	return batch, nil
}

func (n *LogNode) Point(p edge.PointMessage) (edge.Message, error) {
	n.diag.LogPointData(n.level, n.prefix, p)
	return p, nil
}

func (n *LogNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
func (n *LogNode) DeleteGroup(d edge.DeleteGroupMessage) (edge.Message, error) {
	return d, nil
}
func (n *LogNode) Done() {}
