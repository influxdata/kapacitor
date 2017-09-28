package tick_test

import (
	"bytes"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
	"github.com/influxdata/kapacitor/tick/ast"
)

var _ ast.Node = &NullNode{}

type NullNode struct {
	*tick.NullPosition
}

func (n *NullNode) String() string                     { return "" }
func (n *NullNode) Format(*bytes.Buffer, string, bool) {}
func (n *NullNode) Equal(o interface{}) (ok bool)      { _, ok = o.(*NullNode); return }
func (n *NullNode) MarshalJSON() ([]byte, error)       { return nil, nil }

// StreamFrom builds a simple pipeline for testing
func StreamFrom() (pipe *pipeline.Pipeline, stream *pipeline.StreamNode, from *pipeline.FromNode) {
	stream = &pipeline.StreamNode{}
	pipe = pipeline.CreatePipelineSources(stream)
	from = stream.From()
	return pipe, stream, from
}

func PipelineTick(pipe *pipeline.Pipeline) (string, error) {
	ast := tick.AST{}
	err := ast.Build(pipe)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	ast.Program.Format(&buf, "", false)
	return buf.String(), nil
}
