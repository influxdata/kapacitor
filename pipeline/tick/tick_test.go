package tick_test

import (
	"bytes"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
)

// StreamFrom builds a simple pipeline for testing
func StreamFrom() (pipe *pipeline.Pipeline, stream *pipeline.StreamNode, from *pipeline.FromNode) {
	stream = &pipeline.StreamNode{}
	pipe = pipeline.CreatePipelineSources(stream)
	from = stream.From()
	return pipe, stream, from
}

func BatchQuery(q string) (pipe *pipeline.Pipeline, batch *pipeline.BatchNode, query *pipeline.QueryNode) {
	batch = &pipeline.BatchNode{}
	pipe = pipeline.CreatePipelineSources(batch)
	query = batch.Query(q)
	return pipe, batch, query
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
