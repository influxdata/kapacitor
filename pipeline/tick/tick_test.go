package tick_test

import (
	"bytes"
	"go/importer"
	"go/types"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/pipeline/tick"
	"github.com/influxdata/kapacitor/tick/stateful"
)

// TestPipelineImplemented checks if all nodes in the pipeline package
// have corresponding implementations in the tick package.
//
// If you get a test error here, then you need to implement
// a conversion node from pipeline node to the ast node.
func TestPipelineImplemented(t *testing.T) {
	tickPkg, err := importer.For("source", nil).Import("github.com/influxdata/kapacitor/pipeline/tick")
	if err != nil {
		t.Fatalf("error importing github.com/influxdata/kapacitor/pipeline: %v", err)
	}
	// tickScope lists all the types in the tick package
	tickScope := tickPkg.Scope()

	pipelinePkg, err := importer.For("source", nil).Import("github.com/influxdata/kapacitor/pipeline")
	if err != nil {
		t.Fatalf("error importing github.com/influxdata/kapacitor/pipeline: %v; perhaps kapacitor is not in $GOPATH/src/influxdata/kapacitor?", err)
	}

	pipelineScope := pipelinePkg.Scope()
	node := pipelineScope.Lookup("Node")
	if node == nil {
		t.Fatalf("%s.Node not found", pipelinePkg.Path())
	}
	nodeIface := node.Type()

	for _, name := range pipelineScope.Names() {
		strct := pipelineScope.Lookup(name)
		if !strct.Exported() {
			continue
		}
		// Only review the types whose pointers implement the pipeline.Node interface
		typ := strct.Type()
		if !types.AssignableTo(types.NewPointer(typ), nodeIface) {
			continue
		}

		// AlertNodeData have special behavior because each alert type embeds
		// AlertNodeData.  If a type embeds AlertNodeData we will skip it.
		// NoOpNode is a special case that doesn't have an AST representation
		if name == "AlertNodeData" || name == "NoOpNode" {
			continue
		}
		var isAlert bool
		if strct, ok := typ.Underlying().(*types.Struct); ok {
			for i := 0; i < strct.NumFields(); i++ {
				field := strct.Field(i)
				if field.Name() == "AlertNodeData" {
					isAlert = true
					break
				}
			}
		}

		if isAlert {
			continue
		}

		// IF YOU GET A TEST ERROR HERE:
		// All pipeline nodes must have corresponding tick structs that
		// can convert to an AST.  By convention you'll need to name node in the
		// tick package with the same name as in the pipeline package.
		if tickScope.Lookup(name) == nil {
			t.Errorf("Pipeline Node %s.%s not does not have an implementation in %s.%s to convert to AST representation",
				pipelinePkg.Path(), name, tickPkg.Path(), name)
		}
	}
}

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

func PipelineTickTestHelper(t *testing.T, pipe *pipeline.Pipeline, want string, udfs ...*pipeline.UDFNode) {
	t.Helper() //mark test as helper so its excluded from the test stack trace.
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	if got != want {
		t.Errorf("unexpected TICKscript:\ngot:\n%v\nwant:\n%v\n", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}

	d := deadman{}
	edge := pipeline.StreamEdge
	if strings.Contains(got, "batch") {
		edge = pipeline.BatchEdge
	}
	scope := stateful.NewScope()
	for _, udf := range udfs {
		scope.SetDynamicMethod(udf.UDFName, func(self interface{}, args ...interface{}) (interface{}, error) {
			return udf, nil
		})
	}

	_, err = pipeline.CreatePipeline(got, edge, scope, d, nil)
	if err != nil {
		t.Errorf("TICKscript not able to be parsed %v", err)
	}
}

type deadman struct {
	interval  time.Duration
	threshold float64
	id        string
	message   string
	global    bool
}

func (d deadman) Interval() time.Duration { return d.interval }
func (d deadman) Threshold() float64      { return d.threshold }
func (d deadman) Id() string              { return d.id }
func (d deadman) Message() string         { return d.message }
func (d deadman) Global() bool            { return d.global }
