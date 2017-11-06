package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestStateDuration(t *testing.T) {
	pipe, _, from := StreamFrom()
	lambda := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Left: &ast.ReferenceNode{
				Reference: "cpu",
			},
			Right: &ast.StringNode{
				Literal: "cpu-total",
			},
			Operator: ast.TokenNotEqual,
		},
	}

	sd := from.StateDuration(lambda)
	sd.As = "AKA"
	sd.Unit = time.Minute

	want := `stream
    |from()
    |stateDuration(lambda: "cpu" != 'cpu-total')
        .as('AKA')
        .unit(1m)
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestStateCount(t *testing.T) {
	pipe, _, from := StreamFrom()
	lambda := &ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Left: &ast.ReferenceNode{
				Reference: "cpu",
			},
			Right: &ast.StringNode{
				Literal: "cpu-total",
			},
			Operator: ast.TokenNotEqual,
		},
	}

	sd := from.StateCount(lambda)
	sd.As = "AKA"

	want := `stream
    |from()
    |stateCount(lambda: "cpu" != 'cpu-total')
        .as('AKA')
`
	PipelineTickTestHelper(t, pipe, want)
}
