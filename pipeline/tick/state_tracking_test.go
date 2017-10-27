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

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |stateDuration(lambda: "cpu" != 'cpu-total')
        .as('AKA')
        .unit(1m)
`
	if got != want {
		t.Errorf("TestStateDuration = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
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

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |stateCount(lambda: "cpu" != 'cpu-total')
        .as('AKA')
`
	if got != want {
		t.Errorf("TestStateCount = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
