package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestEval(t *testing.T) {
	pipe, _, from := StreamFrom()
	eval := from.Eval(&ast.LambdaNode{
		Expression: &ast.BinaryNode{
			Operator: ast.TokenAnd,
			Left: &ast.BinaryNode{
				Left: &ast.ReferenceNode{
					Reference: "cpu",
				},
				Right: &ast.StringNode{
					Literal: "cpu-total",
				},
				Operator: ast.TokenNotEqual,
			},
			Right: &ast.BinaryNode{
				Left: &ast.ReferenceNode{
					Reference: "host",
				},
				Right: &ast.RegexNode{
					Literal: `logger\d+`,
				},
				Operator: ast.TokenRegexEqual,
			},
		},
	})
	eval.As("cells").Tags("cells").Keep("petri", "dish").Quiet()

	want := `stream
    |from()
    |eval(lambda: "cpu" != 'cpu-total' AND "host" =~ /logger\d+/)
        .as('cells')
        .tags('cells')
        .quiet()
        .keep('petri', 'dish')
`
	PipelineTickTestHelper(t, pipe, want)
}
