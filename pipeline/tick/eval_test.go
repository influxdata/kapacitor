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
			Left: &ast.LambdaNode{
				Expression: &ast.BinaryNode{
					Left: &ast.ReferenceNode{
						Reference: "cpu",
					},
					Right: &ast.StringNode{
						Literal: "cpu-total",
					},
					Operator: ast.TokenNotEqual,
				},
			},
			Right: &ast.LambdaNode{
				Expression: &ast.BinaryNode{
					Left: &ast.ReferenceNode{
						Reference: "host",
					},
					Right: &ast.RegexNode{
						Literal: `logger\d+`,
					},
					Operator: ast.TokenRegexEqual,
				},
			},
		},
	})
	eval.As("multiply", "divide").Tags("cells").Keep("petri", "dish").Quiet()

	want := `stream
    |from()
    |eval(lambda: lambda: "cpu" != 'cpu-total' AND lambda: "host" =~ /logger\d+/)
        .as('multiply', 'divide')
        .tags('cells')
        .quiet()
        .keep('petri', 'dish')
`
	PipelineTickTestHelper(t, pipe, want)
}
