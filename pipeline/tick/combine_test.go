package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestCombine(t *testing.T) {
	pipe, _, from := StreamFrom()
	combine := from.Combine(&ast.LambdaNode{
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
	combine.As("pumpkin", "eggs", "cinnamon", "ginger", "nutmeg", "condensedMilk")
	combine.Delimiter = "cup"
	combine.Tolerance = time.Hour + 10*time.Minute
	combine.Max = 1

	want := `stream
    |from()
    |combine(lambda: lambda: "cpu" != 'cpu-total' AND lambda: "host" =~ /logger\d+/)
        .as('pumpkin', 'eggs', 'cinnamon', 'ginger', 'nutmeg', 'condensedMilk')
        .delimiter('cup')
        .tolerance(70m)
        .max(1)
`
	PipelineTickTestHelper(t, pipe, want)
}
