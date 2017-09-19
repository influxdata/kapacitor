package pipeline

import (
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestLambdaTick(t *testing.T) {
	tests := []struct {
		name   string
		lambda *ast.LambdaNode
		want   string
	}{
		{
			name: "not equal",
			lambda: &ast.LambdaNode{
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
			want: `lambda: "cpu" != 'cpu-total'`,
		},
		{
			name: "regex requal",
			lambda: &ast.LambdaNode{
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
			want: `lambda: "host" =~ /logger\d+/`,
		},
		{
			name: "lambda with compound logic",
			want: `lambda: "cpu" != 'cpu-total' AND "host" =~ /logger\d+/`,
			lambda: &ast.LambdaNode{
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LambdaTick(tt.lambda)
			if got != tt.want {
				t.Errorf("%q. TestLambdaTick() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
