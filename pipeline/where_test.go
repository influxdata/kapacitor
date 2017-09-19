package pipeline

import (
	"bytes"
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestWhereNode_Tick(t *testing.T) {
	tests := []struct {
		name  string
		where *ast.LambdaNode
		want  string
	}{
		{
			name: "where not equal",
			where: &ast.LambdaNode{
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
			want: `|where(lambda: "cpu" != 'cpu-total')`,
		},
		{
			name: "where with regex",
			where: &ast.LambdaNode{
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
			want: `|where(lambda: "host" =~ /logger\d+/)`,
		},
		{
			name: "where with compound logic",
			want: `|where(lambda: "cpu" != 'cpu-total' AND "host" =~ /logger\d+/)`,
			where: &ast.LambdaNode{
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
			n := newWhereNode(StreamEdge, tt.where)

			var buf bytes.Buffer
			n.Tick(&buf)
			got := buf.String()
			if got != tt.want {
				t.Errorf("%q. TestWhereNode_Tick() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
