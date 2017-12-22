package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

func TestWhere(t *testing.T) {
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
			want: `batch
    |query('select cpu_usage from cpu')
    |where(lambda: "cpu" != 'cpu-total')
`,
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
			want: `batch
    |query('select cpu_usage from cpu')
    |where(lambda: "host" =~ /logger\d+/)
`,
		},
		{
			name: "where with compound logic",
			want: `batch
    |query('select cpu_usage from cpu')
    |where(lambda: lambda: "cpu" != 'cpu-total' AND lambda: "host" =~ /logger\d+/)
`,
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
			batch := &pipeline.BatchNode{}
			pipe := pipeline.CreatePipelineSources(batch)
			batch.Query("select cpu_usage from cpu").Where(tt.where)
			got, err := PipelineTick(pipe)
			if err != nil {
				t.Fatalf("Unexpected error building pipeline %v", err)
			}
			if got != tt.want {
				t.Errorf("%q. TestWhere() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
