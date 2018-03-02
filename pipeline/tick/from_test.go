package tick_test

import (
	"testing"
	"time"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestFrom(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Where(&ast.LambdaNode{
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
	from.GroupBy("this", "that", "these", "those").GroupByMeasurement()
	from.Database = "mydb"
	from.RetentionPolicy = "myrp"
	from.Measurement = "mymeasurement"
	from.Truncate = time.Second
	from.Round = time.Second

	want := `stream
    |from()
        .database('mydb')
        .retentionPolicy('myrp')
        .measurement('mymeasurement')
        .groupByMeasurement()
        .round(1s)
        .truncate(1s)
        .where(lambda: "cpu" != 'cpu-total' AND "host" =~ /logger\d+/)
        .groupBy('this', 'that', 'these', 'those')
`
	PipelineTickTestHelper(t, pipe, want)
}
