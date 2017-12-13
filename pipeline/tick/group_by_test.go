package tick_test

import (
	"testing"

	"github.com/influxdata/kapacitor/tick/ast"
)

func TestGroupBy(t *testing.T) {
	pipe, _, from := StreamFrom()
	logger := from.Log()
	logger.Level = "Coxeter"
	logger.GroupBy("simplex", "cube", "orthoplpex", "demicube").ByMeasurement()

	want := `stream
    |from()
    |log()
        .level('Coxeter')
    |groupBy('simplex', 'cube', 'orthoplpex', 'demicube')
        .exclude()
        .byMeasurement()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestGroupByStar(t *testing.T) {
	pipe, _, from := StreamFrom()
	logger := from.Log()
	logger.Level = "Coxeter"
	star := &ast.StarNode{}
	logger.GroupBy(star).Exclude("4_21", "2_41", "1_42").ByMeasurement()

	want := `stream
    |from()
    |log()
        .level('Coxeter')
    |groupBy(*)
        .exclude('4_21', '2_41', '1_42')
        .byMeasurement()
`
	PipelineTickTestHelper(t, pipe, want)
}
