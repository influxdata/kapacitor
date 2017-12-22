package tick_test

import (
	"testing"
)

func TestGroupBy(t *testing.T) {
	pipe, _, from := StreamFrom()
	logger := from.Log()
	logger.Level = "Coxeter"
	logger.GroupBy("simplex", "cube", "orthoplpex", "demicube").Exclude("4_21", "2_41", "1_42").ByMeasurement()

	want := `stream
    |from()
    |log()
        .level('Coxeter')
    |groupBy('simplex', 'cube', 'orthoplpex', 'demicube')
        .exclude('4_21', '2_41', '1_42')
        .byMeasurement()
`
	PipelineTickTestHelper(t, pipe, want)
}
