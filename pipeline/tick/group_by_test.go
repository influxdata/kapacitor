package tick_test

import (
	"fmt"
	"testing"
)

func TestGroupBy(t *testing.T) {
	pipe, _, from := StreamFrom()
	logger := from.Log()
	logger.Level = "Coxeter"
	logger.GroupBy("simplex", "cube", "orthoplpex", "demicube").Exclude("4_21", "2_41", "1_42").ByMeasurement()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |log()
        .level('Coxeter')
    |groupBy('simplex', 'cube', 'orthoplpex', 'demicube')
        .exclude('4_21', '2_41', '1_42')
        .byMeasurement()
`
	if got != want {
		t.Errorf("TestGroupBy = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
