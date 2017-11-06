package tick_test

import (
	"testing"
)

func TestBatch(t *testing.T) {
	pipe, _, _ := BatchQuery("select cpu_usage from cpu")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `batch
    |query('select cpu_usage from cpu')
`
	if got != want {
		t.Errorf("TestBatch = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
