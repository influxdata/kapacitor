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

func TestBatchFlux(t *testing.T) {
	pipe, _, _ := BatchQueryFlux(`from(bucket: "example")`)
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `batch
    |queryFlux('from(bucket: "example")')
`
	if got != want {
		t.Errorf("TestBatch = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
