package tick_test

import (
	"testing"
)

func TestStream(t *testing.T) {
	pipe, _, _ := StreamFrom()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
`
	if got != want {
		t.Errorf("TestStream = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
