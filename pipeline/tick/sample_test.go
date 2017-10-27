package tick_test

import (
	"testing"
	"time"
)

func TestSample(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Sample(int64(10)).Sample(time.Millisecond)
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |sample(10)
    |sample(1ms)
`
	if got != want {
		t.Errorf("TestSample = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
