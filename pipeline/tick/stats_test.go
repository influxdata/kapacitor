package tick_test

import (
	"fmt"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	pipe, _, from := StreamFrom()
	stats := from.Stats(2 * time.Second)
	stats.Align()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `var from1 = stream
    |from()

from1
    |stats(2s)
        .align()
`
	if got != want {
		t.Errorf("TestStats = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
