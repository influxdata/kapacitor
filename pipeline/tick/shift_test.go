package tick_test

import (
	"fmt"
	"testing"
	"time"
)

func TestShift(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Shift(time.Hour)
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |shift(1h)
`
	if got != want {
		t.Errorf("TestShift = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
