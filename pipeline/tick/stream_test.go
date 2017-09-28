package tick_test

import (
	"fmt"
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
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
