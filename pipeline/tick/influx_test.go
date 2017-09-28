package tick_test

import (
	"fmt"
	"testing"
)

func TestInfluxQL(t *testing.T) {
	pipe, _, from := StreamFrom()
	influx := from.Mean("streets").Bottom(5, "rung", "tags")
	influx.As = "my"
	influx.UsePointTimes()

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |mean('streets')
        .as('mean')
    |bottom('rung', 5, 'tags')
        .as('my')
        .usePointTimes()
`
	if got != want {
		t.Errorf("TestInfluxQL = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
