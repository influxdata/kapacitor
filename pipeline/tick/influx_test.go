package tick_test

import (
	"testing"
)

func TestInfluxQL(t *testing.T) {
	pipe, _, from := StreamFrom()
	influx := from.Mean("streets").Bottom(5, "rung", "tags")
	influx.As = "my"
	influx.UsePointTimes()

	want := `stream
    |from()
    |mean('streets')
        .as('mean')
    |bottom('rung', 5, 'tags')
        .as('my')
        .usePointTimes()
`
	PipelineTickTestHelper(t, pipe, want)
}
