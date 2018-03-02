package tick_test

import (
	"testing"
)

func TestInfluxQLBottom(t *testing.T) {
	pipe, _, from := StreamFrom()
	influx := from.Mean("streets").Bottom(5, "rung", "tags")
	influx.As = "my"
	influx.UsePointTimes()

	want := `stream
    |from()
    |mean('streets')
        .as('mean')
    |bottom(5, 'rung', 'tags')
        .as('my')
        .usePointTimes()
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestInfluxQLTop(t *testing.T) {
	pipe, _, from := StreamFrom()
	influx := from.Top(5, "shelf", "tags", "youre_it")
	influx.As = "brass_ring"

	want := `stream
    |from()
    |top(5, 'shelf', 'tags', 'youre_it')
        .as('brass_ring')
`
	PipelineTickTestHelper(t, pipe, want)
}

func TestInfluxQLCount(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Count("dracula")

	want := `stream
    |from()
    |count('dracula')
        .as('count')
`
	PipelineTickTestHelper(t, pipe, want)
}
