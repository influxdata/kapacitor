package tick_test

import (
	"testing"
)

func TestHTTPOut(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.HttpOut("There is never any ending to Paris – Hemingway")

	want := `stream
    |from()
    |httpOut('There is never any ending to Paris – Hemingway')
`
	PipelineTickTestHelper(t, pipe, want)
}
