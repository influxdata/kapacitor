package tick_test

import (
	"testing"
)

func TestTrickle(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Trickle()
	want := `stream
    |from()
    |trickle()
`
	PipelineTickTestHelper(t, pipe, want)
}
