package tick_test

import (
	"testing"
)

func TestStream(t *testing.T) {
	pipe, _, _ := StreamFrom()
	want := `stream
    |from()
`
	PipelineTickTestHelper(t, pipe, want)
}
