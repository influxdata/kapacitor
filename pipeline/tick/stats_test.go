package tick_test

import (
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	pipe, _, from := StreamFrom()
	stats := from.Stats(2 * time.Second)
	stats.Align()

	want := `var from1 = stream
    |from()

from1
    |stats(2s)
        .align()
`
	PipelineTickTestHelper(t, pipe, want)
}
