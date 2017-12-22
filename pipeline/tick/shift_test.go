package tick_test

import (
	"testing"
	"time"
)

func TestShift(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Shift(time.Hour)

	want := `stream
    |from()
    |shift(1h)
`
	PipelineTickTestHelper(t, pipe, want)
}
