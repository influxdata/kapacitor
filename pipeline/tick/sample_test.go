package tick_test

import (
	"testing"
	"time"
)

func TestSample(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.Sample(int64(10)).Sample(time.Millisecond)

	want := `stream
    |from()
    |sample(10)
    |sample(1ms)
`
	PipelineTickTestHelper(t, pipe, want)
}
