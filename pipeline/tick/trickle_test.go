package tick_test

import (
	"testing"
	"time"
)

func TestTrickle(t *testing.T) {
	pipe, _, from := StreamFrom()
	w := from.Window()
	w.Every = time.Second
	w.Trickle()
	want := `stream
    |from()
    |window()
        .every(1s)
    |trickle()
`
	PipelineTickTestHelper(t, pipe, want)
}
