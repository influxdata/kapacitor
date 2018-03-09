package tick_test

import (
	"testing"
	"time"
)

func TestChangeDetect(t *testing.T) {
	pipe, _, from := StreamFrom()
	d := from.ChangeDetect("work")
	d.As = "very important"
	d.Unit = time.Hour
	d.NonNegative()

	want := `stream
    |from()
    |changeDetect('work')
        .as('very important')
        .unit(1h)
        .nonNegative()
`
	PipelineTickTestHelper(t, pipe, want)
}
