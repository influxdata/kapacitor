package tick_test

import (
	"testing"
	"time"
)

func TestDerivative(t *testing.T) {
	pipe, _, from := StreamFrom()
	d := from.Derivative("work")
	d.As = "very important"
	d.Unit = time.Hour
	d.NonNegative()

	want := `stream
    |from()
    |derivative('work')
        .as('very important')
        .unit(1h)
        .nonNegative()
`
	PipelineTickTestHelper(t, pipe, want)
}
