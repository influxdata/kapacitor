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
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |derivative('work')
        .as('very important')
        .unit(1h)
        .nonNegative()
`
	if got != want {
		t.Errorf("TestDerivative = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
