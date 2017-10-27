package tick_test

import (
	"testing"
	"time"
)

func TestFlatten(t *testing.T) {
	pipe, _, from := StreamFrom()
	flatten := from.Flatten()
	flatten.On("stanley")
	flatten.Delimiter = "blackline"
	flatten.Tolerance = time.Second
	flatten.DropOriginalFieldName()
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |flatten()
        .on('stanley')
        .delimiter('blackline')
        .tolerance(1s)
        .dropOriginalFieldName()
`
	if got != want {
		t.Errorf("TestFlatten = %v, want %v", got, want)
		t.Log(got) // print is helpful to get the correct format.
	}
}
