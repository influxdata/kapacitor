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

	want := `stream
    |from()
    |flatten()
        .on('stanley')
        .delimiter('blackline')
        .tolerance(1s)
        .dropOriginalFieldName()
`
	PipelineTickTestHelper(t, pipe, want)
}
