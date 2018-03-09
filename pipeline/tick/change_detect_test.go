package tick_test

import (
	"testing"
)

func TestChangeDetect(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.ChangeDetect("work")

	want := `stream
    |from()
    |changeDetect('work')
`
	PipelineTickTestHelper(t, pipe, want)
}
