package tick_test

import (
	"testing"
)

func TestDelete(t *testing.T) {
	pipe, _, from := StreamFrom()
	delete := from.Delete()
	delete.Field("f1")
	delete.Field("monaco")
	delete.Tag("race")
	delete.Tag("city")

	want := `stream
    |from()
    |delete()
        .field('f1')
        .field('monaco')
        .tag('race')
        .tag('city')
`

	PipelineTickTestHelper(t, pipe, want)
}
