package tick_test

import (
	"testing"
)

func TestDefault(t *testing.T) {
	pipe, _, from := StreamFrom()
	def := from.Default()
	def.Field("judgement", "plantiff")
	def.Field("finance", "loan")
	def.Tag("vocabulary", "volcano")
	def.Tag("make", "toyota")

	want := `stream
    |from()
    |default()
        .field('finance', 'loan')
        .field('judgement', 'plantiff')
        .tag('make', 'toyota')
        .tag('vocabulary', 'volcano')
`
	PipelineTickTestHelper(t, pipe, want)
}
