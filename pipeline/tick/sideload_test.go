package tick_test

import (
	"testing"
)

func TestSideload(t *testing.T) {
	pipe, _, from := StreamFrom()
	def := from.Sideload()
	def.Source = "file:///tmpdir"
	def.Order("a", "b", "c")
	def.Field("judgement", "plantiff")
	def.Field("finance", "loan")
	def.Tag("vocabulary", "volcano")
	def.Tag("make", "toyota")
	def.HttpUser = ("user")
	def.HttpPassword = ("password")

	want := `stream
    |from()
    |sideload()
        .source('file:///tmpdir')
        .httpUser('user')
        .httpPassword('password')
        .order('a', 'b', 'c')
        .field('finance', 'loan')
        .field('judgement', 'plantiff')
        .tag('make', 'toyota')
        .tag('vocabulary', 'volcano')
`
	PipelineTickTestHelper(t, pipe, want)
}
