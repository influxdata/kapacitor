package tick_test

import (
	"fmt"
	"testing"
)

func TestDefault(t *testing.T) {
	pipe, _, from := StreamFrom()
	def := from.Default()
	def.Field("judgement", "plantiff")
	def.Field("finance", "loan")
	def.Tag("vocabulary", "volcano")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |default()
        .field('finance', 'loan')
        .field('judgement', 'plantiff')
        .tag('vocabulary', 'volcano')
`
	if got != want {
		t.Errorf("TestDefault = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
