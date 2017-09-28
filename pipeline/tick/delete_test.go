package tick_test

import (
	"fmt"
	"testing"
)

func TestDelete(t *testing.T) {
	pipe, _, from := StreamFrom()
	delete := from.Delete()
	delete.Field("f1")
	delete.Field("monaco")
	delete.Tag("race")
	delete.Tag("city")

	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |delete()
        .field('f1')
        .field('monaco')
        .tag('race')
        .tag('city')
`
	if got != want {
		t.Errorf("TestDelete = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
