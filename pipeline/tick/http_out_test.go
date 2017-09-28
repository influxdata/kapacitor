package tick_test

import (
	"fmt"
	"testing"
)

func TestHTTPOut(t *testing.T) {
	pipe, _, from := StreamFrom()
	from.HttpOut("There is never any ending to Paris – Hemingway")
	got, err := PipelineTick(pipe)
	if err != nil {
		t.Fatalf("Unexpected error building pipeline %v", err)
	}

	want := `stream
    |from()
    |httpOut('There is never any ending to Paris – Hemingway')
`
	if got != want {
		t.Errorf("TestHTTPOut = %v, want %v", got, want)
		fmt.Println(got) // print is helpful to get the correct format.
	}
}
