package istrings

import (
	"testing"
)

func TestBuilder_IString_alloc(t *testing.T) {
	s := "hello my friends"
	Get(s)
	var a *IString
	if testing.AllocsPerRun(10, func() {
		for i := 0; i < 10000; i++ {
			a = Get(s)
		}
	}) != 0 {
		t.Fatalf("Get shouldn't allocate when the string is already interned")
	}

	if allocs := testing.AllocsPerRun(1, func() {
		a = Get("a new string to test allocs")
	}); allocs > 1 {
		t.Fatalf("Get shouldn't allocate more than once when it sees a new string, but it allocated %f times ", allocs)
	}
	_ = a
}
