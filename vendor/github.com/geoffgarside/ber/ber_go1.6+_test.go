// +build go1.6

package ber

import "testing"

func TestMarshalNilValue(t *testing.T) {
	nilValueTestData := []interface{}{
		nil,
		struct{ V interface{} }{},
	}
	for i, test := range nilValueTestData {
		if _, err := Marshal(test); err == nil {
			t.Fatalf("#%d: successfully marshaled nil value", i)
		}
	}
}
