package override

import (
	"reflect"
	"testing"
)

func TestGetSectionName(t *testing.T) {
	testCases := []struct {
		f       reflect.StructField
		expName string
		expOK   bool
	}{
		{
			f: reflect.StructField{
				Name: "FieldName",
				Tag:  `override:"name"`,
			},
			expName: "name",
			expOK:   true,
		},
		{
			f: reflect.StructField{
				Name: "FieldName",
			},
			expName: "FieldName",
			expOK:   false,
		},
		{
			f: reflect.StructField{
				Name: "FieldName",
				Tag:  `override:"name,element-key=id"`,
			},
			expName: "name",
			expOK:   true,
		},
	}

	for _, tc := range testCases {
		t.Log(tc.f)
		name, ok := getSectionName(tc.f)
		if got, exp := name, tc.expName; got != exp {
			t.Errorf("unexpected name got %q exp %q", got, exp)
		}
		if got, exp := ok, tc.expOK; got != exp {
			t.Errorf("unexpected ok got %t exp %t", got, exp)
		}
	}
}
