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

func Test_isZero(t *testing.T) {
	tt := []struct {
		exp   bool
		value struct {
			X string
			Y int
		}
	}{
		{
			exp: true,
			value: struct {
				X string
				Y int
			}{},
		},
		{
			exp: false,
			value: struct {
				X string
				Y int
			}{X: "hello"},
		},
		{
			exp: false,
			value: struct {
				X string
				Y int
			}{Y: 10},
		},
		{
			exp: false,
			value: struct {
				X string
				Y int
			}{X: "hello", Y: 10},
		},
	}

	for _, tst := range tt {
		if got, exp := isZero(reflect.ValueOf(tst.value)), tst.exp; got != exp {
			t.Errorf("unexpected result for isZero of %v got %t exp %t", tst.value, got, exp)
		}
	}

}
