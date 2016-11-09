package config

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func Test_encodeOverride_decodeOverride(t *testing.T) {
	testCases := []struct {
		o   Override
		exp []byte
	}{
		{
			o:   Override{},
			exp: []byte(`{"version":1,"value":{"id":"","options":null,"create":false}}`),
		},
		{
			o: Override{
				ID: "42",
			},
			exp: []byte(`{"version":1,"value":{"id":"42","options":null,"create":false}}`),
		},
		{
			o: Override{
				ID:     "42",
				Create: true,
			},
			exp: []byte(`{"version":1,"value":{"id":"42","options":null,"create":true}}`),
		},
		{
			o: Override{
				ID: "42",
				Options: map[string]interface{}{
					"a": json.Number("1"),
					"b": []interface{}{"x", "y", "z"},
					"c": map[string]interface{}{"k1": "x", "k2": "y", "k3": "z"},
				},
				Create: true,
			},
			exp: []byte(`{"version":1,"value":{"id":"42","options":{"a":1,"b":["x","y","z"],"c":{"k1":"x","k2":"y","k3":"z"}},"create":true}}`),
		},
	}
	for _, tc := range testCases {
		got, err := tc.o.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, tc.exp) {
			t.Errorf("unexpected encoding:\ngot\n%s\nexp\n%s\n", string(got), string(tc.exp))
		}
		o := new(Override)
		if err := o.UnmarshalBinary(got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(*o, tc.o) {
			t.Errorf("unexpected decoding:\ngot\n%v\nexp\n%v\n", *o, tc.o)
		}
	}
}
