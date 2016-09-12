package server

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestConfig_applyEnvOverridesToMap(t *testing.T) {
	testCases := []struct {
		env    [][]string
		prefix string
		expMap map[string]string
	}{
		{
			prefix: "VARS",
			env: [][]string{
				{"VARS_VAR1", "foo"},
				{"VARS_VAR2", "bar"},
				{"VARS_EMPTY", ""},
			},
			expMap: map[string]string{
				"VAR1":  "foo",
				"VAR2":  "bar",
				"EMPTY": "",
			},
		},
		{
			prefix: "KAPACITOR_VARS",
			env: [][]string{
				{"KAPACITOR_VARS_MY_NAMED_VAR", "foo=with=equals=chars"},
				{"KAPACITOR_VARS_SOME_OTHER_VAR", "bar"},
				{"SKIP_ME_VAR", "should be skipped"},
			},
			expMap: map[string]string{
				"MY_NAMED_VAR":   "foo=with=equals=chars",
				"SOME_OTHER_VAR": "bar",
			},
		},
	}
	for _, tc := range testCases {
		for _, kv := range tc.env {
			os.Setenv(kv[0], kv[1])
		}
		m := make(map[string]string)
		err := applyEnvOverridesToMap(tc.prefix, reflect.ValueOf(m))
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(m, tc.expMap) {
			t.Errorf("unexpected map: got %v exp %v", m, tc.expMap)
		}
		for _, kv := range tc.env {
			os.Unsetenv(kv[0])
		}
	}
}
func TestConfig_applyEnvOverridesToMap_InvalidMap(t *testing.T) {
	testCases := []struct {
		m   interface{}
		err string
	}{
		{
			m:   (map[string]string)(nil),
			err: "cannot apply env to nil map",
		},
		{
			m: map[int]int{
				1: 1,
			},
			err: "map is not a map[string]string",
		},
		{
			m: map[string]int{
				"1": 1,
			},
			err: "map is not a map[string]string",
		},
		{
			m: map[int]string{
				1: "1",
			},
			err: "map is not a map[string]string",
		},
	}
	for _, tc := range testCases {
		err := applyEnvOverridesToMap("", reflect.ValueOf(tc.m))
		if got, exp := fmt.Sprintf("%v", err), tc.err; got != exp {
			t.Errorf("expected error: got %s, exp %s", got, exp)
		}
	}
}
