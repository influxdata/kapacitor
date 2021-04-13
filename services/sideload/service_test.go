package sideload_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/sideload"
)

func NewService() *sideload.Service {
	s := sideload.NewService(nil)
	return s
}

func TestService_Source_Lookup(t *testing.T) {
	s := NewService()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	conf := httppost.Config{URLTemplate: fmt.Sprintf("file://%s/testdata/src0", wd)}
	e := &httppost.Endpoint{}
	if err := e.Update(conf); err != nil {
		t.Fatal(err)
	}

	src, err := s.Source(e)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	testCases := []struct {
		order []string
		key   string
		want  interface{}
	}{
		{
			order: []string{
				"host/hostA.yml",
				"default.yml",
			},
			key:  "key0",
			want: 5.0,
		},
		{
			order: []string{
				"host/hostA.yml",
				"default.yml",
			},
			key:  "key1",
			want: "one",
		},
		{
			order: []string{
				"host/hostA.yml",
				"hostgroup/foo.yml",
				"default.yml",
			},
			key:  "key0",
			want: 5.0,
		},
		{
			order: []string{
				"host/hostA.yml",
				"hostgroup/foo.yml",
				"default.yml",
			},
			key:  "key1",
			want: "foo",
		},
	}
	for i, tc := range testCases {
		tc := tc
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			got := src.Lookup(tc.order, tc.key)
			if !cmp.Equal(got, tc.want) {
				t.Errorf("unexpected values: -want/+got:\n%s", cmp.Diff(tc.want, got))
			}
			if err := e.Update(conf); err != nil {
				t.Fatal(err)
			}
		})
	}
}
