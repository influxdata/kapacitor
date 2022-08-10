package sideload_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	khttp "github.com/influxdata/kapacitor/http"
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

	testhandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs, err := os.Open(r.URL.Path)
		t.Logf(r.URL.Path)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
		}
		if _, err := io.Copy(w, fs); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
		}
		fs.Close()
	})

	httpServer := httptest.NewServer(testhandler)
	defer httpServer.Close()

	httpsServer := httptest.NewTLSServer(testhandler)

	defClient := http.DefaultClient
	khttp.DefaultClient = httpsServer.Client()
	defer func() {
		http.DefaultClient = defClient
	}()
	defer httpsServer.Close()

	cases := []struct {
		scheme string
		url    string
	}{
		{scheme: "file", url: fmt.Sprintf("file://%s/testdata/src0", wd)},
		{scheme: "http", url: fmt.Sprintf("%s/testdata/src0", httpServer.URL)},
		{scheme: "https", url: fmt.Sprintf("%s/testdata/src0", httpsServer.URL)},
	}
	for _, c := range cases {
		t.Run(c.scheme, func(t *testing.T) {
			conf := httppost.Config{URLTemplate: c.url}
			e := &httppost.Endpoint{}
			if err := e.Update(conf); err != nil {
				t.Fatal(err)
			}

			src, err := s.Source(e, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()

			testCases := []struct {
				order []string
				key   string
				want  interface{}
				ttl   time.Duration
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
				{
					order: []string{
						"host/hostA.yml",
						"hostgroup/foo.yml",
						"default.yml",
					},
					key:  "key1",
					want: "foo",
					ttl:  10 * time.Millisecond,
				},
				{
					order: []string{
						"host/hostA.yml",
						"hostgroup/foo.yml",
						"default.yml",
					},
					key:  "key1",
					want: "foo",
					ttl:  100 * time.Millisecond,
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
		})
	}
}
