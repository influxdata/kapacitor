package client_test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/usage-client/v1"
	"github.com/stretchr/testify/require"
)

type SimpleSaveable struct {
}

func (s SimpleSaveable) Path() string {
	return "/foo"
}

func Test_ClientSave(t *testing.T) {
	r := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer ts.Close()

	c := client.New("")
	c.URL = ts.URL

	res, err := c.Save(SimpleSaveable{})
	r.NoError(err)
	r.Equal(200, res.StatusCode)
}

func Test_ClientSave_AuthHeaderSet(t *testing.T) {
	r := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.Header.Get("X-Authorization")))
	}))
	defer ts.Close()

	c := client.New("my-token")
	c.URL = ts.URL

	res, err := c.Save(SimpleSaveable{})
	r.NoError(err)
	r.Equal(200, res.StatusCode)

	b, _ := ioutil.ReadAll(res.Body)
	r.Equal("my-token", string(b))
}

func Test_ClientSave_500(t *testing.T) {
	r := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, `{"error":"oops!"}`)
	}))
	defer ts.Close()

	c := client.New("")
	c.URL = ts.URL

	res, err := c.Save(SimpleSaveable{})
	r.Equal(500, res.StatusCode)
	r.Error(err)
	r.Equal("oops!", err.Error())
}

func Test_ClientSave_422(t *testing.T) {
	r := require.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		fmt.Fprint(w, `
{
  "errors": {
    "cluster_id": [
      "ClusterID can not be blank."
    ],
    "host": [
      "Host can not be blank."
    ],
    "product": [
      "Product can not be blank."
    ],
    "server_id": [
      "ServerID can not be blank."
    ],
    "version": [
      "Version can not be blank."
    ]
  }
}
`)
	}))
	defer ts.Close()

	c := client.New("")
	c.URL = ts.URL

	res, err := c.Save(SimpleSaveable{})
	r.Equal(422, res.StatusCode)
	r.Error(err)

	ve := err.(client.ValidationErrors)
	r.Equal([]string{"Version can not be blank."}, ve.Errors["version"])
}
