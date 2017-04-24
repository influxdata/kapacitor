package client_test

import (
	"fmt"
	"testing"

	"github.com/influxdata/usage-client/v1"
	"github.com/stretchr/testify/require"
)

func Test_Client_RegistrationURL(t *testing.T) {
	r := require.New(t)
	c := client.New("foo")
	u, _ := c.RegistrationURL(client.Registration{
		ClusterID:   "clus1",
		Product:     "chronograf",
		RedirectURL: "http://example.com",
	})
	r.Equal("https://usage.influxdata.com/start?cluster_id=clus1&product=chronograf&redirect_url=http%3A%2F%2Fexample.com", u)
}

func Test_Registration_IsValid(t *testing.T) {
	r := require.New(t)
	reg := client.Registration{}
	err := reg.IsValid()
	r.Error(err)

	reg.ClusterID = "clus1"
	err = reg.IsValid()
	r.Error(err)

	reg.ClusterID = ""
	reg.Product = "foo"
	err = reg.IsValid()
	r.Error(err)

	reg.ClusterID = "clus1"
	reg.Product = "foo"
	err = reg.IsValid()
	r.NoError(err)
}

// Example of getting a registration URL for a product
func Example_registrationURL() {
	c := client.New("")
	r := client.Registration{
		ClusterID:   "clus1",
		Product:     "chronograf",
		RedirectURL: "http://example.com",
	}

	s, _ := c.RegistrationURL(r)
	fmt.Printf("s: %s\n", s)
	// https://usage.influxdata.com/start?cluster_id=clus1&product=chronograf&redirect_url=http%3A%2F%2Fexample.com
}
