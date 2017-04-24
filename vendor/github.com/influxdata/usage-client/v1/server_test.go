package client_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/influxdata/usage-client/v1"
	"github.com/stretchr/testify/require"
)

func Test_Server_Implements_Saveable(t *testing.T) {
	r := require.New(t)
	r.Implements((*client.Saveable)(nil), client.Server{})
}

// Example of saving a server to the Usage API
func Example_saveServer() {
	c := client.New("token-goes-here")
	// override the URL for testing
	c.URL = "https://usage.staging.influxdata.com"

	s := client.Server{
		ClusterID: "clus1",
		Host:      "example.com",
		Product:   "jambox",
		Version:   "1.0",
		ServerID:  "serv1",
	}

	res, err := c.Save(s)
	fmt.Printf("err: %s\n", err)
	b, _ := ioutil.ReadAll(res.Body)
	fmt.Printf("b: %s\n", b)
}
