package client_test

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/influxdata/usage-client/v1"
	"github.com/stretchr/testify/require"
)

func Test_Stats_Implements_Saveable(t *testing.T) {
	r := require.New(t)
	r.Implements((*client.Saveable)(nil), client.Stats{})
}

// Example of saving Stats data to Enterprise
func Example_saveStats() {
	c := client.New("token-goes-here")
	// override the URL for testing
	c.URL = "https://usage.staging.influxdata.com"

	st := client.Stats{
		ClusterID: "clus1",
		ServerID:  "serv1",
		Product:   "influxdb",
		Data: []client.StatsData{
			client.StatsData{
				Name: "engine",
				Tags: client.Tags{
					"path":    "/home/philip/.influxdb/data/_internal/monitor/1",
					"version": "bz1",
				},
				Values: client.Values{
					"blks_write":          39,
					"blks_write_bytes":    2421,
					"blks_write_bytes_c":  2202,
					"points_write":        39,
					"points_write_dedupe": 39,
				},
			},
		},
	}

	res, err := c.Save(st)
	fmt.Printf("err: %s\n", err)
	b, _ := ioutil.ReadAll(res.Body)
	fmt.Printf("b: %s\n", b)
}
