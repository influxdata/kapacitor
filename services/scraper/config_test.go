package scraper_test

import (
	"testing"

	"github.com/influxdata/kapacitor/services/file_discovery"
	"github.com/influxdata/kapacitor/services/scraper"
	"github.com/prometheus/common/log"
)

func TestConfig_MultiConfig(t *testing.T) {
	tests := []struct {
		name            string
		discoveryConfig []file_discovery.Config
		scraperConfig   []scraper.Config
	}{
		{
			name: "first discovery config in multi config",
			discoveryConfig: []file_discovery.Config{
				{
					Enabled: true,
					ID:      "first",
				},
				{
					Enabled: true,
					ID:      "second",
				},
			},
			scraperConfig: []scraper.Config{
				{
					Enabled:         true,
					DiscoverID:      "first",
					DiscoverService: file_discovery.Config{}.Service(),
				},
			},
		},
	}

	for _, tt := range tests {
		diag := scraper.Diagnostic(log.NewNopLogger())
		for i := range tt.discoveryConfig {
			tt.discoveryConfig[i].Init()
		}
		registy := scraper.NewService(tt.scraperConfig, diag)
		registy.Open()
		svc := file_discovery.NewService(tt.discoveryConfig, registy, diag)
		svc.Open()
		pairs := registy.Pairs()
		if len(pairs) == 0 {
			t.Errorf("cannot register discovery config: %s", tt.name)
		}
		svc.Close()
		registy.Close()
	}
}
