package scraper

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
)

// Discoverer represents a service that discovers hosts to scrape
type Discoverer interface {
	// Service returns the service type of the Discoverer
	Service() string
	// ID returns the unique ID of this specific discoverer
	ServiceID() string
	// Prom creates a prometheus scrape configuration.
	// TODO: replace when reimplement TargetManager
	Prom(c *config.ScrapeConfig)
	// TODO: future work
	// Run(ctx context.Context)
	// Find() hosts or something
}

// NewDiscoverer creates a standalone prometheus discoverer for sd, such as
// when testing a discovery service. The returned function unregisters the
// discoverer's metrics and must be called once the discoverer is no longer
// in use.
func NewDiscoverer(sd discovery.Config, logger log.Logger) (discovery.Discoverer, func(), error) {
	reg := prometheus.NewRegistry()
	metrics := sd.NewDiscovererMetrics(reg, discovery.NewRefreshMetrics(reg))
	if err := metrics.Register(); err != nil {
		return nil, nil, fmt.Errorf("failed to register %s discovery metrics: %w", sd.Name(), err)
	}
	d, err := sd.NewDiscoverer(discovery.DiscovererOptions{
		Logger:  logger,
		Metrics: metrics,
	})
	if err != nil {
		metrics.Unregister()
		return nil, nil, err
	}
	return d, metrics.Unregister, nil
}
