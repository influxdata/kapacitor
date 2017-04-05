package scraper

import "github.com/prometheus/prometheus/config"

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
