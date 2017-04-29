package scraper

// Pair is the linked discovery/scraper pair
type Pair struct {
	Discoverer Discoverer
	Scraper    Config
}

// Registry represents the combined configuration state of discoverers and scrapers
type Registry interface {
	// Commit finishes the update to the registry configuration
	Commit() error
	// AddDiscoverer adds discoverers to the registry
	AddDiscoverer(Discoverer)
	// RemoveDiscoverer removes discoverers from the registry
	RemoveDiscoverer(Discoverer)
	// AddScrapers adds scrapers to the registry
	AddScrapers([]Config)
	// RemoveScrapers removes scrapers from the registry
	RemoveScrapers([]Config)
	// Pairs returns the linked scraper/discovery combinations
	Pairs() []Pair
}
