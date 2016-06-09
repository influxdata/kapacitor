package ponyExpress

import (
	"strings"
	"sync"
)

// Type refers to the different Package types
type Type int

// There are two package types, Write and Query
const (
	Write Type = iota
	Query
)

func startPonyExpress(packageCh <-chan Package, directiveCh <-chan Directive, responseCh chan<- Response, testID string) {

	c := &ponyExpress{
		testID: testID,

		addresses: []string{"localhost:8086"},
		precision: "ns",
		database:  "stress",
		startDate: "2016-01-01",
		qdelay:    "0s",
		wdelay:    "0s",

		wconc: 10,
		qconc: 5,

		packageChan:   packageCh,
		directiveChan: directiveCh,

		responseChan: responseCh,
	}
	// start listening for writes and queries
	go c.listen()

	// start listening for state changes
	go c.directiveListen()
}

type ponyExpress struct {
	testID string

	// State for the Stress Test
	addresses []string
	precision string
	startDate string
	database  string
	wdelay    string
	qdelay    string

	// Channels from statements
	packageChan   <-chan Package
	directiveChan <-chan Directive

	// Response channel
	responseChan chan<- Response

	// Concurrency utilities
	sync.WaitGroup
	sync.Mutex

	// Concurrency Limit for Writes and Reads
	wconc int
	qconc int

	// Manage Read and Write concurrency seperately
	wc *ConcurrencyLimiter
	rc *ConcurrencyLimiter
}

// NewTestPonyExpress returns a blank ponyExpress for testing
func newTestPonyExpress(url string) (*ponyExpress, chan Directive, chan Package) {
	pkgChan := make(chan Package)
	dirChan := make(chan Directive)
	pe := &ponyExpress{
		testID:        "foo_id",
		addresses:     []string{url},
		precision:     "s",
		startDate:     "2016-01-01",
		database:      "fooDatabase",
		wdelay:        "50ms",
		qdelay:        "50ms",
		wconc:         5,
		qconc:         5,
		packageChan:   pkgChan,
		directiveChan: dirChan,
		wc:            NewConcurrencyLimiter(1),
		rc:            NewConcurrencyLimiter(1),
	}
	return pe, dirChan, pkgChan
}

// client starts listening for Packages on the main channel
func (pe *ponyExpress) listen() {

	defer pe.Wait()

	// Keep track of number of concurrent readers and writers seperately
	pe.wc = NewConcurrencyLimiter(pe.wconc)
	pe.rc = NewConcurrencyLimiter(pe.qconc)

	// Manage overall number of goroutines and keep at 2 x (wconc + qconc)
	l := NewConcurrencyLimiter((pe.wconc + pe.qconc) * 2)

	// Concume incoming packages
	for p := range pe.packageChan {
		l.Increment()
		go func(p Package) {
			defer l.Decrement()
			switch p.T {
			case Write:
				pe.spinOffWritePackage(p)
			case Query:
				pe.spinOffQueryPackage(p)
			}
		}(p)
	}

}

// Set handles all SET requests for test state
func (pe *ponyExpress) directiveListen() {
	for d := range pe.directiveChan {
		pe.Lock()
		switch d.Property {

		// addresses is a []string of target InfluxDB instance(s) for the test
		// comes in as a "|" seperated array of addresses
		case "addresses":
			addr := strings.Split(d.Value, "|")
			pe.addresses = addr

		// percison is the write precision for InfluxDB
		case "precision":
			pe.precision = d.Value

		// writeinterval is an optional delay between batches
		case "writeinterval":
			pe.wdelay = d.Value

		// queryinterval is an optional delay between the batches
		case "queryinterval":
			pe.qdelay = d.Value

		// database is the InfluxDB database to target for both writes and queries
		case "database":
			pe.database = d.Value

			// concurrency is the number concurrent writers to the database
		case "writeconcurrency":
			conc := parseInt(d.Value)
			pe.wconc = conc
			// Reset the ConcurrencyLimiter
			pe.wc.NewMax(conc)

			// concurrentqueries is the number of concurrent queries to run against the database
		case "queryconcurrency":
			conc := parseInt(d.Value)
			pe.qconc = conc
			// Reset the ConcurrencyLimiter
			pe.rc.NewMax(conc)
		}

		// Decrement the tracker
		d.Tracer.Done()
		pe.Unlock()
	}
}
