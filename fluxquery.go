package kapacitor

import (
	"sync"
	"time"
)

type QueryFlux struct {
	org     string
	orgID   string
	stmt    string
	queryMu sync.Mutex
	Now     time.Time
}

func NewQueryFlux(queryString, org, orgID string) (*QueryFlux, error) {
	return &QueryFlux{org: org, orgID: orgID, stmt: queryString}, nil
}

// Deep clone this query
func (q *QueryFlux) Clone() (*QueryFlux, error) {
	q.queryMu.Lock()
	defer q.queryMu.Unlock()
	return &QueryFlux{
		stmt:  q.stmt,
		org:   q.org,
		orgID: q.orgID,
		Now:   q.Now,
	}, nil

}

func (q *QueryFlux) String() string {
	return q.stmt
}
