package influxdb

import (
	"github.com/influxdata/kapacitor/istrings"
	"github.com/zeebo/xxh3"
)

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    istrings.IString                      `json:"name,omitempty"`
	Tags    map[istrings.IString]istrings.IString `json:"tags,omitempty"`
	Columns []istrings.IString                    `json:"columns,omitempty"`
	Values  [][]interface{}                       `json:"values,omitempty"`
}

// SameSeries returns true if r contains values for the same series as o.
func (r *Row) SameSeries(o *Row) bool {
	return r.tagsHash() == o.tagsHash() && r.Name == o.Name
}

// tagsHash returns a hash of tag key/value pairs.
func (r *Row) tagsHash() (hash uint64) {
	for k := range r.Tags {
		hash += xxh3.HashString(k.String())
		hash += xxh3.HashString(r.Tags[k].String())
	}
	return
}

// Rows represents a collection of rows. Rows implements sort.Interface.
type Rows []*Row

func (p Rows) Len() int { return len(p) }

func (p Rows) Less(i, j int) bool {
	// Sort by name first.
	if p[i].Name != p[j].Name {
		return p[i].Name.String() < p[j].Name.String()
	}

	// Sort by tag set hash. Tags don't have a meaningful sort order so we
	// just compute a hash and sort by that instead. This allows the tests
	// to receive rows in a predictable order every time.
	return p[i].tagsHash() < p[j].tagsHash()
}

func (p Rows) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
