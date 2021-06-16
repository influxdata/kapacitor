package models

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/influxdata/kapacitor/istrings"
)

type Result struct {
	Series Rows  `json:"series"`
	Err    error `json:"error,omitempty"`
}

func (r Result) String() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func (r *Result) UnmarshalJSON(data []byte) error {
	var o struct {
		Series Rows   `json:"series"`
		Err    string `json:"error"`
	}
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}
	r.Series = o.Series
	if o.Err != "" {
		r.Err = errors.New(o.Err)
	}
	return nil
}

// Rows represents a collection of rows. Rows implements sort.Interface.
type Rows []*Row

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    istrings.IString                      `json:"name,omitempty"`
	Tags    map[istrings.IString]istrings.IString `json:"tags,omitempty"`
	Columns []istrings.IString                    `json:"columns,omitempty"`
	Values  [][]interface{}                       `json:"values,omitempty"`
}

var timeIString = istrings.Get("time")
var stringIString = istrings.Get("string")

func (r *Row) UnmarshalJSON(data []byte) error {
	var o struct {
		Name    istrings.IString                      `json:"name,omitempty"`
		Tags    map[istrings.IString]istrings.IString `json:"tags,omitempty"`
		Columns []istrings.IString                    `json:"columns,omitempty"`
		Values  [][]interface{}                       `json:"values,omitempty"`
	}
	if err := json.Unmarshal(data, &o); err != nil {
		return err
	}
	r.Name = o.Name
	r.Tags = o.Tags
	r.Columns = o.Columns
	r.Values = o.Values

	// Parse all time columns
	for i, v := range r.Values {
	columns:
		for j, c := range r.Columns {
			str, ok := v[j].(string)
			switch {
			case !ok:
				continue columns
			case c == timeIString:
				t, err := time.Parse(time.RFC3339, str)
				if err != nil {
					continue
				}
				r.Values[i][j] = t
			default:
				r.Values[i][j] = istrings.Get(str)
			}
		}
	}
	return nil
}
