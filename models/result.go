package models

import (
	"encoding/json"
	"errors"
	"time"
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
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
}

func (r *Row) UnmarshalJSON(data []byte) error {
	var o struct {
		Name    string            `json:"name,omitempty"`
		Tags    map[string]string `json:"tags,omitempty"`
		Columns []string          `json:"columns,omitempty"`
		Values  [][]interface{}   `json:"values,omitempty"`
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
		for j, c := range r.Columns {
			if c == "time" {
				tStr, ok := v[j].(string)
				if !ok {
					continue
				}
				t, err := time.Parse(time.RFC3339, tStr)
				if err != nil {
					continue
				}
				r.Values[i][j] = t
				break
			}
		}
	}
	return nil
}
