package models

import (
	"time"
)

//Represents purely a set of fields and a time.
type TimeFields struct {
	Fields Fields    `json:"fields"`
	Time   time.Time `json:"time"`
}

type Batch struct {
	Name   string            `json:"name,omitempty"`
	Group  GroupID           `json:"-"`
	Tags   map[string]string `json:"tags,omitempty"`
	Points []TimeFields      `json:"points,omitempty"`
}
