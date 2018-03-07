package alert

import (
	"sync"
	"sync/atomic"

	"github.com/influxdata/kapacitor/models"
)

type InhibitorLookup struct {
	mu         sync.RWMutex
	inhibitors map[string][]*Inhibitor
}

func NewInhibitorLookup() *InhibitorLookup {
	return &InhibitorLookup{
		inhibitors: make(map[string][]*Inhibitor),
	}
}

func (l *InhibitorLookup) IsInhibited(category string, tags models.Tags) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for _, i := range l.inhibitors[category] {
		if i.IsInhibited(category, tags) {
			return true
		}
	}
	return false
}

func (l *InhibitorLookup) AddInhibitor(in *Inhibitor) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inhibitors[in.category] = append(l.inhibitors[in.category], in)
}

func (l *InhibitorLookup) RemoveInhibitor(in *Inhibitor) {
	l.mu.Lock()
	defer l.mu.Unlock()

	inhibitors := l.inhibitors[in.category]
	for i := range inhibitors {
		if inhibitors[i] == in {
			l.inhibitors[in.category] = append(inhibitors[:i], inhibitors[i+1:]...)
			break
		}
	}
}

// Inhibitor tracks whether an alert category + tag set have been inhibited
type Inhibitor struct {
	category  string
	tags      models.Tags
	inhibited int32
}

func NewInhibitor(category string, tags models.Tags) *Inhibitor {
	return &Inhibitor{
		category:  category,
		tags:      tags,
		inhibited: 0,
	}
}

func (i *Inhibitor) Set(inhibited bool) {
	v := int32(0)
	if inhibited {
		v = 1
	}
	atomic.StoreInt32(&i.inhibited, v)
}

func (i *Inhibitor) IsInhibited(category string, tags models.Tags) bool {
	if atomic.LoadInt32(&i.inhibited) == 0 {
		return false
	}
	return i.isMatch(category, tags)
}

func (i *Inhibitor) isMatch(category string, tags models.Tags) bool {
	if category != i.category {
		return false
	}
	for k, v := range i.tags {
		if tags[k] != v {
			return false
		}
	}
	return true
}
