package alert

import (
	"context"
	"log"
	"sort"
	"sync"

	"github.com/pkg/errors"
)

type Service struct {
	mu sync.RWMutex

	topics map[string]*Topic
	// Map topic name -> []Handler
	handlers map[string][]Handler

	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		topics:   make(map[string]*Topic),
		handlers: make(map[string][]Handler),
		logger:   l,
	}

	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Collect(event Event) error {
	s.mu.RLock()
	topic := s.topics[event.Topic]
	handlers := s.handlers[event.Topic]
	s.mu.RUnlock()

	if topic == nil {
		return nil
	}
	s.logger.Println("D! handling event", event.Topic, event.State.ID)
	topic.UpdateEvent(event.State)
	ctxt := context.TODO()
	for _, h := range handlers {
		// TODO do not return early, collect all errors
		err := h.Handle(ctxt, event)
		if err != nil {
			return errors.Wrapf(err, "handler %s failed", h.Name())
		}
	}
	return nil
}

func (s *Service) DeleteTopic(topic string) {
	s.mu.Lock()
	delete(s.topics, topic)
	delete(s.handlers, topic)
	s.mu.Unlock()
}

func (s *Service) RegisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

TOPICS:
	for _, topic := range topics {
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newTopic(topic)
		}

		handlers := s.handlers[topic]
		for _, cur := range handlers {
			if cur == h {
				continue TOPICS
			}
		}
		s.handlers[topic] = append(handlers, h)
	}
}

func (s *Service) DeregisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

TOPICS:
	for _, topic := range topics {
		handlers := s.handlers[topic]
		for i := 0; i < len(handlers); i++ {
			if handlers[i] == h {
				if i < len(handlers)-1 {
					handlers[i] = handlers[len(handlers)-1]
				}
				s.handlers[topic] = handlers[:len(handlers)-1]
				continue TOPICS
			}
		}
	}
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
//
// TODO: implement pattern restriction
func (s *Service) TopicStatus(pattern string, minLevel Level) map[string]Level {
	s.mu.RLock()
	res := make(map[string]Level, len(s.topics))
	for name, topic := range s.topics {
		level := topic.MaxLevel()
		if level >= minLevel && match(pattern, topic.name) {
			res[name] = level
		}
	}
	s.mu.RUnlock()
	return res
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return

// at least 'minLevel' severity
//
// TODO: implement pattern restriction
func (s *Service) TopicStatusDetails(pattern string, minLevel Level) map[string]map[string]EventState {
	s.mu.RLock()
	topics := make([]*Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		if topic.MaxLevel() >= minLevel && match(pattern, topic.name) {
			topics = append(topics, topic)
		}
	}
	s.mu.RUnlock()

	res := make(map[string]map[string]EventState, len(topics))

	for _, topic := range topics {
		// TODO: move this into a method of Topic
		topic.mu.RLock()
		idx := sort.Search(len(topic.sorted), func(i int) bool {
			return topic.sorted[i].Level < minLevel
		})
		if idx > 0 {
			ids := make(map[string]EventState, idx)
			for i := 0; i < idx; i++ {
				ids[topic.sorted[i].ID] = *topic.sorted[i]
			}
			res[topic.name] = ids
		}
		topic.mu.RUnlock()
	}

	return res
}

func match(pattern, name string) bool {
	return true
}

type Topic struct {
	name string

	mu     sync.RWMutex
	events map[string]*EventState
	sorted []*EventState
}

func newTopic(name string) *Topic {
	return &Topic{
		name:   name,
		events: make(map[string]*EventState),
	}
}

func (t *Topic) MaxLevel() Level {
	level := OK
	t.mu.RLock()
	if len(t.sorted) > 0 {
		level = t.sorted[0].Level
	}
	t.mu.RUnlock()
	return level
}

// UpdateEvent will store the latest state for the given ID and return true if
// the update caused a level change for the ID
func (t *Topic) UpdateEvent(state EventState) bool {
	var needSort bool
	t.mu.Lock()
	cur := t.events[state.ID]
	if cur == nil {
		needSort = true
		cur = new(EventState)
		t.events[state.ID] = cur
		t.sorted = append(t.sorted, cur)
	}
	needSort = needSort || cur.Level != state.Level

	*cur = state

	if needSort {
		sort.Sort(sortedStates(t.sorted))
	}

	t.mu.Unlock()

	return needSort
}

type sortedStates []*EventState

func (e sortedStates) Len() int          { return len(e) }
func (e sortedStates) Swap(i int, j int) { e[i], e[j] = e[j], e[i] }
func (e sortedStates) Less(i int, j int) bool {
	if e[i].Level > e[j].Level {
		return true
	}
	return e[i].ID < e[j].ID
}
