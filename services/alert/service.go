package alert

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

// eventBufferSize is the number of events to buffer to each handler per topic.
const eventBufferSize = 100

type Service struct {
	mu sync.RWMutex

	topics map[string]*Topic
	logger *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		topics: make(map[string]*Topic),
		logger: l,
	}

	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, t := range s.topics {
		t.Close()
		delete(s.topics, topic)
	}
	return nil
}

func (s *Service) Collect(event Event) error {
	s.mu.RLock()
	topic := s.topics[event.Topic]
	s.mu.RUnlock()

	if topic == nil {
		return nil
	}

	return topic.Handle(event)
}

func (s *Service) DeleteTopic(topic string) {
	s.mu.Lock()
	t := s.topics[topic]
	delete(s.topics, topic)
	s.mu.Unlock()
	if t != nil {
		t.Close()
	}
}

func (s *Service) RegisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newTopic(topic)
		}
		s.topics[topic].AddHandler(h)
	}
}

func (s *Service) DeregisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		s.topics[topic].RemoveHandler(h)
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

	mu sync.RWMutex

	events map[string]*EventState
	sorted []*EventState

	handlers []*handler
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

func (t *Topic) AddHandler(h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, cur := range t.handlers {
		if cur.Equal(h) {
			return
		}
	}
	hdlr := newHandler(h)
	t.handlers = append(t.handlers, hdlr)
}

func (t *Topic) RemoveHandler(h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < len(t.handlers); i++ {
		if t.handlers[i].Equal(h) {
			// Close handler
			t.handlers[i].Close()
			if i < len(t.handlers)-1 {
				t.handlers[i] = t.handlers[len(t.handlers)-1]
			}
			t.handlers = t.handlers[:len(t.handlers)-1]
			break
		}
	}
}

func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Close all handlers
	for _, h := range t.handlers {
		h.Close()
	}
	t.handlers = nil
}

func (t *Topic) Handle(event Event) error {
	t.updateEvent(event.State)
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Handle event
	var errs multiError
	for _, h := range t.handlers {
		err := h.Handle(event)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) != 0 {
		return errs
	}
	return nil
}

// updateEvent will store the latest state for the given ID.
func (t *Topic) updateEvent(state EventState) {
	var needSort bool
	t.mu.Lock()
	defer t.mu.Unlock()
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

// handler wraps a Handler implementation in order to provide buffering and non-blocking event handling.
type handler struct {
	h        Handler
	events   chan Event
	aborting chan struct{}
	wg       sync.WaitGroup
}

func newHandler(h Handler) *handler {
	hdlr := &handler{
		h:        h,
		events:   make(chan Event, eventBufferSize),
		aborting: make(chan struct{}),
	}
	hdlr.wg.Add(1)
	go func() {
		defer hdlr.wg.Done()
		hdlr.run()
	}()
	return hdlr
}

func (h *handler) Equal(o Handler) (b bool) {
	defer func() {
		// Recover in case the interface concrete type is not a comparable type.
		r := recover()
		if r != nil {
			b = false
		}
	}()
	b = h.h == o
	return
}

func (h *handler) Close() {
	close(h.events)
	h.wg.Wait()
}

func (h *handler) Abort() {
	close(h.aborting)
	h.wg.Wait()
}

func (h *handler) Handle(event Event) error {
	select {
	case h.events <- event:
		return nil
	default:
		return fmt.Errorf("failed to deliver event %q to handler", event.State.ID)
	}
}

func (h *handler) run() {
	for {
		select {
		case event, ok := <-h.events:
			if !ok {
				return
			}
			h.h.Handle(event)
		case <-h.aborting:
			return
		}
	}
}

// multiError is a list of errors.
type multiError []error

func (e multiError) Error() string {
	if len(e) == 1 {
		return e[0].Error()
	}
	msg := "multiple errors:"
	for _, err := range e {
		msg += "\n" + err.Error()
	}
	return msg
}
