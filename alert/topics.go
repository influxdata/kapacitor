package alert

import (
	"fmt"
	"log"
	"path"
	"sort"
	"sync"
)

const (
	// eventBufferSize is the number of events to buffer to each handler per topic.
	eventBufferSize = 100
)

type Topics struct {
	mu sync.RWMutex

	topics map[string]*Topic

	logger *log.Logger
}

func NewTopics(l *log.Logger) *Topics {
	s := &Topics{
		topics: make(map[string]*Topic),
		logger: l,
	}
	return s
}

func (s *Topics) Open() error {
	return nil
}

func (s *Topics) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, t := range s.topics {
		t.close()
		delete(s.topics, topic)
	}
	return nil
}

func (s *Topics) Topic(id string) (*Topic, bool) {
	s.mu.RLock()
	t, ok := s.topics[id]
	s.mu.RUnlock()
	return t, ok
}

func (s *Topics) RestoreTopic(id string, eventStates map[string]EventState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.topics[id]
	if !ok {
		t = newTopic(id)
		s.topics[id] = t
	}
	t.restoreEventStates(eventStates)
}

func (s *Topics) EventState(topic, event string) (EventState, bool) {
	s.mu.RLock()
	t, ok := s.topics[topic]
	s.mu.RUnlock()
	if !ok {
		return EventState{}, false
	}
	return t.EventState(event)
}

func (s *Topics) Collect(event Event) error {
	s.mu.RLock()
	topic := s.topics[event.Topic]
	s.mu.RUnlock()

	if topic == nil {
		// Create the empty topic
		s.mu.Lock()
		// Check again if the topic was created, now that we have the write lock
		topic = s.topics[event.Topic]
		if topic == nil {
			topic = newTopic(event.Topic)
			s.topics[event.Topic] = topic
		}
		s.mu.Unlock()
	}

	return topic.handleEvent(event)
}

func (s *Topics) DeleteTopic(topic string) {
	s.mu.Lock()
	t := s.topics[topic]
	delete(s.topics, topic)
	s.mu.Unlock()
	if t != nil {
		t.close()
	}
}

func (s *Topics) RegisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newTopic(topic)
		}
		s.topics[topic].addHandler(h)
	}
}

func (s *Topics) DeregisterHandler(topics []string, h Handler) {
	if len(topics) == 0 || h == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range topics {
		s.topics[topic].removeHandler(h)
	}
}

func (s *Topics) ReplaceHandler(oldTopics, newTopics []string, oldH, newH Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, topic := range oldTopics {
		s.topics[topic].removeHandler(oldH)
	}

	for _, topic := range newTopics {
		if _, ok := s.topics[topic]; !ok {
			s.topics[topic] = newTopic(topic)
		}
		s.topics[topic].addHandler(newH)
	}
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Topics) TopicStatus(pattern string, minLevel Level) map[string]Level {
	s.mu.RLock()
	res := make(map[string]Level, len(s.topics))
	for _, topic := range s.topics {
		if !match(pattern, topic.ID()) {
			continue
		}
		level := topic.MaxLevel()
		if level >= minLevel {
			res[topic.ID()] = level
		}
	}
	s.mu.RUnlock()
	return res
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return
// at least 'minLevel' severity
func (s *Topics) TopicStatusEvents(pattern string, minLevel Level) map[string]map[string]EventState {
	s.mu.RLock()
	topics := make([]*Topic, 0, len(s.topics))
	for _, topic := range s.topics {
		if topic.MaxLevel() >= minLevel && match(pattern, topic.id) {
			topics = append(topics, topic)
		}
	}
	s.mu.RUnlock()

	res := make(map[string]map[string]EventState, len(topics))

	for _, topic := range topics {
		res[topic.ID()] = topic.EventStates(minLevel)
	}

	return res
}

func match(pattern, id string) bool {
	if pattern == "" {
		return true
	}
	matched, _ := path.Match(pattern, id)
	return matched
}

type Topic struct {
	id string

	mu sync.RWMutex

	events map[string]*EventState
	sorted []*EventState

	handlers []*bufHandler
}

func newTopic(id string) *Topic {
	return &Topic{
		id:     id,
		events: make(map[string]*EventState),
	}
}
func (t *Topic) ID() string {
	return t.id
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

func (t *Topic) addHandler(h Handler) {
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

func (t *Topic) removeHandler(h Handler) {
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

func (t *Topic) restoreEventStates(eventStates map[string]EventState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = make(map[string]*EventState, len(eventStates))
	t.sorted = make([]*EventState, 0, len(eventStates))
	for id, state := range eventStates {
		e := new(EventState)
		*e = state
		t.events[id] = e
		t.sorted = append(t.sorted, e)
	}
	sort.Sort(sortedStates(t.sorted))
}

func (t *Topic) EventStates(minLevel Level) map[string]EventState {
	t.mu.RLock()
	events := make(map[string]EventState, len(t.sorted))
	for _, e := range t.sorted {
		if e.Level < minLevel {
			break
		}
		events[e.ID] = *e
	}
	t.mu.RUnlock()
	return events
}

func (t *Topic) EventState(event string) (EventState, bool) {
	t.mu.RLock()
	state, ok := t.events[event]
	t.mu.RUnlock()
	if ok {
		return *state, true
	}
	return EventState{}, false
}

func (t *Topic) close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	// Close all handlers
	for _, h := range t.handlers {
		h.Close()
	}
	t.handlers = nil
}

func (t *Topic) handleEvent(event Event) error {
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

// bufHandler wraps a Handler implementation in order to provide buffering and non-blocking event handling.
type bufHandler struct {
	h        Handler
	events   chan Event
	aborting chan struct{}
	wg       sync.WaitGroup
}

func newHandler(h Handler) *bufHandler {
	hdlr := &bufHandler{
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

func (h *bufHandler) Equal(o Handler) (b bool) {
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

func (h *bufHandler) Close() {
	close(h.events)
	h.wg.Wait()
}

func (h *bufHandler) Abort() {
	close(h.aborting)
	h.wg.Wait()
}

func (h *bufHandler) Handle(event Event) error {
	select {
	case h.events <- event:
		return nil
	default:
		return fmt.Errorf("failed to deliver event %q to handler", event.State.ID)
	}
}

func (h *bufHandler) run() {
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
