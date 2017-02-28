package alert

import (
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type handler struct {
	Spec    HandlerSpec
	Handler alert.Handler
}

type handlerAction interface {
	alert.Handler
	SetNext(h alert.Handler)
	Close()
}

type Service struct {
	mu sync.RWMutex

	specsDAO  HandlerSpecDAO
	topicsDAO TopicStateDAO

	APIServer *apiServer

	handlers map[string]handler

	closedTopics map[string]bool

	topics *alert.Topics

	HTTPDService interface {
		AddPreviewRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	StorageService interface {
		Store(namespace string) storage.Interface
	}

	Commander command.Commander

	logger *log.Logger

	AlertaService interface {
		DefaultHandlerConfig() alerta.HandlerConfig
		Handler(alerta.HandlerConfig, *log.Logger) (alert.Handler, error)
	}
	HipChatService interface {
		Handler(hipchat.HandlerConfig, *log.Logger) alert.Handler
	}
	OpsGenieService interface {
		Handler(opsgenie.HandlerConfig, *log.Logger) alert.Handler
	}
	PagerDutyService interface {
		Handler(pagerduty.HandlerConfig, *log.Logger) alert.Handler
	}
	PushoverService interface {
		Handler(pushover.HandlerConfig, *log.Logger) alert.Handler
	}
	SensuService interface {
		Handler(*log.Logger) alert.Handler
	}
	SlackService interface {
		Handler(slack.HandlerConfig, *log.Logger) alert.Handler
	}
	SMTPService interface {
		Handler(smtp.HandlerConfig, *log.Logger) alert.Handler
	}
	SNMPTrapService interface {
		Handler(snmptrap.HandlerConfig, *log.Logger) (alert.Handler, error)
	}
	TalkService interface {
		Handler(*log.Logger) alert.Handler
	}
	TelegramService interface {
		Handler(telegram.HandlerConfig, *log.Logger) alert.Handler
	}
	VictorOpsService interface {
		Handler(victorops.HandlerConfig, *log.Logger) alert.Handler
	}
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		handlers:     make(map[string]handler),
		closedTopics: make(map[string]bool),
		topics:       alert.NewTopics(l),
		logger:       l,
	}
	s.APIServer = &apiServer{
		Registrar: s,
		Statuser:  s,
		persister: s,
	}
	return s
}

// The storage namespace for all task data.
const alertNamespace = "alert_store"

func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create DAO
	store := s.StorageService.Store(alertNamespace)
	specsDAO, err := newHandlerSpecKV(store)
	if err != nil {
		return err
	}
	s.specsDAO = specsDAO
	topicsDAO, err := newTopicStateKV(store)
	if err != nil {
		return err
	}
	s.topicsDAO = topicsDAO

	// Load saved handlers
	if err := s.loadSavedHandlerSpecs(); err != nil {
		return err
	}

	// Load saved topic state
	if err := s.loadSavedTopicStates(); err != nil {
		return err
	}

	s.APIServer.HTTPDService = s.HTTPDService
	if err := s.APIServer.Open(); err != nil {
		return err
	}

	return nil
}

func (s *Service) loadSavedHandlerSpecs() error {
	offset := 0
	limit := 100
	for {
		specs, err := s.specsDAO.List("", offset, limit)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			if err := s.loadHandlerSpec(spec); err != nil {
				s.logger.Println("E! failed to load handler on startup", err)
			}
		}

		offset += limit
		if len(specs) != limit {
			break
		}
	}
	return nil
}

func (s *Service) convertEventStatesToAlert(states map[string]EventState) map[string]alert.EventState {
	newStates := make(map[string]alert.EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateToAlert(id, state)
	}
	return newStates
}
func (s *Service) convertEventStateToAlert(id string, state EventState) alert.EventState {
	return alert.EventState{
		ID:       id,
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func (s *Service) convertEventStatesFromAlert(states map[string]alert.EventState) map[string]EventState {
	newStates := make(map[string]EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateFromAlert(state)
	}
	return newStates
}

func (s *Service) convertEventStateFromAlert(state alert.EventState) EventState {
	return EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: state.Duration,
		Level:    state.Level,
	}
}

func (s *Service) loadSavedTopicStates() error {
	offset := 0
	limit := 100
	for {
		topicStates, err := s.topicsDAO.List("", offset, limit)
		if err != nil {
			return err
		}

		for _, ts := range topicStates {
			s.topics.RestoreTopic(ts.Topic, s.convertEventStatesToAlert(ts.EventStates))
		}

		offset += limit
		if len(topicStates) != limit {
			break
		}
	}
	return nil
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics.Close()
	return s.APIServer.Close()
}

func validatePattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

func (s *Service) EventState(topic, event string) (alert.EventState, bool) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return alert.EventState{}, false
	}
	return t.EventState(event)
}

func (s *Service) Collect(event alert.Event) error {
	s.mu.RLock()
	closed := s.closedTopics[event.Topic]
	s.mu.RUnlock()
	if closed {
		// Restore topic
		if err := s.restoreClosedTopic(event.Topic); err != nil {
			return err
		}
	}

	err := s.topics.Collect(event)
	if err != nil {
		return err
	}
	return s.persistTopicState(event.Topic)
}

func (s *Service) persistTopicState(topic string) error {
	t, ok := s.topics.Topic(topic)
	if !ok {
		// Topic was deleted since event was collected, nothing to do.
		return nil
	}

	ts := TopicState{
		Topic:       topic,
		EventStates: s.convertEventStatesFromAlert(t.EventStates(alert.OK)),
	}
	return s.topicsDAO.Put(ts)
}

func (s *Service) restoreClosedTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closedTopics[topic] {
		// Topic already restored
		return nil
	}
	if err := s.restoreTopic(topic); err != nil {
		return err
	}
	// Topic no longer closed
	delete(s.closedTopics, topic)
	return nil
}

// restoreTopic restores a topic's state from the storage and registers any handlers.
// Caller must have lock to call.
func (s *Service) restoreTopic(topic string) error {
	// Restore events state from storage
	ts, err := s.topicsDAO.Get(topic)
	if err != nil && err != ErrNoTopicStateExists {
		return err
	} else if err != ErrNoTopicStateExists {
		s.topics.RestoreTopic(topic, s.convertEventStatesToAlert(ts.EventStates))
	} // else nothing to restore

	// Re-Register all handlers
	topics := []string{topic}
	for _, h := range s.handlers {
		if h.Spec.HasTopic(topic) {
			s.topics.RegisterHandler(topics, h.Handler)
		}
	}
	return nil
}

func (s *Service) topic(id string) (*alert.Topic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.topics.Topic(id)
	return t, ok
}

func (s *Service) RestoreTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreTopic(topic)
}

func (s *Service) CloseTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Delete running topic
	s.topics.DeleteTopic(topic)
	s.closedTopics[topic] = true

	// Save the final topic state
	return s.persistTopicState(topic)
}

func (s *Service) DeleteTopic(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.closedTopics, topic)
	s.topics.DeleteTopic(topic)
	return s.topicsDAO.Delete(topic)
}

func (s *Service) UpdateEvent(topic string, event alert.EventState) error {
	s.topics.UpdateEvent(topic, event)
	return s.persistTopicState(topic)
}

func (s *Service) RegisterHandler(topics []string, h alert.Handler) {
	s.topics.RegisterHandler(topics, h)
}

func (s *Service) DeregisterHandler(topics []string, h alert.Handler) {
	s.topics.DeregisterHandler(topics, h)
}

// loadHandlerSpec initializes a spec that already exists.
// Caller must have the write lock.
func (s *Service) loadHandlerSpec(spec HandlerSpec) error {
	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.handlers[spec.ID] = h

	s.topics.RegisterHandler(spec.Topics, h.Handler)
	return nil
}

func (s *Service) RegisterHandlerSpec(spec HandlerSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	s.mu.RLock()
	_, ok := s.handlers[spec.ID]
	s.mu.RUnlock()
	if ok {
		return fmt.Errorf("cannot register handler, handler with ID %q already exists", spec.ID)
	}

	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	// Persist handler spec
	if err := s.specsDAO.Create(spec); err != nil {
		return err
	}

	s.mu.Lock()
	s.handlers[spec.ID] = h
	s.mu.Unlock()

	s.topics.RegisterHandler(spec.Topics, h.Handler)
	return nil
}

func (s *Service) DeregisterHandlerSpec(id string) error {
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()

	if ok {
		// Delete handler spec
		if err := s.specsDAO.Delete(id); err != nil {
			return err
		}
		s.topics.DeregisterHandler(h.Spec.Topics, h.Handler)

		if ha, ok := h.Handler.(handlerAction); ok {
			ha.Close()
		}

		s.mu.Lock()
		delete(s.handlers, id)
		s.mu.Unlock()
	}
	return nil
}

func (s *Service) UpdateHandlerSpec(oldSpec, newSpec HandlerSpec) error {
	if err := newSpec.Validate(); err != nil {
		return err
	}
	newH, err := s.createHandlerFromSpec(newSpec)
	if err != nil {
		return err
	}

	s.mu.RLock()
	oldH := s.handlers[oldSpec.ID]
	s.mu.RUnlock()

	// Persist new handler specs
	if newSpec.ID == oldSpec.ID {
		if err := s.specsDAO.Replace(newSpec); err != nil {
			return err
		}
	} else {
		if err := s.specsDAO.Create(newSpec); err != nil {
			return err
		}
		if err := s.specsDAO.Delete(oldSpec.ID); err != nil {
			return err
		}
	}

	s.mu.Lock()
	delete(s.handlers, oldSpec.ID)
	s.handlers[newSpec.ID] = newH
	s.mu.Unlock()

	s.topics.ReplaceHandler(oldSpec.Topics, newSpec.Topics, oldH.Handler, newH.Handler)
	return nil
}

// TopicStatus returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStatus(pattern string, minLevel alert.Level) (map[string]alert.TopicStatus, error) {
	return s.topics.TopicStatus(pattern, minLevel), nil
}

// TopicStatusDetails is similar to TopicStatus, but will additionally return
// at least 'minLevel' severity
func (s *Service) TopicStatusEvents(pattern string, minLevel alert.Level) (map[string]map[string]alert.EventState, error) {
	return s.topics.TopicStatusEvents(pattern, minLevel), nil
}

func (s *Service) HandlerSpec(id string) (HandlerSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.handlers[id]
	if !ok {
		return HandlerSpec{}, fmt.Errorf("handler %s does not exist", id)
	}
	return h.Spec, nil
}

func (s *Service) HandlerSpecs(pattern string) ([]HandlerSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	handlers := make([]HandlerSpec, 0, len(s.handlers))
	for id, h := range s.handlers {
		if alert.PatternMatch(pattern, id) {
			handlers = append(handlers, h.Spec)
		}
	}
	return handlers, nil
}

func (s *Service) createHandlerFromSpec(spec HandlerSpec) (handler, error) {
	if 0 == len(spec.Actions) {
		return handler{}, errors.New("invalid handler spec, must have at least one action")
	}

	// Create actions chained together in a singly linked list
	var prev, first handlerAction
	for _, actionSpec := range spec.Actions {
		curr, err := s.createHandlerActionFromSpec(actionSpec)
		if err != nil {
			return handler{}, err
		}
		if first == nil {
			// Keep first action
			first = curr
		}
		if prev != nil {
			// Link previous action to current action
			prev.SetNext(curr)
		}
		prev = curr
	}

	// set a noopHandler for the last action
	prev.SetNext(noopHandler{})

	return handler{Spec: spec, Handler: first}, nil
}

func decodeOptions(options map[string]interface{}, c interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
		DecodeHook:  mapstructure.StringToTimeDurationHookFunc(),
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize mapstructure decoder")
	}
	if err := dec.Decode(options); err != nil {
		return errors.Wrapf(err, "failed to decode options into %T", c)
	}
	return nil
}

func (s *Service) createHandlerActionFromSpec(spec HandlerActionSpec) (ha handlerAction, err error) {
	switch spec.Kind {
	case "aggregate":
		c := AggregateHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		ha = NewAggregateHandler(c, s.logger)
	case "alerta":
		c := s.AlertaService.DefaultHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h, err := s.AlertaService.Handler(c, s.logger)
		if err != nil {
			return nil, err
		}
		ha = newPassThroughHandler(newExternalHandler(h))
	case "exec":
		c := ExecHandlerConfig{
			Commander: s.Commander,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := NewExecHandler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "hipchat":
		c := hipchat.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.HipChatService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "log":
		c := DefaultLogHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h, err := NewLogHandler(c, s.logger)
		if err != nil {
			return nil, err
		}
		ha = newPassThroughHandler(newExternalHandler(h))
	case "opsgenie":
		c := opsgenie.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.OpsGenieService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "pagerduty":
		c := pagerduty.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.PagerDutyService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "pushover":
		c := pushover.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.PushoverService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "post":
		c := PostHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := NewPostHandler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "publish":
		c := PublishHandlerConfig{
			topics: s.topics,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := NewPublishHandler(c, s.logger)
		ha = newPassThroughHandler(h)
	case "sensu":
		h := s.SensuService.Handler(s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "slack":
		c := slack.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.SlackService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "smtp":
		c := smtp.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.SMTPService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "snmptrap":
		c := snmptrap.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h, err := s.SNMPTrapService.Handler(c, s.logger)
		if err != nil {
			return nil, err
		}
		ha = newPassThroughHandler(newExternalHandler(h))
	case "stateChangesOnly":
		c := StateChangesOnlyHandlerConfig{
			topics: s.topics,
		}
		ha = NewStateChangesOnlyHandler(c, s.logger)
	case "talk":
		h := s.TalkService.Handler(s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "tcp":
		c := TCPHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := NewTCPHandler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "telegram":
		c := telegram.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.TelegramService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	case "victorops":
		c := victorops.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return
		}
		h := s.VictorOpsService.Handler(c, s.logger)
		ha = newPassThroughHandler(newExternalHandler(h))
	default:
		err = fmt.Errorf("unsupported action kind %q", spec.Kind)
	}
	return
}

// PassThroughHandler implements HandlerAction and passes through all events to the next handler.
type passThroughHandler struct {
	h    alert.Handler
	next alert.Handler
}

func newPassThroughHandler(h alert.Handler) *passThroughHandler {
	return &passThroughHandler{
		h: h,
	}
}

func (h *passThroughHandler) Handle(event alert.Event) {
	h.h.Handle(event)
	h.next.Handle(event)
}

func (h *passThroughHandler) SetNext(next alert.Handler) {
	h.next = next
}
func (h *passThroughHandler) Close() {
}

// NoopHandler implements Handler and does nothing with the event
type noopHandler struct{}

func (h noopHandler) Handle(event alert.Event) {}

// ExternalHandler wraps an existing handler that calls out to external services.
// The events are checked for the NoExternal flag before being passed onto the external handler.
type externalHandler struct {
	h alert.Handler
}

func (h *externalHandler) Handle(event alert.Event) {
	if !event.NoExternal {
		h.h.Handle(event)
	}
}

func newExternalHandler(h alert.Handler) *externalHandler {
	return &externalHandler{
		h: h,
	}
}
