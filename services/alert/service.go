package alert

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"sort"
	"strings"
	"sync"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/influxdata/kapacitor/alert"
	client "github.com/influxdata/kapacitor/client/v1"
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

const (
	alertsPath         = "/alerts"
	alertsPathAnchored = "/alerts/"

	topicsPath             = alertsPath + "/topics"
	topicsPathAnchored     = alertsPath + "/topics/"
	topicsBasePath         = httpd.BasePreviewPath + topicsPath
	topicsBasePathAnchored = httpd.BasePreviewPath + topicsPathAnchored

	handlersPath             = alertsPath + "/handlers"
	handlersPathAnchored     = alertsPath + "/handlers/"
	handlersBasePath         = httpd.BasePreviewPath + handlersPath
	handlersBasePathAnchored = httpd.BasePreviewPath + handlersPathAnchored

	topicEventsPath   = "events"
	topicHandlersPath = "handlers"

	eventsPattern   = "*/" + topicEventsPath
	eventPattern    = "*/" + topicEventsPath + "/*"
	handlersPattern = "*/" + topicHandlersPath

	eventsRelation   = "events"
	handlersRelation = "handlers"
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

	handlers map[string]handler

	closedTopics map[string]bool

	topics *alert.Topics

	routes       []httpd.Route
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

	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     topicsPath,
			HandlerFunc: s.handleListTopics,
		},
		{
			Method:      "GET",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopic,
		},
		{
			Method:      "DELETE",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleDeleteTopic,
		},
		{
			Method:      "GET",
			Pattern:     handlersPath,
			HandlerFunc: s.handleListHandlers,
		},
		{
			Method:      "POST",
			Pattern:     handlersPath,
			HandlerFunc: s.handleCreateHandler,
		},
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     handlersPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
		{
			Method:      "PATCH",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handlePatchHandler,
		},
		{
			Method:      "PUT",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handlePutHandler,
		},
		{
			Method:      "DELETE",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handleDeleteHandler,
		},
		{
			Method:      "GET",
			Pattern:     handlersPathAnchored,
			HandlerFunc: s.handleGetHandler,
		},
	}

	return s.HTTPDService.AddPreviewRoutes(s.routes)
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
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

func validatePattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

type sortedTopics []client.Topic

func (s sortedTopics) Len() int               { return len(s) }
func (s sortedTopics) Less(i int, j int) bool { return s[i].ID < s[j].ID }
func (s sortedTopics) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Service) handleListTopics(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalide pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := alert.ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	statuses, err := s.TopicStatus(pattern, minLevel)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get topic statuses: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	list := make([]client.Topic, 0, len(statuses))
	for topic, status := range statuses {
		list = append(list, s.createClientTopic(topic, status))
	}
	sort.Sort(sortedTopics(list))

	topics := client.Topics{
		Link:   client.Link{Relation: client.Self, Href: r.URL.String()},
		Topics: list,
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topics, true))
}

func (s *Service) topicIDFromPath(p string) (id string) {
	d := p
	for d != "." {
		id = d
		d = path.Dir(d)
	}
	return
}

func pathMatch(pattern, p string) (match bool) {
	match, _ = path.Match(pattern, p)
	return
}

func (s *Service) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	if err := s.DeleteTopic(id); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete topic %q: %v", id, err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleRouteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	t, ok := s.topics.Topic(id)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("topic %q does not exist", id), true, http.StatusNotFound)
		return
	}

	switch {
	case pathMatch(eventsPattern, p):
		s.handleListTopicEvents(t, w, r)
	case pathMatch(eventPattern, p):
		s.handleTopicEvent(t, w, r)
	case pathMatch(handlersPattern, p):
		s.handleListTopicHandlers(t, w, r)
	default:
		s.handleTopic(t, w, r)
	}
}

func (s *Service) handlerLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(handlersBasePath, id)}
}
func (s *Service) topicLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, id)}
}
func (s *Service) topicEventsLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicEventsPath)}
}
func (s *Service) topicEventLink(topic, event string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicEventsPath, event)}
}
func (s *Service) topicHandlersLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicHandlersPath)}
}
func (s *Service) topicHandlerLink(topic, handler string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicHandlersPath, handler)}
}

func (s *Service) createClientTopic(topic string, status alert.TopicStatus) client.Topic {
	return client.Topic{
		ID:           topic,
		Link:         s.topicLink(topic),
		Level:        status.Level.String(),
		Collected:    status.Collected,
		EventsLink:   s.topicEventsLink(topic, eventsRelation),
		HandlersLink: s.topicHandlersLink(topic, handlersRelation),
	}
}

func (s *Service) handleTopic(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	topic := s.createClientTopic(t.ID(), t.Status())

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topic, true))
}

func (s *Service) convertEventStatesFromAlert(states map[string]alert.EventState) map[string]EventState {
	newStates := make(map[string]EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateFromAlert(state)
	}
	return newStates
}

func (s *Service) convertEventStatesToAlert(states map[string]EventState) map[string]alert.EventState {
	newStates := make(map[string]alert.EventState, len(states))
	for id, state := range states {
		newStates[id] = s.convertEventStateToAlert(id, state)
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

func (s *Service) convertEventStateToClient(state alert.EventState) client.EventState {
	return client.EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: client.Duration(state.Duration),
		Level:    state.Level.String(),
	}
}

func (s *Service) convertHandlerSpec(spec HandlerSpec) client.Handler {
	actions := make([]client.HandlerAction, 0, len(spec.Actions))
	for _, actionSpec := range spec.Actions {
		action := client.HandlerAction{
			Kind:    actionSpec.Kind,
			Options: actionSpec.Options,
		}
		actions = append(actions, action)
	}
	return client.Handler{
		Link:    s.handlerLink(spec.ID),
		ID:      spec.ID,
		Topics:  spec.Topics,
		Actions: actions,
	}
}

func (s *Service) handleListTopicEvents(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := alert.ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	events := t.EventStates(minLevel)
	res := client.TopicEvents{
		Link:   s.topicEventsLink(t.ID(), client.Self),
		Topic:  t.ID(),
		Events: make([]client.TopicEvent, 0, len(events)),
	}
	for id, state := range events {
		res.Events = append(res.Events, client.TopicEvent{
			Link:  s.topicEventLink(t.ID(), id),
			ID:    id,
			State: s.convertEventStateToClient(state),
		})
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(res, true))
}

func (s *Service) handleTopicEvent(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	id := path.Base(r.URL.Path)
	state, ok := t.EventState(id)
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("event %q does not exist for topic %q", id, t.ID()), true, http.StatusNotFound)
		return
	}
	event := client.TopicEvent{
		Link:  s.topicEventLink(t.ID(), id),
		ID:    id,
		State: s.convertEventStateToClient(state),
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(event, true))
}

func (s *Service) handleListTopicHandlers(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	var handlers []client.Handler
	for _, h := range s.handlers {
		if h.Spec.HasTopic(t.ID()) {
			handlers = append(handlers, s.convertHandlerSpec(h.Spec))
		}
	}
	sort.Sort(sortedHandlers(handlers))
	th := client.TopicHandlers{
		Link:     s.topicHandlersLink(t.ID(), client.Self),
		Topic:    t.ID(),
		Handlers: handlers,
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(th, true))
}

type sortedHandlers []client.Handler

func (s sortedHandlers) Len() int               { return len(s) }
func (s sortedHandlers) Less(i int, j int) bool { return s[i].ID < s[j].ID }
func (s sortedHandlers) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *Service) handleListHandlers(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	specs, err := s.HandlerSpecs(pattern)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get handler specs: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	list := make([]client.Handler, len(specs))
	for i := range specs {
		list[i] = s.convertHandlerSpec(specs[i])
	}
	sort.Sort(sortedHandlers(list))

	handlers := client.Handlers{
		Link:     client.Link{Relation: client.Self, Href: r.URL.String()},
		Handlers: list,
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(handlers, true))
}

func (s *Service) handleCreateHandler(w http.ResponseWriter, r *http.Request) {
	handlerSpec := HandlerSpec{}
	err := json.NewDecoder(r.Body).Decode(&handlerSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler json: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	if err := handlerSpec.Validate(); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler spec: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	err = s.RegisterHandlerSpec(handlerSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to create handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	h := s.convertHandlerSpec(handlerSpec)
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(h, true))
}

func (s *Service) handlePatchHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	patchBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to read request body: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid patch json: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	specBytes, err := json.Marshal(h.Spec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to marshal JSON: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	newBytes, err := patch.Apply(specBytes)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to apply patch: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	newSpec := HandlerSpec{}
	if err := json.Unmarshal(newBytes, &newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmarshal patched json: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	if err := newSpec.Validate(); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler spec: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	if err := s.UpdateHandlerSpec(h.Spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *Service) handlePutHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	newSpec := HandlerSpec{}
	err := json.NewDecoder(r.Body).Decode(&newSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmar JSON: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	if err := newSpec.Validate(); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler spec: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	if err := s.UpdateHandlerSpec(h.Spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *Service) handleDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	if err := s.DeregisterHandlerSpec(id); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to delete handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	s.mu.RLock()
	h, ok := s.handlers[id]
	s.mu.RUnlock()
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	ch := s.convertHandlerSpec(h.Spec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
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
		if match(pattern, id) {
			handlers = append(handlers, h.Spec)
		}
	}
	return handlers, nil
}
func match(pattern, id string) bool {
	if pattern == "" {
		return true
	}
	matched, _ := path.Match(pattern, id)
	return matched
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
