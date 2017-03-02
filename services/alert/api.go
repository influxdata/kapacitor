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

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/influxdata/kapacitor/alert"
	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/httpd"
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

type apiServer struct {
	Registrar    HandlerSpecRegistrar
	Topics       Topics
	Persister    TopicPersister
	routes       []httpd.Route
	HTTPDService interface {
		AddPreviewRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	logger *log.Logger
}

func (s *apiServer) Open() error {
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

func (s *apiServer) Close() error {
	if s.HTTPDService != nil {
		s.HTTPDService.DelRoutes(s.routes)
	}
	return nil
}

type sortedTopics []client.Topic

func (s sortedTopics) Len() int               { return len(s) }
func (s sortedTopics) Less(i int, j int) bool { return s[i].ID < s[j].ID }
func (s sortedTopics) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *apiServer) handleListTopics(w http.ResponseWriter, r *http.Request) {
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
	states, err := s.Topics.TopicStates(pattern, minLevel)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get topic states: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	list := make([]client.Topic, 0, len(states))
	for topic, state := range states {
		list = append(list, s.createClientTopic(topic, state))
	}
	sort.Sort(sortedTopics(list))

	topics := client.Topics{
		Link:   client.Link{Relation: client.Self, Href: r.URL.String()},
		Topics: list,
	}

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topics, true))
}

func (s *apiServer) topicIDFromPath(p string) (id string) {
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

func (s *apiServer) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	if err := s.Persister.DeleteTopic(id); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete topic %q: %v", id, err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) handleRouteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)

	switch {
	case pathMatch(eventsPattern, p):
		s.handleListTopicEvents(id, w, r)
	case pathMatch(eventPattern, p):
		s.handleTopicEvent(id, w, r)
	case pathMatch(handlersPattern, p):
		s.handleListTopicHandlers(id, w, r)
	default:
		s.handleTopic(id, w, r)
	}
}

func (s *apiServer) handlerLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(handlersBasePath, id)}
}
func (s *apiServer) topicLink(id string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, id)}
}
func (s *apiServer) topicEventsLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicEventsPath)}
}
func (s *apiServer) topicEventLink(topic, event string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicEventsPath, event)}
}
func (s *apiServer) topicHandlersLink(id string, r client.Relation) client.Link {
	return client.Link{Relation: r, Href: path.Join(topicsBasePath, id, topicHandlersPath)}
}
func (s *apiServer) topicHandlerLink(topic, handler string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(topicsBasePath, topic, topicHandlersPath, handler)}
}

func (s *apiServer) createClientTopic(topic string, state alert.TopicState) client.Topic {
	return client.Topic{
		ID:           topic,
		Link:         s.topicLink(topic),
		Level:        state.Level.String(),
		Collected:    state.Collected,
		EventsLink:   s.topicEventsLink(topic, eventsRelation),
		HandlersLink: s.topicHandlersLink(topic, handlersRelation),
	}
}

func (s *apiServer) handleTopic(id string, w http.ResponseWriter, r *http.Request) {
	state, ok, err := s.Topics.TopicState(id)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get topic state: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown topic: %q", id), true, http.StatusNotFound)
		return
	}
	topic := s.createClientTopic(id, state)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(topic, true))
}

func (s *apiServer) convertEventStateToClient(state alert.EventState) client.EventState {
	return client.EventState{
		Message:  state.Message,
		Details:  state.Details,
		Time:     state.Time,
		Duration: client.Duration(state.Duration),
		Level:    state.Level.String(),
	}
}

func (s *apiServer) convertHandlerSpec(spec HandlerSpec) client.Handler {
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

func (s *apiServer) handleListTopicEvents(topic string, w http.ResponseWriter, r *http.Request) {
	minLevelStr := r.URL.Query().Get("min-level")
	minLevel, err := alert.ParseLevel(minLevelStr)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}
	events, err := s.Topics.EventStates(topic, minLevel)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get topic events: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	res := client.TopicEvents{
		Link:   s.topicEventsLink(topic, client.Self),
		Topic:  topic,
		Events: make([]client.TopicEvent, 0, len(events)),
	}
	for id, state := range events {
		res.Events = append(res.Events, client.TopicEvent{
			Link:  s.topicEventLink(topic, id),
			ID:    id,
			State: s.convertEventStateToClient(state),
		})
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(res, true))
}

func (s *apiServer) handleTopicEvent(topic string, w http.ResponseWriter, r *http.Request) {
	eventID := path.Base(r.URL.Path)
	state, ok, err := s.Topics.EventState(topic, eventID)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get event state: %s", err.Error()), true, http.StatusInternalServerError)
		return
	}
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown event %q in topic %q", eventID, topic), true, http.StatusNotFound)
		return
	}
	event := client.TopicEvent{
		Link:  s.topicEventLink(topic, eventID),
		ID:    eventID,
		State: s.convertEventStateToClient(state),
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(event, true))
}

func (s *apiServer) handleListTopicHandlers(topic string, w http.ResponseWriter, r *http.Request) {
	var handlers []client.Handler
	specs, err := s.Registrar.HandlerSpecs("")
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get handler specs: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	for _, spec := range specs {
		if spec.HasTopic(topic) {
			handlers = append(handlers, s.convertHandlerSpec(spec))
		}
	}
	sort.Sort(sortedHandlers(handlers))
	th := client.TopicHandlers{
		Link:     s.topicHandlersLink(topic, client.Self),
		Topic:    topic,
		Handlers: handlers,
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(th, true))
}

type sortedHandlers []client.Handler

func (s sortedHandlers) Len() int               { return len(s) }
func (s sortedHandlers) Less(i int, j int) bool { return s[i].ID < s[j].ID }
func (s sortedHandlers) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *apiServer) handleListHandlers(w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	specs, err := s.Registrar.HandlerSpecs(pattern)
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

func (s *apiServer) handleCreateHandler(w http.ResponseWriter, r *http.Request) {
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

	err = s.Registrar.RegisterHandlerSpec(handlerSpec)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to create handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	h := s.convertHandlerSpec(handlerSpec)
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(h, true))
}

func (s *apiServer) handlePatchHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	spec, err := s.Registrar.HandlerSpec(id)
	if err != nil {
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
	specBytes, err := json.Marshal(spec)
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

	if err := s.Registrar.UpdateHandlerSpec(spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *apiServer) handlePutHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	spec, err := s.Registrar.HandlerSpec(id)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	newSpec := HandlerSpec{}
	if err := json.NewDecoder(r.Body).Decode(&newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmar JSON: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	if err := newSpec.Validate(); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid handler spec: ", err.Error()), true, http.StatusBadRequest)
		return
	}

	if err := s.Registrar.UpdateHandlerSpec(spec, newSpec); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	ch := s.convertHandlerSpec(newSpec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(ch, true))
}

func (s *apiServer) handleDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	if err := s.Registrar.DeregisterHandlerSpec(id); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to delete handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) handleGetHandler(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, handlersBasePathAnchored)
	spec, err := s.Registrar.HandlerSpec(id)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", id), true, http.StatusNotFound)
		return
	}

	h := s.convertHandlerSpec(spec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(h, true))
}
