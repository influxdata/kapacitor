package alert

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	topicsBasePath         = httpd.BasePath + topicsPath
	topicsBasePathAnchored = httpd.BasePath + topicsPathAnchored

	topicEventsPath           = "events"
	topicHandlersPath         = "handlers"
	topicHandlersPathAnchored = topicHandlersPath + "/"

	eventsPattern   = "*/" + topicEventsPath
	eventPattern    = "*/" + topicEventsPath + "/*"
	handlersPattern = "*/" + topicHandlersPath
	handlerPattern  = "*/" + topicHandlersPath + "/*"

	eventsRelation   = "events"
	handlersRelation = "handlers"
)

type apiServer struct {
	Registrar    HandlerSpecRegistrar
	Topics       Topics
	Persister    TopicPersister
	routes       []httpd.Route
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
	diag Diagnostic
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
			HandlerFunc: s.handleRouteTopicGet,
		},
		{
			Method:      "POST",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopicPost,
		},
		{
			Method:      "PATCH",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopicPatch,
		},
		{
			Method:      "PUT",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopicPut,
		},
		{
			Method:      "DELETE",
			Pattern:     topicsPathAnchored,
			HandlerFunc: s.handleRouteTopicDelete,
		},
		{
			// Satisfy CORS checks.
			Method:      "OPTIONS",
			Pattern:     topicsPathAnchored,
			HandlerFunc: httpd.ServeOptions,
		},
	}

	return s.HTTPDService.AddRoutes(s.routes)
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
		httpd.HttpError(w, fmt.Sprint("invalid pattern: ", err.Error()), true, http.StatusBadRequest)
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

func (s *apiServer) handlerIDFromPath(p string) (string, bool) {
	dir, id := path.Split(p)
	if !strings.HasSuffix(dir, topicHandlersPathAnchored) {
		return "", false
	}
	return id, true
}

func (s *apiServer) eventIDFromPath(p string) (id string) {
	return path.Base(p)
}

func pathMatch(pattern, p string) (match bool) {
	match, _ = path.Match(pattern, p)
	return
}

func (s *apiServer) handleDeleteTopic(topic string, w http.ResponseWriter, r *http.Request) {
	if err := s.Persister.DeleteTopic(topic); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete topic %q: %v", topic, err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) handleRouteTopicGet(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)

	switch {
	case pathMatch(eventsPattern, p):
		s.handleListEvents(id, w, r)
	case pathMatch(eventPattern, p):
		event := s.eventIDFromPath(p)
		s.handleGetEvent(id, event, w, r)
	case pathMatch(handlersPattern, p):
		s.handleListHandlers(id, w, r)
	case pathMatch(handlerPattern, p):
		handler, _ := s.handlerIDFromPath(p)
		s.handleGetHandler(id, handler, w, r)
	default:
		s.handleGetTopic(id, w, r)
	}
}

func (s *apiServer) handleRouteTopicPost(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	topic := s.topicIDFromPath(p)
	s.handleCreateHandler(topic, w, r)
}

func (s *apiServer) handleRouteTopicPut(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	topic := s.topicIDFromPath(p)
	handler, _ := s.handlerIDFromPath(p)
	s.handlePutHandler(topic, handler, w, r)
}
func (s *apiServer) handleRouteTopicPatch(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	topic := s.topicIDFromPath(p)
	handler, _ := s.handlerIDFromPath(p)
	s.handlePatchHandler(topic, handler, w, r)
}
func (s *apiServer) handleRouteTopicDelete(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	topic := s.topicIDFromPath(p)
	handler, ok := s.handlerIDFromPath(p)
	if !ok {
		// We only have a topic path
		s.handleDeleteTopic(topic, w, r)
	} else {
		// We have a topic handler path
		s.handleDeleteHandler(topic, handler, w, r)
	}
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

func (s *apiServer) handleGetTopic(id string, w http.ResponseWriter, r *http.Request) {
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

func (s *apiServer) convertHandlerSpec(spec HandlerSpec) client.TopicHandler {
	return client.TopicHandler{
		Link:    s.topicHandlerLink(spec.Topic, spec.ID),
		ID:      spec.ID,
		Kind:    spec.Kind,
		Options: spec.Options,
		Match:   spec.Match,
	}
}

func (s *apiServer) handleListEvents(topic string, w http.ResponseWriter, r *http.Request) {
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

func (s *apiServer) handleGetEvent(topic, eventID string, w http.ResponseWriter, r *http.Request) {
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

func (s *apiServer) handleListHandlers(topic string, w http.ResponseWriter, r *http.Request) {
	pattern := r.URL.Query().Get("pattern")
	if err := validatePattern(pattern); err != nil {
		httpd.HttpError(w, fmt.Sprint("invalid pattern: ", err.Error()), true, http.StatusBadRequest)
		return
	}
	specs, err := s.Registrar.HandlerSpecs(topic, pattern)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get handler specs: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	handlers := make([]client.TopicHandler, len(specs))
	for i, spec := range specs {
		handlers[i] = s.convertHandlerSpec(spec)
	}
	sort.Sort(sortedHandlers(handlers))
	th := client.TopicHandlers{
		Link:     client.Link{Relation: client.Self, Href: r.URL.String()},
		Topic:    topic,
		Handlers: handlers,
	}
	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(th, true))
}

type sortedHandlers []client.TopicHandler

func (s sortedHandlers) Len() int               { return len(s) }
func (s sortedHandlers) Less(i int, j int) bool { return s[i].ID < s[j].ID }
func (s sortedHandlers) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

func (s *apiServer) handlerSpecFromJSON(topic string, r io.Reader) (HandlerSpec, error) {
	handlerSpec := HandlerSpec{}
	err := json.NewDecoder(r).Decode(&handlerSpec)
	if err != nil {
		return HandlerSpec{}, err
	}
	handlerSpec.Topic = topic
	return handlerSpec, nil
}

func (s *apiServer) handleCreateHandler(topic string, w http.ResponseWriter, r *http.Request) {
	handlerSpec, err := s.handlerSpecFromJSON(topic, r.Body)
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

func (s *apiServer) handlePatchHandler(topic, handler string, w http.ResponseWriter, r *http.Request) {
	spec, ok, err := s.Registrar.HandlerSpec(topic, handler)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get handler %q: %v", handler, err), true, http.StatusInternalServerError)
		return
	}
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", handler), true, http.StatusNotFound)
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
	newSpec := HandlerSpec{
		Topic: topic,
	}
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

func (s *apiServer) handlePutHandler(topic, handler string, w http.ResponseWriter, r *http.Request) {
	spec, ok, err := s.Registrar.HandlerSpec(topic, handler)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get handler %q: %v", handler, err), true, http.StatusInternalServerError)
		return
	}
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", handler), true, http.StatusNotFound)
		return
	}

	newSpec, err := s.handlerSpecFromJSON(topic, r.Body)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to unmarshal JSON: ", err.Error()), true, http.StatusBadRequest)
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

func (s *apiServer) handleDeleteHandler(topic, handler string, w http.ResponseWriter, r *http.Request) {
	if err := s.Registrar.DeregisterHandlerSpec(topic, handler); err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to delete handler: ", err.Error()), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) handleGetHandler(topic, handler string, w http.ResponseWriter, r *http.Request) {
	spec, ok, err := s.Registrar.HandlerSpec(topic, handler)
	if err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to get handler %q: %v", handler, err), true, http.StatusInternalServerError)
		return
	}
	if !ok {
		httpd.HttpError(w, fmt.Sprintf("unknown handler: %q", handler), true, http.StatusNotFound)
		return
	}

	h := s.convertHandlerSpec(spec)

	w.WriteHeader(http.StatusOK)
	w.Write(httpd.MarshalJSON(h, true))
}
