package alert

import (
	"encoding/json"
	"fmt"
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
	Statuser     TopicStatuser
	persister    TopicPersister
	routes       []httpd.Route
	HTTPDService interface {
		AddPreviewRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
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
	s.HTTPDService.DelRoutes(s.routes)
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
	statuses, err := s.Statuser.TopicStatus(pattern, minLevel)
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
	if err := s.persister.DeleteTopic(id); err != nil {
		httpd.HttpError(w, fmt.Sprintf("failed to delete topic %q: %v", id, err), true, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *apiServer) handleRouteTopic(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, topicsBasePathAnchored)
	id := s.topicIDFromPath(p)
	t, ok := s.persister.topic(id)
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

func (s *apiServer) createClientTopic(topic string, status alert.TopicStatus) client.Topic {
	return client.Topic{
		ID:           topic,
		Link:         s.topicLink(topic),
		Level:        status.Level.String(),
		Collected:    status.Collected,
		EventsLink:   s.topicEventsLink(topic, eventsRelation),
		HandlersLink: s.topicHandlersLink(topic, handlersRelation),
	}
}

func (s *apiServer) handleTopic(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	topic := s.createClientTopic(t.ID(), t.Status())

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

func (s *apiServer) handleListTopicEvents(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
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

func (s *apiServer) handleTopicEvent(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
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

func (s *apiServer) handleListTopicHandlers(t *alert.Topic, w http.ResponseWriter, r *http.Request) {
	var handlers []client.Handler
	specs, err := s.Registrar.HandlerSpecs("")
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to get handler specs: ", err.Error()), true, http.StatusInternalServerError)
		return
	}

	for _, spec := range specs {
		if spec.HasTopic(t.ID()) {
			handlers = append(handlers, s.convertHandlerSpec(spec))
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
