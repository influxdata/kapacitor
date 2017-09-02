package alert

import (
	"encoding"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"sync"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type Service struct {
	mu sync.RWMutex

	specsDAO  HandlerSpecDAO
	topicsDAO TopicStateDAO

	APIServer *apiServer

	handlers map[string]map[string]handler

	closedTopics map[string]bool

	topics         *alert.Topics
	EventCollector EventCollector

	HTTPDService interface {
		AddPreviewRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}

	StorageService interface {
		Store(namespace string) storage.Interface
		Register(name string, store storage.StoreActioner)
		Versions() storage.Versions
	}

	Commander command.Commander

	diagnostic diagnostic.Diagnostic

	AlertaService interface {
		DefaultHandlerConfig() alerta.HandlerConfig
		Handler(alerta.HandlerConfig, diagnostic.Diagnostic) (alert.Handler, error)
	}
	HipChatService interface {
		Handler(hipchat.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	MQTTService interface {
		Handler(mqtt.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	OpsGenieService interface {
		Handler(opsgenie.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	PagerDutyService interface {
		Handler(pagerduty.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	PushoverService interface {
		Handler(pushover.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	HTTPPostService interface {
		Handler(httppost.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	SensuService interface {
		Handler(sensu.HandlerConfig, diagnostic.Diagnostic) (alert.Handler, error)
	}
	SlackService interface {
		Handler(slack.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	SMTPService interface {
		Handler(smtp.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	SNMPTrapService interface {
		Handler(snmptrap.HandlerConfig, diagnostic.Diagnostic) (alert.Handler, error)
	}
	TalkService interface {
		Handler(diagnostic.Diagnostic) alert.Handler
	}
	TelegramService interface {
		Handler(telegram.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
	VictorOpsService interface {
		Handler(victorops.HandlerConfig, diagnostic.Diagnostic) alert.Handler
	}
}

func NewService(d diagnostic.Diagnostic) *Service {
	s := &Service{
		handlers:     make(map[string]map[string]handler),
		closedTopics: make(map[string]bool),
		topics:       alert.NewTopics(d),
		diagnostic:   d,
	}
	s.APIServer = &apiServer{
		Registrar:  s,
		Topics:     s,
		Persister:  s,
		diagnostic: d,
	}
	s.EventCollector = s
	return s
}

const (
	// Public name of the handler specs store.
	handlerSpecsAPIName = "handler-specs"
	// Public name of the handler specs store.
	topicStatesAPIName = "topic-states"
	// The storage namespace for all task data.
	alertNamespace = "alert_store"
)

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
	s.StorageService.Register(handlerSpecsAPIName, s.specsDAO)
	topicsDAO, err := newTopicStateKV(store)
	if err != nil {
		return err
	}
	s.topicsDAO = topicsDAO
	s.StorageService.Register(topicStatesAPIName, s.topicsDAO)

	// Migrate v1.2 handlers
	if err := s.migrateHandlerSpecs(store); err != nil {
		return err
	}

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

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics.Close()
	return s.APIServer.Close()
}

const (
	handlerSpecsStoreVersion  = "alert_topic_handler_specs"
	handlerSpecsStoreVersion1 = "1"
)

func (s *Service) migrateHandlerSpecs(store storage.Interface) error {
	specVersion, err := s.StorageService.Versions().Get(handlerSpecsStoreVersion)
	if err != nil && err != storage.ErrNoKeyExists {
		return err
	}
	if specVersion == handlerSpecsStoreVersion1 {
		// Already migrated
		return nil
	}
	s.diagnostic.Diag(
		"level", "debug",
		"msg", "migrating old v1.2 handler specs",
	)

	// v1.2 HandlerActionSpec
	type oldHandlerActionSpec struct {
		Kind    string                 `json:"kind"`
		Options map[string]interface{} `json:"options"`
	}

	// v1.2 HandlerSpec
	type oldHandlerSpec struct {
		ID      string                 `json:"id"`
		Topics  []string               `json:"topics"`
		Actions []oldHandlerActionSpec `json:"actions"`
	}
	oldDataPrefix := "/" + handlerPrefix + "/data"
	oldKeyPattern := regexp.MustCompile(fmt.Sprintf(`^%s/[-\._\p{L}0-9]+$`, oldDataPrefix))

	// Process to migrate to new handler specs:
	//     1. Gather all old handlers
	//     2. Define new handlers that are equivalent
	//     3. Check that there are no ID conflicts
	//     4. Save the new handlers
	//     5. Delete the old specs
	//
	// All steps are performed in a single transaction,
	// so it can be rolledback in case of an error.
	err = store.Update(func(tx storage.Tx) error {
		var newHandlers []HandlerSpec
		kvs, err := tx.List(oldDataPrefix)
		if err != nil {
			return err
		}
		//s.logger.Printf("D! found %d handler rows", len(kvs))
		s.diagnostic.Diag(
			"level", "debug",
			"hanler_rows", len(kvs), // TODO: idk about this
		)

		var oldKeys []string
		for _, kv := range kvs {
			if !oldKeyPattern.MatchString(kv.Key) {
				//s.logger.Println("D! found new handler skipping:", kv.Key)
				s.diagnostic.Diag(
					"level", "debug",
					"msg", "found new handler skipping",
					"key", kv.Key,
				)
				continue
			}
			oldKeys = append(oldKeys, kv.Key)
			var old oldHandlerSpec
			err := storage.VersionJSONDecode(kv.Value, func(version int, dec *json.Decoder) error {
				if version != 1 {
					return fmt.Errorf("old handler specs should all be version 1, got version %d", version)
				}
				return dec.Decode(&old)
			})
			if err != nil {
				return errors.Wrapf(err, "failed to read old handler spec data for %s", kv.Key)
			}

			//s.logger.Println("D! migrating old handler spec", old.ID)
			s.diagnostic.Diag(
				"level", "debug",
				"msg", "migrating old handler spec",
				"id", old.ID, // TODO: eh?
			)

			// Create new handlers from the old
			hasStateChangesOnly := false
			aggregatePrefix := ""
			for i, action := range old.Actions {
				new := HandlerSpec{
					ID:      fmt.Sprintf("%s-%s-%d", old.ID, action.Kind, i),
					Kind:    action.Kind,
					Options: action.Options,
				}
				if hasStateChangesOnly {
					new.Match = "changed() == TRUE"
				}
				switch action.Kind {
				case "stateChangesOnly":
					hasStateChangesOnly = true
					// No need to add a handler for this one
				case "aggregate":
					newPrefix := aggregatePrefix + "aggregate_topic-" + old.ID + "-"
					for _, topic := range old.Topics {
						new.Topic = aggregatePrefix + topic
						new.Options["topic"] = newPrefix + topic
						newHandlers = append(newHandlers, new)
					}
					aggregatePrefix = newPrefix
				default:
					for _, topic := range old.Topics {
						new.Topic = aggregatePrefix + topic
						newHandlers = append(newHandlers, new)
					}
				}
			}
		}

		// Check that all new handlers are unique
		for _, handler := range newHandlers {
			if _, err := s.specsDAO.GetTx(tx, handler.Topic, handler.ID); err != ErrNoHandlerSpecExists {
				return fmt.Errorf("handler %q for topic %q already exists", handler.ID, handler.Topic)
			}
		}

		s.diagnostic.Diag(
			"level", "debug",
			"msg", "creating new hanlders in place of old handlers",
			"count", len(newHandlers), // TODO: idk?
		)

		// Create new handlers
		for _, handler := range newHandlers {
			if err := s.specsDAO.CreateTx(tx, handler); err != nil {
				return errors.Wrap(err, "failed to create new handler during migration")
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Save version
	return s.StorageService.Versions().Set(handlerSpecsStoreVersion, handlerSpecsStoreVersion1)
}

func (s *Service) loadSavedHandlerSpecs() error {
	offset := 0
	limit := 100
	for {
		specs, err := s.specsDAO.List("*", "", offset, limit)
		if err != nil {
			return err
		}

		for _, spec := range specs {
			if err := s.loadHandlerSpec(spec); err != nil {
				s.diagnostic.Diag(
					"level", "error",
					"msg", "failed to load handler on startup",
					"error", err,
				)

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

func validatePattern(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}

// set a topic handler in the internal map, caller must have lock.
func (s *Service) setTopicHandler(topic, id string, h handler) {
	if s.handlers[topic] == nil {
		s.handlers[topic] = make(map[string]handler)
	}
	s.handlers[topic][id] = h
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
	for _, h := range s.handlers[topic] {
		s.topics.RegisterHandler(topic, h.Handler)
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

func (s *Service) RegisterAnonHandler(topic string, h alert.Handler) {
	s.topics.RegisterHandler(topic, h)
}

func (s *Service) DeregisterAnonHandler(topic string, h alert.Handler) {
	s.topics.DeregisterHandler(topic, h)
}

// loadHandlerSpec initializes a spec that already exists.
// Caller must have the write lock.
func (s *Service) loadHandlerSpec(spec HandlerSpec) error {
	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.setTopicHandler(spec.Topic, spec.ID, h)
	s.topics.RegisterHandler(spec.Topic, h.Handler)
	return nil
}

func (s *Service) RegisterHandlerSpec(spec HandlerSpec) error {
	if err := spec.Validate(); err != nil {
		return err
	}

	h, err := s.createHandlerFromSpec(spec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.handlers[spec.Topic][spec.ID]
	if ok {
		return fmt.Errorf("cannot register handler, handler with ID %q already exists", spec.ID)
	}

	// Persist handler spec
	if err := s.specsDAO.Create(spec); err != nil {
		return err
	}

	s.setTopicHandler(spec.Topic, spec.ID, h)

	s.topics.RegisterHandler(spec.Topic, h.Handler)
	return nil
}

type closer interface {
	Close()
}

func (s *Service) DeregisterHandlerSpec(topic, handler string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.handlers[topic][handler]

	if ok {
		// Delete handler spec
		if err := s.specsDAO.Delete(topic, handler); err != nil {
			return err
		}
		s.topics.DeregisterHandler(topic, h.Handler)

		if ha, ok := h.Handler.(closer); ok {
			ha.Close()
		}

		delete(s.handlers[h.Spec.Topic], handler)
	}
	return nil
}

func (s *Service) UpdateHandlerSpec(oldSpec, newSpec HandlerSpec) error {
	if err := newSpec.Validate(); err != nil {
		return err
	}
	if newSpec.Topic != oldSpec.Topic {
		return errors.New("cannot change topic in update")
	}
	topic := newSpec.Topic
	newH, err := s.createHandlerFromSpec(newSpec)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	oldH := s.handlers[topic][oldSpec.ID]

	// Persist new handler specs
	if newSpec.ID == oldSpec.ID {
		if err := s.specsDAO.Replace(newSpec); err != nil {
			return err
		}
	} else {
		if err := s.specsDAO.Create(newSpec); err != nil {
			return err
		}
		if err := s.specsDAO.Delete(oldSpec.Topic, oldSpec.ID); err != nil {
			return err
		}
	}

	delete(s.handlers[topic], oldSpec.ID)
	s.setTopicHandler(newSpec.Topic, newSpec.ID, newH)

	s.topics.ReplaceHandler(topic, oldH.Handler, newH.Handler)
	return nil
}

// TopicState returns the state for the specified topic.
func (s *Service) TopicState(topic string) (alert.TopicState, bool, error) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return alert.TopicState{}, false, nil
	}
	return t.State(), true, nil
}

// TopicStates returns the max alert level for each topic matching 'pattern', not returning
// any topics with max alert levels less severe than 'minLevel'
func (s *Service) TopicStates(pattern string, minLevel alert.Level) (map[string]alert.TopicState, error) {
	return s.topics.TopicState(pattern, minLevel), nil
}

// EventState returns the current state of the event.
func (s *Service) EventState(topic, event string) (alert.EventState, bool, error) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return alert.EventState{}, false, nil
	}
	state, ok := t.EventState(event)
	return state, ok, nil
}

// EventStates returns the current state of events for the specified topic.
// Only events greater or equal to minLevel will be returned
func (s *Service) EventStates(topic string, minLevel alert.Level) (map[string]alert.EventState, error) {
	t, ok := s.topics.Topic(topic)
	if !ok {
		return nil, fmt.Errorf("unknown topic %q", topic)
	}
	return t.EventStates(minLevel), nil
}

func (s *Service) HandlerSpec(topic, handler string) (HandlerSpec, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.handlers[topic][handler]
	if !ok {
		return HandlerSpec{}, false, nil
	}
	return h.Spec, true, nil
}

func (s *Service) HandlerSpecs(topic, pattern string) ([]HandlerSpec, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	handlers := make([]HandlerSpec, 0, len(s.handlers))
	for id, h := range s.handlers[topic] {
		if alert.PatternMatch(pattern, id) {
			handlers = append(handlers, h.Spec)
		}
	}
	return handlers, nil
}

func decodeOptions(options map[string]interface{}, c interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		ErrorUnused: true,
		Result:      c,
		DecodeHook:  decodeStringToTextUnmarshaler,
	})
	if err != nil {
		return errors.Wrap(err, "failed to initialize mapstructure decoder")
	}
	if err := dec.Decode(options); err != nil {
		return errors.Wrapf(err, "failed to decode options into %T", c)
	}
	return nil
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

// decodeStringToTextUnmarshaler will decode a string value into any type
// that implements the encoding.TextUnmarshaler interface.
func decodeStringToTextUnmarshaler(f, t reflect.Type, data interface{}) (interface{}, error) {
	if f.Kind() != reflect.String {
		return data, nil
	}
	isPtr := true
	if t.Kind() != reflect.Ptr {
		isPtr = false
		t = reflect.PtrTo(t)
	}
	if t.Implements(textUnmarshalerType) {
		value := reflect.New(t.Elem())
		tum := value.Interface().(encoding.TextUnmarshaler)
		str := data.(string)
		err := tum.UnmarshalText([]byte(str))
		if err != nil {
			return nil, err
		}

		if isPtr {
			return value.Interface(), nil
		}
		return reflect.Indirect(value).Interface(), nil
	}
	return data, nil
}

func (s *Service) createHandlerFromSpec(spec HandlerSpec) (handler, error) {
	var h alert.Handler
	var err error
	switch spec.Kind {
	case "aggregate":
		c := newDefaultAggregateHandlerConfig(s.EventCollector)
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = NewAggregateHandler(c, s.diagnostic)
		if err != nil {
			return handler{}, err
		}
	case "alerta":
		c := s.AlertaService.DefaultHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.AlertaService.Handler(c, s.diagnostic)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "exec":
		c := ExecHandlerConfig{
			Commander: s.Commander,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = NewExecHandler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "hipchat":
		c := hipchat.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.HipChatService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "log":
		c := DefaultLogHandlerConfig()
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = NewLogHandler(c, s.diagnostic)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "mqtt":
		c := mqtt.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.MQTTService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "opsgenie":
		c := opsgenie.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.OpsGenieService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "pagerduty":
		c := pagerduty.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.PagerDutyService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "pushover":
		c := pushover.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.PushoverService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "post":
		c := httppost.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.HTTPPostService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "publish":
		c := PublishHandlerConfig{
			ec: s.EventCollector,
		}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = NewPublishHandler(c, s.diagnostic)
	case "sensu":
		c := sensu.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.SensuService.Handler(c, s.diagnostic)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "slack":
		c := slack.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.SlackService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "smtp":
		c := smtp.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.SMTPService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "snmptrap":
		c := snmptrap.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h, err = s.SNMPTrapService.Handler(c, s.diagnostic)
		if err != nil {
			return handler{}, err
		}
		h = newExternalHandler(h)
	case "talk":
		h = s.TalkService.Handler(s.diagnostic)
		h = newExternalHandler(h)
	case "tcp":
		c := TCPHandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = NewTCPHandler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "telegram":
		c := telegram.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.TelegramService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	case "victorops":
		c := victorops.HandlerConfig{}
		err = decodeOptions(spec.Options, &c)
		if err != nil {
			return handler{}, err
		}
		h = s.VictorOpsService.Handler(c, s.diagnostic)
		h = newExternalHandler(h)
	default:
		err = fmt.Errorf("unsupported action kind %q", spec.Kind)
	}
	if spec.Match != "" {
		// Wrap handler in match handler
		h, err = newMatchHandler(spec.Match, h, s.diagnostic)
	}
	return handler{Spec: spec, Handler: h}, err
}
