package config

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	client "github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/services/config/override"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

const (
	configPath         = "/config"
	configPathAnchored = "/config/"
	configBasePath     = httpd.BasePath + configPathAnchored

	// The amount of time an update is allowed take, when sending and receiving.
	updateTimeout = 5 * time.Second
)

type Diagnostic interface {
	Error(msg string, err error)
}

type ConfigUpdate struct {
	Name      string
	NewConfig []interface{}
	ErrC      chan<- error
}

type Service struct {
	enabled bool
	config  interface{}
	diag    Diagnostic
	updates chan<- ConfigUpdate
	routes  []httpd.Route

	// Cached map of section name to element key name
	elementKeys map[string]string

	overrides OverrideDAO

	StorageService interface {
		Store(namespace string) storage.Interface
		Register(name string, store storage.StoreActioner)
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(c Config, config interface{}, d Diagnostic, updates chan<- ConfigUpdate) *Service {
	return &Service{
		enabled: c.Enabled,
		config:  config,
		diag:    d,
		updates: updates,
	}
}

const (
	// Public name of overrides store
	overridesAPIName = "overrides"
	// The storage namespace for all configuration override data.
	configNamespace = "config_overrides"
)

func (s *Service) Open() error {
	store := s.StorageService.Store(configNamespace)
	overrides, err := newOverrideKV(store)
	if err != nil {
		return err
	}
	s.overrides = overrides
	s.StorageService.Register(overridesAPIName, s.overrides)

	// Cache element keys
	if elementKeys, err := override.ElementKeys(s.config); err != nil {
		return errors.Wrap(err, "failed to determine the element keys")
	} else {
		s.elementKeys = elementKeys
	}

	// Define API routes
	s.routes = []httpd.Route{
		{
			Method:      "GET",
			Pattern:     configPath,
			HandlerFunc: s.handleGetConfig,
		},
		{
			Method:      "GET",
			Pattern:     configPathAnchored,
			HandlerFunc: s.handleGetConfig,
		},
		{
			Method:      "POST",
			Pattern:     configPathAnchored,
			HandlerFunc: s.handleUpdateSection,
		},
	}

	err = s.HTTPDService.AddRoutes(s.routes)
	return errors.Wrap(err, "failed to add API routes")
}

func (s *Service) Close() error {
	close(s.updates)
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

type updateAction struct {
	section    string
	element    string
	hasElement bool

	Set    map[string]interface{} `json:"set"`
	Delete []string               `json:"delete"`
	Add    map[string]interface{} `json:"add"`
	Remove []string               `json:"remove"`
}

func (ua updateAction) Validate() error {
	if ua.section == "" {
		return errors.New("must provide section name")
	}
	if !validSectionOrElement.MatchString(ua.section) {
		return fmt.Errorf("invalid section name %q", ua.section)
	}
	if ua.element != "" && !validSectionOrElement.MatchString(ua.element) {
		return fmt.Errorf("invalid element name %q", ua.element)
	}

	sEmpty := len(ua.Set) == 0
	dEmpty := len(ua.Delete) == 0
	aEmpty := len(ua.Add) == 0
	rEmpty := len(ua.Remove) == 0

	if (!sEmpty || !dEmpty) && !(aEmpty && rEmpty) {
		return errors.New("cannot provide both set/delete and add/remove actions in the same update")
	}

	if !aEmpty && ua.element != "" {
		return errors.New("must not provide an element name when adding an a new override")
	}

	if !rEmpty && ua.element != "" {
		return errors.New("must not provide element when removing an override")
	}

	if rEmpty && aEmpty && !ua.hasElement {
		return errors.New("element not specified, are you missing a trailing '/'?")
	}
	return nil
}

var validSectionOrElement = regexp.MustCompile(`^[-\w+]+$`)

func sectionAndElementToID(section, element string) string {
	id := path.Join(section, element)
	if element == "" {
		id += "/"
	}
	return id
}

func sectionAndElementFromPath(p, basePath string) (section, element string, hasSection, hasElement bool) {
	if !strings.HasPrefix(p, basePath) {
		return "", "", false, false
	}
	s, e := sectionAndElementFromID(strings.TrimPrefix(p, basePath))
	return s, e, true, (e != "" || s != "" && strings.HasSuffix(p, "/"))
}

func sectionAndElementFromID(id string) (section, element string) {
	parts := strings.Split(id, "/")
	if l := len(parts); l == 1 {
		section = parts[0]
	} else if l == 2 {
		section = parts[0]
		element = parts[1]
	}
	return
}

func (s *Service) sectionLink(section string) client.Link {
	return client.Link{Relation: client.Self, Href: path.Join(configBasePath, section)}
}

func (s *Service) elementLink(section, element string) client.Link {
	href := path.Join(configBasePath, section, element)
	if element == "" {
		href += "/"
	}
	return client.Link{Relation: client.Self, Href: href}
}

var configLink = client.Link{Relation: client.Self, Href: path.Join(httpd.BasePath, configPath)}

func (s *Service) handleUpdateSection(w http.ResponseWriter, r *http.Request) {
	if !s.enabled {
		httpd.HttpError(w, "config override service is not enabled", true, http.StatusForbidden)
		return
	}
	section, element, hasSection, hasElement := sectionAndElementFromPath(r.URL.Path, configBasePath)
	if !hasSection {
		httpd.HttpError(w, "no section specified", true, http.StatusBadRequest)
		return

	}
	ua := updateAction{
		section:    section,
		element:    element,
		hasElement: hasElement,
	}
	err := json.NewDecoder(r.Body).Decode(&ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to decode JSON:", err), true, http.StatusBadRequest)
		return
	}

	// Apply sets/deletes to stored overrides
	overrides, saveFunc, err := s.overridesForUpdateAction(ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to apply update: ", err), true, http.StatusBadRequest)
		return
	}

	// Apply overrides to config object
	os := convertOverrides(overrides)
	newConfig, err := override.OverrideConfig(s.config, os)
	if err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusBadRequest)
		return
	}

	// collect element values
	sectionList := make([]interface{}, len(newConfig[section]))
	for i, s := range newConfig[section] {
		sectionList[i] = s.Value()
	}

	// Construct ConfigUpdate
	errC := make(chan error, 1)
	cu := ConfigUpdate{
		Name:      section,
		NewConfig: sectionList,
		ErrC:      errC,
	}

	// Send update
	sendTimer := time.NewTimer(updateTimeout)
	defer sendTimer.Stop()
	select {
	case <-sendTimer.C:
		httpd.HttpError(w, fmt.Sprintf("failed to send configuration update %s/%s: timeout", section, element), true, http.StatusInternalServerError)
		return
	case s.updates <- cu:
	}

	// Wait for error
	recvTimer := time.NewTimer(updateTimeout)
	defer recvTimer.Stop()
	select {
	case <-recvTimer.C:
		httpd.HttpError(w, fmt.Sprintf("failed to update configuration %s/%s: timeout", section, element), true, http.StatusInternalServerError)
		return
	case err := <-errC:
		if err != nil {
			httpd.HttpError(w, fmt.Sprintf("failed to update configuration %s/%s: %v", section, element, err), true, http.StatusInternalServerError)
			return
		}
	}

	// Save the result of the update
	if err := saveFunc(); err != nil {
		httpd.HttpError(w, err.Error(), true, http.StatusInternalServerError)
		return
	}

	// Success
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	if !s.enabled {
		httpd.HttpError(w, "config override service is not enabled", true, http.StatusForbidden)
		return
	}
	section, element, hasSection, hasElement := sectionAndElementFromPath(r.URL.Path, configBasePath)
	config, err := s.getConfig(section)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to resolve current config:", err), true, http.StatusInternalServerError)
		return
	}
	if hasSection && section == "" {
		httpd.HttpError(w, "section not specified, do you have an extra trailing '/'?", true, http.StatusBadRequest)
		return
	}
	if !hasSection {
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(config); err != nil {
			s.diag.Error("failed to JSON encode configuration", err)
		}
	} else if section != "" {
		sec, ok := config.Sections[section]
		if !ok {
			httpd.HttpError(w, fmt.Sprint("unknown section: ", section), true, http.StatusNotFound)
			return
		}
		if hasElement {
			var elementEntry client.ConfigElement
			// Find specified element
			elementKey := s.elementKeys[section]
			found := false
			for _, e := range sec.Elements {
				if (element == "" && elementKey == "") || e.Options[elementKey] == element {
					elementEntry = e
					found = true
					break
				}
			}
			if found {
				w.WriteHeader(http.StatusOK)
				if err := json.NewEncoder(w).Encode(elementEntry); err != nil {
					s.diag.Error("failed to JSON encode element", err)
				}
			} else {
				httpd.HttpError(w, fmt.Sprintf("unknown section/element: %s/%s", section, element), true, http.StatusNotFound)
				return
			}
		} else {
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(sec); err != nil {
				s.diag.Error("failed to JSON encode sec", err)
			}
		}
	}
}

// overridesForUpdateAction produces a list of overrides relevant to the update action and
// returns  save function. Call the save function to permanently store the result of the update.
func (s *Service) overridesForUpdateAction(ua updateAction) ([]Override, func() error, error) {
	if err := ua.Validate(); err != nil {
		return nil, nil, errors.Wrap(err, "invalid update action")
	}
	section := ua.section
	element := ua.element
	if len(ua.Remove) == 0 {
		// If we are adding find element value based on the element key
		if len(ua.Add) > 0 {
			key, ok := s.elementKeys[section]
			if !ok {
				return nil, nil, fmt.Errorf("unknown section %q", section)
			}
			if key == "" {
				return nil, nil, fmt.Errorf("section %q is not a list, cannot add new element", section)
			}
			elementValue, ok := ua.Add[key]
			if !ok {
				return nil, nil, fmt.Errorf("missing key %q in \"add\" map", key)
			}
			if str, ok := elementValue.(string); !ok {
				return nil, nil, fmt.Errorf("expected %q key to be a string, got %T", key, elementValue)
			} else {
				element = str
			}
		}

		id := sectionAndElementToID(section, element)

		// Apply changes to single override
		o, err := s.overrides.Get(id)
		if err == ErrNoOverrideExists {
			o = Override{
				ID:      id,
				Options: make(map[string]interface{}),
			}
		} else if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
		} else if err == nil && len(ua.Add) > 0 {
			return nil, nil, errors.Wrapf(err, "cannot add new override, override already exists for %s", id)
		}
		if len(ua.Add) > 0 {
			// Drop all previous options and only use the current set.
			o.Options = make(map[string]interface{}, len(ua.Add))
			o.Create = true
			for k, v := range ua.Add {
				o.Options[k] = v
			}
		} else {
			for k, v := range ua.Set {
				o.Options[k] = v
			}
			for _, k := range ua.Delete {
				delete(o.Options, k)
			}
		}
		saveFunc := func() error {
			if err := s.overrides.Set(o); err != nil {
				return errors.Wrapf(err, "failed to save override %s", o.ID)
			}
			return nil
		}

		// Get all overrides for the section
		overrides, err := s.overrides.List(section)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to get existing overrides for section %s", ua.section)
		}

		// replace modified override
		found := false
		for i := range overrides {
			if overrides[i].ID == id {
				overrides[i] = o
				found = true
				break
			}
		}
		if !found {
			overrides = append(overrides, o)
		}
		return overrides, saveFunc, nil
	} else {
		// Remove the list of overrides
		removed := make([]string, len(ua.Remove))
		removeLookup := make(map[string]bool, len(ua.Remove))
		for i, r := range ua.Remove {
			id := sectionAndElementToID(section, r)
			removed[i] = id
			removeLookup[id] = true
		}
		// Get overrides for the section
		overrides, err := s.overrides.List(section)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to get existing overrides for section %s", ua.section)
		}

		// Filter overrides
		filtered := overrides[:0]
		for _, o := range overrides {
			if !removeLookup[o.ID] {
				filtered = append(filtered, o)
			}
		}
		saveFunc := func() error {
			for _, id := range removed {
				if err := s.overrides.Delete(id); err != nil {
					return errors.Wrapf(err, "failed to remove existing override %s", id)
				}
			}
			return nil
		}
		return filtered, saveFunc, nil
	}
}

func convertOverrides(overrides []Override) []override.Override {
	os := make([]override.Override, len(overrides))
	for i, o := range overrides {
		section, element := sectionAndElementFromID(o.ID)
		if o.Create {
			element = ""
		}
		os[i] = override.Override{
			Section: section,
			Element: element,
			Options: o.Options,
			Create:  o.Create,
		}
	}
	return os
}

// getConfig returns a map of a fully resolved configuration object.
func (s *Service) getConfig(section string) (client.ConfigSections, error) {
	overrides, err := s.overrides.List(section)
	if err != nil {
		return client.ConfigSections{}, errors.Wrap(err, "failed to retrieve config overrides")
	}
	os := convertOverrides(overrides)
	sections, err := override.OverrideConfig(s.config, os)
	if err != nil {
		return client.ConfigSections{}, errors.Wrap(err, "failed to apply configuration overrides")
	}
	config := client.ConfigSections{
		Link:     configLink,
		Sections: make(map[string]client.ConfigSection, len(sections)),
	}
	for name, elements := range sections {
		if !strings.HasPrefix(name, section) {
			// Skip sections we did not request
			continue
		}
		sec := config.Sections[name]
		sec.Link = s.sectionLink(name)
		for _, element := range elements {
			redacted, list, err := element.Redacted()
			if err != nil {
				return client.ConfigSections{}, errors.Wrap(err, "failed to get redacted configuration data")
			}
			sec.Elements = append(sec.Elements, client.ConfigElement{
				Link:     s.elementLink(name, element.ElementID()),
				Options:  redacted,
				Redacted: list,
			})
		}
		config.Sections[name] = sec
	}
	return config, nil
}

func (s *Service) Config() (map[string][]interface{}, error) {
	overrides, err := s.overrides.List("")
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve config overrides")
	}
	os := convertOverrides(overrides)
	sections, err := override.OverrideConfig(s.config, os)
	if err != nil {
		return nil, errors.Wrap(err, "failed to apply configuration overrides")
	}
	config := make(map[string][]interface{}, len(sections))
	for name, sectionList := range sections {
		for _, section := range sectionList {
			config[name] = append(config[name], section.Value())
		}
	}
	return config, nil
}
