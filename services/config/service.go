package config

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"regexp"
	"strings"

	"github.com/influxdata/kapacitor/services/config/override"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

const (
	configPath         = "/config"
	configPathAnchored = "/config/"
	basePath           = httpd.BasePath + configPathAnchored
)

type ConfigUpdate struct {
	Name      string
	NewConfig []interface{}
}

type Service struct {
	overrider *override.Overrider
	logger    *log.Logger
	updates   chan<- ConfigUpdate
	routes    []httpd.Route

	// Cached map of section name to element key name
	elementKeys map[string]string

	overrides OverrideDAO

	StorageService interface {
		Store(namespace string) storage.Interface
	}
	HTTPDService interface {
		AddRoutes([]httpd.Route) error
		DelRoutes([]httpd.Route)
	}
}

func NewService(config interface{}, l *log.Logger, updates chan<- ConfigUpdate) *Service {
	overrider := override.New(config)
	overrider.OptionNameFunc = override.TomlFieldName
	return &Service{
		overrider: overrider,
		logger:    l,
		updates:   updates,
	}
}

// The storage namespace for all configuration override data.
const configNamespace = "config_overrides"

func (s *Service) Open() error {
	store := s.StorageService.Store(configNamespace)
	s.overrides = newOverrideKV(store)

	// Cache element keys
	if elementKeys, err := s.overrider.ElementKeys(); err != nil {
		return errors.Wrap(err, "failed to determine the element keys")
	} else {
		s.elementKeys = elementKeys
	}
	log.Println(s.overrider)
	log.Println(s.elementKeys)

	// Define API routes
	s.routes = []httpd.Route{
		{
			Name:        "config",
			Method:      "GET",
			Pattern:     configPath,
			HandlerFunc: s.handleGetConfig,
		},
		{
			Name:        "config",
			Method:      "GET",
			Pattern:     configPathAnchored,
			HandlerFunc: s.handleGetConfig,
		},
		{
			Name:        "config",
			Method:      "POST",
			Pattern:     configPathAnchored,
			HandlerFunc: s.handleUpdateSection,
		},
	}

	err := s.HTTPDService.AddRoutes(s.routes)
	return errors.Wrap(err, "failed to add API routes")
}

func (s *Service) Close() error {
	close(s.updates)
	s.HTTPDService.DelRoutes(s.routes)
	return nil
}

type updateAction struct {
	section string
	element string

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

	return nil
}

var validSectionOrElement = regexp.MustCompile(`^[-\w+]+$`)

func sectionAndElementToID(section, element string) string {
	return path.Join(section, element)
}

func sectionAndElementFromPath(p string) (section, element string) {
	return sectionAndElementFromID(strings.TrimPrefix(p, basePath))
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

func (s *Service) handleUpdateSection(w http.ResponseWriter, r *http.Request) {
	section, element := sectionAndElementFromPath(r.URL.Path)
	ua := updateAction{
		section: section,
		element: element,
	}
	err := json.NewDecoder(r.Body).Decode(&ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to decode JSON:", err), true, http.StatusBadRequest)
		return
	}

	// Apply sets/deletes to stored overrides
	overrides, err := s.applyUpdateAction(ua)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to apply update:", err), true, http.StatusBadRequest)
		return
	}

	// Apply overrides to config object
	os := convertOverrides(overrides)
	newConfig, err := s.overrider.OverrideAll(os)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to update config:", err), true, http.StatusBadRequest)
		return
	}
	sectionList := make([]interface{}, len(newConfig[section]))
	for i, s := range newConfig[section] {
		sectionList[i] = s.Value()
	}
	cu := ConfigUpdate{
		Name:      section,
		NewConfig: sectionList,
	}
	s.updates <- cu
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	section, element := sectionAndElementFromPath(r.URL.Path)
	config, err := s.getConfig(section)
	if err != nil {
		httpd.HttpError(w, fmt.Sprint("failed to resolve current config:", err), true, http.StatusInternalServerError)
		return
	}
	if section == "" && element == "" {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(config)
	} else {
		sectionList, ok := config[section]
		if !ok {
			httpd.HttpError(w, fmt.Sprint("unknown section: ", section), true, http.StatusNotFound)
			return
		}
		if element != "" {
			var elementEntry map[string]interface{}
			// Find specified element
			elementKey := s.elementKeys[section]
			for _, options := range sectionList {
				if options[elementKey] == element {
					elementEntry = options
					break
				}
			}
			if len(elementEntry) > 0 {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(elementEntry)
			} else {
				httpd.HttpError(w, fmt.Sprintf("unknown section/element: %s/%s", section, element), true, http.StatusNotFound)
				return
			}
		} else {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(sectionList)
		}
	}
}

func (s *Service) applyUpdateAction(ua updateAction) ([]Override, error) {
	if err := ua.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid update action")
	}
	section := ua.section
	element := ua.element
	if len(ua.Remove) == 0 {
		// If we are adding find element value based on the element key
		if len(ua.Add) > 0 {
			key, ok := s.elementKeys[section]
			if !ok {
				return nil, fmt.Errorf("unknown section %q", section)
			}
			elementValue, ok := ua.Add[key]
			if !ok {
				return nil, fmt.Errorf("mising key %q in \"add\" map", key)
			}
			if str, ok := elementValue.(string); !ok {
				return nil, fmt.Errorf("expected %q key to be a string, got %T", key, elementValue)
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
			return nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
		} else if err == nil && len(ua.Add) > 0 {
			return nil, errors.Wrapf(err, "cannot add new override, override already exists for %s", id)
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

		if err := s.overrides.Set(o); err != nil {
			return nil, errors.Wrapf(err, "failed to retrieve existing overrides for %s", id)
		}
		return []Override{o}, nil
	} else {
		// Remove the list of overrides
		for _, r := range ua.Remove {
			id := sectionAndElementToID(section, r)
			if err := s.overrides.Delete(id); err != nil {
				return nil, errors.Wrapf(err, "failed to remove existing override %s", id)
			}
		}
		// Get remaining overrides for the section
		overrides, err := s.overrides.List(section)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get existing overrides for section %s", ua.section)
		}
		return overrides, nil
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
func (s *Service) getConfig(section string) (map[string][]map[string]interface{}, error) {
	overrides, err := s.overrides.List(section)
	if err != nil {
		return nil, errors.Wrap(err, "failed to retrieve config overrides")
	}
	os := convertOverrides(overrides)
	sections, err := s.overrider.OverrideAll(os)
	if err != nil {
		return nil, errors.Wrap(err, "failed to apply configuration overrides")
	}
	config := make(map[string][]map[string]interface{}, len(sections))
	for name, sectionList := range sections {
		if !strings.HasPrefix(name, section) {
			// Skip sections we did not request
			continue
		}
		for _, section := range sectionList {
			redacted, err := section.Redacted()
			if err != nil {
				return nil, errors.Wrap(err, "failed to get redacted configuration data")
			}
			config[name] = append(config[name], redacted)
		}
	}
	return config, nil
}
