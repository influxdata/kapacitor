package load

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ghodss/yaml"

	"github.com/influxdata/kapacitor/client/v1"
	kexpvar "github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/server/vars"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/pkg/errors"
)

var defaultURL = "http://localhost:9092"

const (
	statErrorCount = "errors"
)

const (
	// Public name of overrides store
	loadAPIName = "load"
	// The storage namespace for all configuration override data.
	loadNamespace = "load_items"
)

type Diagnostic interface {
	Debug(msg string)
	Error(msg string, err error)
	Loading(thing string, file string)
}

type Service struct {
	mu     sync.Mutex
	config Config

	cli        *client.Client
	statsKey   string
	statMap    *kexpvar.Map
	errorCount *kexpvar.Int

	items ItemsDAO

	tasks     map[string]bool
	templates map[string]bool
	handlers  map[string]bool

	StorageService interface {
		Store(namespace string) storage.Interface
		Register(name string, store storage.StoreActioner)
	}

	diag Diagnostic
}

func NewService(c Config, h http.Handler, d Diagnostic) (*Service, error) {
	cfg := client.Config{
		URL:       defaultURL,
		UserAgent: "internal-load-service",
	}
	if h != nil {
		cfg.Transport = client.NewLocalTransport(h)
	}
	cli, err := client.New(cfg)

	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}

	s := &Service{
		config:    c,
		diag:      d,
		cli:       cli,
		tasks:     map[string]bool{},
		templates: map[string]bool{},
		handlers:  map[string]bool{},
	}

	s.statsKey, s.statMap = vars.NewStatistic("load", nil)
	s.errorCount = &kexpvar.Int{}
	s.statMap.Set(statErrorCount, s.errorCount)

	return s, nil
}

func (s *Service) Open() error {
	if s.StorageService == nil {
		return errors.New("StorageService cannot be nil")
	}
	store := s.StorageService.Store(loadNamespace)
	items, err := newItemKV(store)
	if err != nil {
		return err
	}
	s.items = items
	s.StorageService.Register(loadAPIName, s.items)
	return nil
}

func (s *Service) Close() error {
	return nil
}

// taskFiles gets a slice of all files with the .tick file extension
// and any associated files with .json, .yml, and .yaml file extentions
// in the configured task directory.
func (s *Service) taskFiles() (tickscripts []string, taskFiles []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasksDir := s.config.tasksDir()

	files, err := ioutil.ReadDir(tasksDir)
	if err != nil {
		return nil, nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		switch ext := filepath.Ext(filename); ext {
		case ".tick":
			tickscripts = append(tickscripts, filepath.Join(tasksDir, filename))
		case ".yml", ".json", ".yaml":
			taskFiles = append(taskFiles, filepath.Join(tasksDir, filename))
		default:
			continue
		}
	}

	return
}

// templateFiles gets a slice of all files with the .tick file extension
// in the configured template directory.
func (s *Service) templateFiles() (tickscripts []string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	templatesDir := s.config.templatesDir()

	files, err := ioutil.ReadDir(templatesDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		switch ext := filepath.Ext(filename); ext {
		case ".tick":
			tickscripts = append(tickscripts, filepath.Join(templatesDir, filename))
		default:
			continue
		}
	}

	return
}

// HandlerFiles gets a slice of all files with the .json, .yml, and
// .yaml file extentions in the configured handler directory.
func (s *Service) handlerFiles() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	handlers := []string{}

	handlersDir := s.config.handlersDir()

	files, err := ioutil.ReadDir(handlersDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filename := file.Name()
		switch ext := filepath.Ext(filename); ext {
		case ".yml", ".json", ".yaml":
			handlers = append(handlers, filepath.Join(handlersDir, filename))
		default:
			continue
		}
	}

	return handlers, nil
}

func (s *Service) Load() error {
	s.mu.Lock()
	s.tasks = map[string]bool{}
	s.templates = map[string]bool{}
	s.handlers = map[string]bool{}
	s.mu.Unlock()

	if err := s.load(); err != nil {
		s.diag.Error("failed to load new files", err)
		s.errorCount.Add(1)
		return err
	}

	if err := s.removeMissing(); err != nil {
		s.diag.Error("failed to remove missing", err)
		s.errorCount.Add(1)
		return err
	}

	return nil
}

func (s *Service) load() error {
	if !s.config.Enabled {
		return nil
	}

	if _, err := ioutil.ReadDir(s.config.Dir); os.IsNotExist(err) {
		s.diag.Debug("skipping load... load directory does not exists")
		return nil
	}

	s.diag.Debug("loading templates")
	err := s.loadTemplates()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	s.diag.Debug("loading tasks")
	err = s.loadTasks()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to load tasks: %v", err)
	}

	s.diag.Debug("loading handlers")
	err = s.loadHandlers()
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (s *Service) loadTasks() error {
	ticks, templateTasks, err := s.taskFiles()
	if err != nil {
		return err
	}

	for _, f := range ticks {
		s.diag.Loading("task", f)
		if err := s.loadTask(f); err != nil {
			return fmt.Errorf("failed to load file %s: %s", f, err.Error())
		}
	}

	for _, v := range templateTasks {
		s.diag.Loading("template task", v)
		if err := s.loadVars(v); err != nil {
			return fmt.Errorf("failed to load file %s: %s", v, err.Error())
		}
	}

	return nil
}

func (s *Service) loadTask(f string) error {
	file, err := os.Open(f)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("failed to open file %v: %v", f, err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %v: %v", f, err)
	}

	script := string(data)
	fn := file.Name()
	id := strings.TrimSuffix(filepath.Base(fn), filepath.Ext(fn))

	l := s.cli.TaskLink(id)
	task, _ := s.cli.Task(l, nil)
	if task.ID == "" {
		o := client.CreateTaskOptions{
			ID:         id,
			TICKscript: script,
			Status:     client.Enabled,
		}

		if _, err := s.cli.CreateTask(o); err != nil {
			return fmt.Errorf("failed to create task: %v", err)
		}
	} else {
		o := client.UpdateTaskOptions{
			ID:         id,
			TICKscript: script,
		}
		if _, err := s.cli.UpdateTask(l, o); err != nil {
			return fmt.Errorf("failed to create task: %v", err)
		}

		// do reload
		_, err := s.cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Disabled})
		if err != nil {
			return err
		}
		_, err = s.cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Enabled})
		if err != nil {
			return err
		}

	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.items.Set(newTaskItem(id)); err != nil {
		return err
	}
	s.tasks[id] = true

	return nil
}

func (s *Service) loadTemplates() error {
	files, err := s.templateFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		s.diag.Loading("template", f)
		if err := s.loadTemplate(f); err != nil {
			return fmt.Errorf("failed to load file %s: %s", f, err.Error())
		}
	}
	return nil
}

func (s *Service) loadTemplate(f string) error {
	file, err := os.Open(f)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("failed to open file %v: %v", f, err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %v: %v", f, err)
	}

	script := string(data)
	fn := file.Name()
	id := strings.TrimSuffix(filepath.Base(fn), filepath.Ext(fn))

	l := s.cli.TemplateLink(id)
	task, _ := s.cli.Template(l, nil)
	if task.ID == "" {
		o := client.CreateTemplateOptions{
			ID:         id,
			TICKscript: script,
		}

		if _, err := s.cli.CreateTemplate(o); err != nil {
			return fmt.Errorf("failed to create template: %v", err)
		}
	} else {
		o := client.UpdateTemplateOptions{
			ID:         id,
			TICKscript: script,
		}
		if _, err := s.cli.UpdateTemplate(l, o); err != nil {
			return fmt.Errorf("failed to create template: %v", err)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.items.Set(newTemplateItem(id)); err != nil {
		return err
	}
	s.templates[id] = true

	return nil
}

func (s *Service) loadVars(f string) error {
	file, err := os.Open(f)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("failed to open file %v: %v", f, err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %v: %v", f, err)
	}

	fn := file.Name()
	id := strings.TrimSuffix(filepath.Base(fn), filepath.Ext(fn))

	fileVars := client.TaskVars{}
	switch ext := path.Ext(f); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &fileVars); err != nil {
			return errors.Wrapf(err, "failed to unmarshal yaml task vars file %q", f)
		}
	case ".json":
		if err := json.Unmarshal(data, &fileVars); err != nil {
			return errors.Wrapf(err, "failed to unmarshal json task vars file %q", f)
		}
	default:
		return errors.New("bad file extension. Must be YAML or JSON")
	}

	l := s.cli.TaskLink(id)
	task, _ := s.cli.Task(l, nil)
	if task.ID == "" {
		var o client.CreateTaskOptions
		o, err = fileVars.CreateTaskOptions()
		if err != nil {
			return fmt.Errorf("failed to initialize create task options: %v", err)
		}

		o.ID = id
		o.Status = client.Enabled
		if _, err := s.cli.CreateTask(o); err != nil {
			return fmt.Errorf("failed to create task: %v", err)
		}
	} else {
		var o client.UpdateTaskOptions
		o, err := fileVars.UpdateTaskOptions()
		if err != nil {
			return fmt.Errorf("failed to initialize create task options: %v", err)
		}

		o.ID = id
		if _, err := s.cli.UpdateTask(l, o); err != nil {
			return fmt.Errorf("failed to create task: %v", err)
		}
		// do reload
		_, err = s.cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Disabled})
		if err != nil {
			return err
		}
		_, err = s.cli.UpdateTask(l, client.UpdateTaskOptions{Status: client.Enabled})
		if err != nil {
			return err
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.items.Set(newTaskItem(id)); err != nil {
		return err
	}
	s.tasks[id] = true

	return nil
}

func (s *Service) loadHandlers() error {
	files, err := s.handlerFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		s.diag.Loading("handler", f)
		if err := s.loadHandler(f); err != nil {
			return fmt.Errorf("failed to load file %s: %s", f, err.Error())
		}
	}
	return nil
}

func (s *Service) loadHandler(f string) error {
	file, err := os.Open(f)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("failed to open file %v: %v", f, err)
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file %v: %v", f, err)
	}

	var o client.TopicHandlerOptions
	switch ext := path.Ext(f); ext {
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &o); err != nil {
			return errors.Wrapf(err, "failed to unmarshal yaml task vars file %q", f)
		}
	case ".json":
		if err := json.Unmarshal(data, &o); err != nil {
			return errors.Wrapf(err, "failed to unmarshal json task vars file %q", f)
		}
	default:
		return errors.New("bad file extension. Must be YAML or JSON")
	}

	l := s.cli.TopicHandlerLink(o.Topic, o.ID)
	handler, _ := s.cli.TopicHandler(l)
	if handler.ID == "" {
		_, err := s.cli.CreateTopicHandler(s.cli.TopicHandlersLink(o.Topic), o)
		if err != nil {
			return err
		}
	} else {
		_, err := s.cli.ReplaceTopicHandler(l, o)
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[path.Join(o.Topic, o.ID)] = true
	if err := s.items.Set(newTopicHandlerItem(o.Topic, o.ID)); err != nil {
		return err
	}

	return nil
}

func (s *Service) removeMissing() error {

	if err := s.removeTasks(); err != nil {
		return err
	}

	if err := s.removeTemplates(); err != nil {
		return err
	}

	if err := s.removeHandlers(); err != nil {
		return err
	}

	return nil
}

func (s *Service) removeTasks() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	loadedTasks, err := s.loadedTasks()
	if err != nil {
		return err
	}

	for _, id := range diff(s.tasks, loadedTasks) {
		l := s.cli.TaskLink(id)
		if err := s.cli.DeleteTask(l); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) removeTemplates() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	loadedTemplates, err := s.loadedTemplates()
	if err != nil {
		return err
	}
	for _, id := range diff(s.templates, loadedTemplates) {
		l := s.cli.TemplateLink(id)
		if err := s.cli.DeleteTemplate(l); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) removeHandlers() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	loadedHandlers, err := s.loadedHandlers()
	if err != nil {
		return err
	}
	for _, id := range diff(s.handlers, loadedHandlers) {
		pair := strings.Split(id, "/")
		if len(pair) != 2 {
			return errors.New("expected id to be topicID/handlerID")
		}
		l := s.cli.TopicHandlerLink(pair[0], pair[1])
		if err := s.cli.DeleteTopicHandler(l); err != nil {
			return err
		}
	}
	return nil
}

func diff(m map[string]bool, xs []string) []string {
	diffs := []string{}

	for _, x := range xs {
		if m[x] {
			continue
		}
		diffs = append(diffs, x)
	}

	return diffs
}

func (s *Service) loadedTasks() ([]string, error) {
	items, err := s.items.List(tasksStr)
	if err != nil {
		return nil, err
	}
	tasks := []string{}
	for _, item := range items {
		tasks = append(tasks, filepath.Base(item.ID))
	}

	return tasks, nil
}

func (s *Service) loadedTemplates() ([]string, error) {
	items, err := s.items.List(templatesStr)
	if err != nil {
		return nil, err
	}
	templates := []string{}
	for _, item := range items {
		templates = append(templates, filepath.Base(item.ID))
	}

	return templates, nil
}

func (s *Service) loadedHandlers() ([]string, error) {
	items, err := s.items.List(handlersStr)
	if err != nil {
		return nil, err
	}
	handlers := []string{}
	for _, item := range items {
		handlers = append(handlers, strings.TrimLeft(item.ID, "handlers/"))
	}

	return handlers, nil
}
