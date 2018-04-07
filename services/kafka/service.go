package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"text/template"

	"github.com/influxdata/kapacitor/alert"
	"github.com/influxdata/kapacitor/keyvalue"
	"github.com/pkg/errors"
	kafka "github.com/segmentio/kafka-go"
)

type Diagnostic interface {
	WithContext(ctx ...keyvalue.T) Diagnostic
	InsecureSkipVerify()
	Error(msg string, err error)
}

type Cluster struct {
	mu  sync.RWMutex
	cfg Config

	writers map[string]*kafka.Writer
}

func NewCluster(c Config) *Cluster {
	return &Cluster{
		cfg:     c,
		writers: make(map[string]*kafka.Writer),
	}
}

func (c *Cluster) WriteMessage(topic string, key, msg []byte) error {
	w, err := c.writer(topic)
	if err != nil {
		return err
	}
	return w.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: msg,
	})
}

func (c *Cluster) writer(topic string) (*kafka.Writer, error) {
	c.mu.RLock()
	w, ok := c.writers[topic]
	c.mu.RUnlock()
	if !ok {
		c.mu.Lock()
		defer c.mu.Unlock()
		w, ok = c.writers[topic]
		if !ok {
			wc, err := c.cfg.WriterConfig()
			if err != nil {
				return nil, err
			}
			wc.Topic = topic
			w = kafka.NewWriter(wc)
			c.writers[topic] = w
		}
	}
	return w, nil
}

func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, w := range c.writers {
		w.Close()
	}
	return
}

func (c *Cluster) Update(cfg Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if configChanged(c.cfg, cfg) {
		c.clearWriters()
	}
	c.cfg = cfg
	return nil
}

func configChanged(old, new Config) bool {
	if len(old.Brokers) != len(new.Brokers) {
		return true
	}
	sort.Strings(old.Brokers)
	sort.Strings(new.Brokers)
	for i, b := range old.Brokers {
		if new.Brokers[i] != b {
			return true
		}
	}
	return old.UseSSL != new.UseSSL ||
		old.SSLCA != new.SSLCA ||
		old.SSLCert != new.SSLCert ||
		old.SSLKey != new.SSLKey
}

func (c *Cluster) clearWriters() {
	for t, w := range c.writers {
		w.Close()
		delete(c.writers, t)
	}
}

type Service struct {
	mu       sync.RWMutex
	clusters map[string]*Cluster
	diag     Diagnostic
}

func NewService(cs Configs, d Diagnostic) *Service {
	clusters := make(map[string]*Cluster, len(cs))
	for _, c := range cs {
		if c.InsecureSkipVerify {
			d.InsecureSkipVerify()
		}
		clusters[c.ID] = NewCluster(c)
	}
	return &Service{
		diag:     d,
		clusters: clusters,
	}
}

func (s *Service) Cluster(id string) (*Cluster, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clusters[id]
	return c, ok
}
func (s *Service) Update(newConfigs []interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clusterExists := make(map[string]bool, len(s.clusters))

	for _, nc := range newConfigs {
		if c, ok := nc.(Config); ok {
			if err := c.Validate(); err != nil {
				return err
			}
			if c.Enabled {
				if c.InsecureSkipVerify {
					s.diag.InsecureSkipVerify()
				}
				cluster, ok := s.clusters[c.ID]
				if !ok {
					s.clusters[c.ID] = NewCluster(c)
				} else {
					if err := cluster.Update(c); err != nil {
						return errors.Wrapf(err, "failed to update cluster %q", c.ID)
					}
				}
				clusterExists[c.ID] = true
			} else {
				cluster, ok := s.clusters[c.ID]
				if ok {
					cluster.Close()
					delete(s.clusters, c.ID)
				}
			}
		} else {
			return fmt.Errorf("unexpected config object type, got %T exp %T", nc, c)
		}
	}

	// Find any deleted clusters
	for name, cluster := range s.clusters {
		if !clusterExists[name] {
			cluster.Close()
			delete(s.clusters, name)
		}
	}

	return nil
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

type testOptions struct {
	Cluster string `json:"cluster"`
	Topic   string `json:"topic"`
	Key     string `json:"key"`
	Message string `json:"message"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Cluster: "example",
		Topic:   "test",
		Key:     "key",
		Message: "test kafka message",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %t", options)
	}
	c, ok := s.Cluster(o.Cluster)
	if !ok {
		return fmt.Errorf("unknown cluster %q", o.Cluster)
	}
	return c.WriteMessage(o.Topic, []byte(o.Key), []byte(o.Message))
}

type HandlerConfig struct {
	Cluster  string `mapstructure:"cluster"`
	Topic    string `mapstructure:"topic"`
	Template string `mapstructure:"template"`
}

type handler struct {
	s *Service

	cluster  *Cluster
	topic    string
	template *template.Template

	diag Diagnostic
}

func (s *Service) Handler(c HandlerConfig, ctx ...keyvalue.T) (alert.Handler, error) {
	cluster, ok := s.Cluster(c.Cluster)
	if !ok {
		return nil, fmt.Errorf("unknown cluster %q", c.Cluster)
	}
	var t *template.Template
	if c.Template != "" {
		var err error
		t, err = template.New("kafka alert template").Parse(c.Template)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse template")
		}
	}
	return &handler{
		s:        s,
		cluster:  cluster,
		topic:    c.Topic,
		template: t,
		diag:     s.diag.WithContext(ctx...),
	}, nil
}

func (h *handler) Handle(event alert.Event) {
	body, err := h.prepareBody(event.AlertData())
	if err != nil {
		h.diag.Error("failed to prepare kafka message body", err)
	}
	if err := h.cluster.WriteMessage(h.topic, []byte(event.State.ID), body); err != nil {
		h.diag.Error("failed to write message to kafka", err)
	}
}
func (h *handler) prepareBody(ad alert.Data) ([]byte, error) {
	body := bytes.Buffer{}
	if h.template != nil {
		err := h.template.Execute(&body, ad)
		if err != nil {
			return nil, errors.Wrap(err, "failed to execute alert template")
		}
	} else {
		err := json.NewEncoder(&body).Encode(ad)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal alert data json")
		}
	}
	return body.Bytes(), nil
}
