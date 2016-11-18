package talk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/services/alert"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
	}
	return nil
}

type testOptions struct {
	Title string `json:"title"`
	Text  string `json:"text"`
}

func (s *Service) TestOptions() interface{} {
	return &testOptions{
		Title: "testTitle",
		Text:  "test talk text",
	}
}

func (s *Service) Test(options interface{}) error {
	o, ok := options.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(nil, o.Title, o.Text)
}

func (s *Service) Alert(ctxt context.Context, title, text string) error {
	url, post, err := s.preparePost(title, text)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", url, post)
	req.Header.Set("Content-Type", "application/json")
	if ctxt != nil {
		req = req.WithContext(ctxt)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: fmt.Sprintf("failed to understand Talk response. code: %d content: %s", resp.StatusCode, string(body))}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}

func (s *Service) preparePost(title, text string) (string, io.Reader, error) {
	c := s.config()

	if !c.Enabled {
		return "", nil, errors.New("service is not enabled")
	}
	postData := make(map[string]interface{})
	postData["title"] = title
	postData["text"] = text
	postData["authorName"] = c.AuthorName

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return "", nil, err
	}

	return c.URL, &post, nil
}

func (s *Service) Handler() alert.Handler {
	return s
}

func (s *Service) Name() string {
	return "Talk"
}

func (s *Service) Handle(ctxt context.Context, event alert.Event) error {
	return s.Alert(
		ctxt,
		event.State.ID,
		event.State.Message,
	)
}
