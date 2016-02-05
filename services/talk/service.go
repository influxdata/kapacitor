package talk

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

type Service struct {
	url        string
	authorName string
	logger     *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:        c.URL,
		authorName: c.AuthorName,
		logger:     l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Alert(title, text string) error {
	postData := make(map[string]interface{})
	postData["title"] = title
	postData["text"] = text
	postData["authorName"] = s.authorName

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return err
	}

	resp, err := http.Post(s.url, "application/json", &post)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		type response struct {
			Error string `json:"error"`
		}
		r := &response{Error: "failed to understand Talk response"}
		dec := json.NewDecoder(resp.Body)
		dec.Decode(r)
		return errors.New(r.Error)
	}
	return nil
}
