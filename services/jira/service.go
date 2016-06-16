package jira

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	url           string
	username      string
	password      string
	project       string
	issue_type    string
	priority_warn string
	priority_crit string
	global        bool
	logger        *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:           c.URL,
		username:      c.Username,
		password:      c.Password,
		project:       c.Project,
		issue_type:    c.Issue_type,
		priority_warn: c.Priority_warn,
		priority_crit: c.Priority_crit,
		global:        c.Global,
		logger:        l,
	}
}

func (s *Service) Open() error {
	return nil
}

func (s *Service) Close() error {
	return nil
}

func (s *Service) Global() bool {
	return s.global
}

func (s *Service) Alert(project, issue_type, priority_warn, priority_crit, entityID, message string, level kapacitor.AlertLevel, details interface{}) error {
	var priority string
	switch level {
	case kapacitor.WarnAlert:
		priority = priority_warn
		if priority == "" {
			priority = s.priority_warn
		}
	case kapacitor.CritAlert:
		priority = priority_crit
		if priority == "" {
			priority = s.priority_crit
		}
	case kapacitor.InfoAlert:
		return fmt.Errorf("AlertLevel 'info' is currently ignored by the JIRA service")
	default:
		priority = s.priority_warn
	}
	if project == "" {
		project = s.project
	}
	if issue_type == "" {
		issue_type = s.issue_type
	}

	postData := make(map[string]interface{})
	postIssueFields := make(map[string]interface{})

	postIssueType := make(map[string]interface{})
	postIssueType["name"] = issue_type

	postProject := make(map[string]interface{})
	postProject["key"] = project

	postPriority := make(map[string]interface{})
	postPriority["name"] = priority

	postIssueFields["issuetype"] = postIssueType
	postIssueFields["project"] = postProject
	postIssueFields["priority"] = postPriority
	postIssueFields["summary"] = "[" + kapacitor.Product + "] - " + entityID
	postIssueFields["description"] = "Message: " + message
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return err
		}
		postIssueFields["description"] = "Message: " + message + "\nJSON:\n" + string(b)
	}
	postData["fields"] = postIssueFields

	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err := enc.Encode(postData)
	if err != nil {
		return err
	}

	var jira_url *url.URL
	jira_url, err = url.Parse(s.url + "/rest/api/2/issue")
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", jira_url.String(), &post)
	if err != nil {
		return err
	}
	req.SetBasicAuth(s.username, s.password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	_, err = client.Do(req)
	if err != nil {
		return err
	}

	return nil
}
