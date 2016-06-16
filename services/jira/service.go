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
	url          string
	username     string
	password     string
	project      string
	issueType    string
	priorityWarn string
	priorityCrit string
	global       bool
	logger       *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:          c.URL,
		username:     c.Username,
		password:     c.Password,
		project:      c.Project,
		issueType:    c.IssueType,
		priorityWarn: c.PriorityWarn,
		priorityCrit: c.PriorityCrit,
		global:       c.Global,
		logger:       l,
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

func (s *Service) Alert(project, issueType, priorityWarn, priorityCrit, entityID, message string, level kapacitor.AlertLevel, details interface{}) error {
	var priority string
	switch level {
	case kapacitor.WarnAlert:
		priority = priorityWarn
		if priority == "" {
			priority = s.priorityWarn
		}
	case kapacitor.CritAlert:
		priority = priorityCrit
		if priority == "" {
			priority = s.priorityCrit
		}
	case kapacitor.InfoAlert:
		return fmt.Errorf("AlertLevel 'info' is currently ignored by the JIRA service")
	default:
		priority = s.priorityWarn
	}
	if project == "" {
		project = s.project
	}
	if issueType == "" {
		issueType = s.issueType
	}

	postData := make(map[string]interface{})
	postIssueFields := make(map[string]interface{})

	postIssueType := make(map[string]interface{})
	postIssueType["name"] = issueType

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
