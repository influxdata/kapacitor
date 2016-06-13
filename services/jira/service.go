package jira

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/andygrunwald/go-jira"
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

func (s *Service) Alert(incidentKey, desc string, level kapacitor.AlertLevel, details interface{}) error {
	var priority string
	switch level {
	case kapacitor.WarnAlert:
		priority = s.priority_warn
	case kapacitor.CritAlert:
		priority = s.priority_crit
	case kapacitor.InfoAlert:
		return fmt.Errorf("AlertLevel 'info' is currently ignored by the JIRA service")
	default:
		priority = s.priority_warn
	}

	pData := make(map[string]string)
	pData["from"] = kapacitor.Product
	pData["incident_key"] = incidentKey
	pData["description"] = desc
	pData["issue_type"] = s.issue_type
	pData["project"] = s.project
	pData["priority"] = priority
	pData["details"] = ""
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			return err
		}
		pData["details"] = string(b)
	}

	issue := &jira.Issue{
		Fields: &jira.IssueFields{
			Type: jira.IssueType{
				Name: pData["issue_type"],
			},
			Project: jira.Project{
				Key: pData["project"],
			},
			Priority: &jira.Priority{
				Name: pData["priority"],
			},
			Summary:     "[" + pData["from"] + "] - " + pData["incident_key"],
			Description: pData["details"],
		},
	}

	// Post data to JIRA
	jiraClient, err := jira.NewClient(nil, s.url)
	if err != nil {
		return err
	}

	res, err := jiraClient.Authentication.AcquireSessionCookie(s.username, s.password)
	if err != nil || res == false {
		return err
	}

	res_issue, _, err := jiraClient.Issue.Create(issue)
	if err != nil || res_issue == nil {
		return err
	}

	return nil
}
