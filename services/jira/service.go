package jira

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/influxdata/kapacitor"
)

type Service struct {
	url              string
	username         string
	password         string
	project          string
	issueType        string
	issueFinalStatus string
	priorityWarn     string
	priorityCrit     string
	global           bool
	logger           *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		url:              c.URL,
		username:         c.Username,
		password:         c.Password,
		project:          c.Project,
		issueType:        c.IssueType,
		issueFinalStatus: c.IssueFinalStatus,
		priorityWarn:     c.PriorityWarn,
		priorityCrit:     c.PriorityCrit,
		global:           c.Global,
		logger:           l,
	}
}

type Response struct {
	Issues []struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	} `json:"issues"`
	Transitions []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"transitions"`
	Total int `json:"total"`
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

func (s *Service) RequestPost(method, apiUrl string, postData interface{}) (resp *http.Response, err error) {
	var post bytes.Buffer
	enc := json.NewEncoder(&post)
	err = enc.Encode(postData)
	if err != nil {
		return resp, err
	}

	var jira_url *url.URL
	jira_url, err = url.Parse(s.url + apiUrl)
	if err != nil {
		return resp, err
	}

	req, err := http.NewRequest(method, jira_url.String(), &post)
	if err != nil {
		return resp, err
	}
	req.SetBasicAuth(s.username, s.password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		return resp, err
	}
	return resp, err
}

func (s *Service) Alert(project, issueType, issueFinalStatus, priorityWarn, priorityCrit, entityID, message string, level kapacitor.AlertLevel, details interface{}) error {
	var priority string
	var response Response
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
		priority = "OK"
	}
	if project == "" {
		project = s.project
	}
	if issueType == "" {
		issueType = s.issueType
	}

	searchData := make(map[string]interface{})

	searchSummary := "\"[" + kapacitor.Product + "] - " + entityID + "\""
	escapeKeys := [...]string{"[", "]", " "}
	for _, v := range escapeKeys {
		searchSummary = strings.Replace(searchSummary, v, "\\\\"+v, -1)
	}
	searchData["jql"] = "project = " + project + " AND summary ~ " + searchSummary + " AND status != " + s.issueFinalStatus

	resp, err := s.RequestPost("POST", "/rest/api/2/search", searchData)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &response)
	if err != nil {
		return err
	}
	if response.Total > 0 {
		if level >= kapacitor.WarnAlert {
			for _, id := range response.Issues {
				issueUrl := "/rest/api/2/issue/" + id.ID + "/comment"
				postComment := make(map[string]interface{})
				postComment["body"] = "Update: " + message
				_, err := s.RequestPost("POST", issueUrl, postComment)
				if err != nil {
					return err
				}

				issueUrl = "/rest/api/2/issue/" + id.ID
				postUpdate := make(map[string]interface{})
				postPriority := make(map[string][]interface{})
				postSet := make(map[string]interface{})
				postName := make(map[string]interface{})
				postName["name"] = priority
				postSet["set"] = postName
				postPriority["priority"] = append(postPriority["priority"], postSet)
				postUpdate["update"] = postPriority
				_, err = s.RequestPost("PUT", issueUrl, postUpdate)
				if err != nil {
					return err
				}
			}
		} else {
			for _, id := range response.Issues {
				issueUrl := "/rest/api/2/issue/" + id.ID + "/transitions"
				resp, err := s.RequestPost("GET", issueUrl, nil)
				if err != nil {
					return err
				}
				body, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					return err
				}
				err = json.Unmarshal(body, &response)
				if err != nil {
					return err
				}
				var finalTransitionID string
				for _, id := range response.Transitions {
					if id.Name == s.issueFinalStatus {
						finalTransitionID = id.ID
					}
				}
				if finalTransitionID == "" {
					return fmt.Errorf("Unable to detect issue final status ID")
				}
				postClose := make(map[string]interface{})
				postTransition := make(map[string]interface{})
				postTransition["id"] = finalTransitionID
				postClose["transition"] = postTransition
				postUpdate := make(map[string][]interface{})
				postComment := make(map[string]interface{})
				postAdd := make(map[string]interface{})
				postAdd["body"] = "Update: " + message
				postComment["add"] = postAdd
				postUpdate["comment"] = append(postUpdate["comment"], postComment)
				postClose["update"] = postUpdate

				_, err = s.RequestPost("POST", issueUrl, postClose)
				if err != nil {
					return err
				}
			}
		}
	} else {
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

		_, err := s.RequestPost("POST", "/rest/api/2/issue", postData)
		if err != nil {
			return err
		}
	}

	return nil
}
