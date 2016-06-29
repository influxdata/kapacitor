package telegram

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Service struct {
	chatId                string
	parseMode             string
	disableWebPagePreview bool
	disableNotification   bool
	url                   string
	global                bool
	stateChangesOnly      bool
	logger                *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	return &Service{
		chatId:                c.ChatId,
		parseMode:             c.ParseMode,
		disableWebPagePreview: c.DisableWebPagePreview,
		disableNotification:   c.DisableNotification,
		url:                   c.URL + c.Token + "/sendMessage",
		global:                c.Global,
		stateChangesOnly:      c.StateChangesOnly,
		logger:                l,
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
func (s *Service) StateChangesOnly() bool {
	return s.stateChangesOnly
}

func (s *Service) Alert(chatId, parseMode, message string, disableWebPagePreview, disableNotification bool) error {
	if chatId == "" {
		chatId = s.chatId
	}

	if parseMode == "" {
		parseMode = s.parseMode
	}

	if parseMode != "" && parseMode != "Markdown" && parseMode != "HTML" {
		return fmt.Errorf("parseMode %s is not valid, please use 'Markdown' or 'HTML'", parseMode)
	}

	postData := make(map[string]interface{})
	postData["chat_id"] = chatId
	postData["text"] = message

	if parseMode != "" {
		postData["parse_mode"] = parseMode
	}

	if disableWebPagePreview || s.disableWebPagePreview {
		postData["disable_web_page_preview"] = true
	}

	if disableNotification || s.disableNotification {
		postData["disable_notification"] = true
	}

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
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		type response struct {
			Description string `json:"description"`
			ErrorCode   int    `json:"error_code"`
			Ok          bool   `json:"ok"`
		}
		res := &response{}

		err = json.Unmarshal(body, res)

		if err != nil {
			return fmt.Errorf("failed to understand Telegram response (err: %s). url: %s data: %v code: %d content: %s", err.Error(), s.url, &postData, resp.StatusCode, string(body))
		}
		return fmt.Errorf("sendMessage error (%d) description: %s", res.ErrorCode, res.Description)

	}
	return nil
}
