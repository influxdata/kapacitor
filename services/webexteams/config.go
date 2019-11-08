package webexteams

import (
	"errors"
	"net/url"
)

const (
	// DefaultURL is the url for sending messages to the Webex Teams platform
	DefaultURL = "https://api.ciscospark.com/v1/messages"
)

// Config contains basic configuration for enabling the Webex Teams Alert Plugin
type Config struct {
	// Whether Telegram integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`

	// The Telegram Bot URL, should not need to be changed.
	URL string `toml:"url" override:"url"`
	// The Webex Teams Access Token
	Token string `toml:"token" override:"token,redact"`

	// The default Room, can be overridden per alert.
	RoomID string `toml:"room-id" override:"room-id"`

	ToPersonID    string `toml:"to-person-id" override:"to-person-id"`
	ToPersonEmail string `toml:"to-person-email" override:"to-person-email"`

	// Whether all alerts should automatically post to Webex Teams
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

// NewConfig returns Config with the default Webex Teams URL
func NewConfig() Config {
	return Config{
		URL: DefaultURL,
	}
}

// Validate ensures that the bare minimun configuration is supplied
func (c Config) Validate() error {
	if c.Enabled {
		if c.Token == "" {
			return errors.New(InvalidTokenErr)
		}
		if c.RoomID == "" && c.ToPersonID == "" && c.ToPersonEmail == "" {
			return errors.New(MissingDestinationErr)
		}
		if c.URL == "" {
			return errors.New(InvalidURLErr)
		}
		if _, err := url.Parse(c.URL); err != nil {
			return errors.New(InvalidURLErr)
		}
	}
	return nil
}
