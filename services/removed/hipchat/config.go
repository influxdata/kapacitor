package hipchat

import "errors"

var configerr = errors.New("HipChat has been removed, please update your configs")

type Config struct {
	// Whether HipChat integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The HipChat API URL.
	URL string `toml:"url" override:"url"`
	// The authentication token for this notification, can be overridden per alert.
	// https://www.hipchat.com/docs/apiv2/auth for info on obtaining a token.
	Token string `toml:"token" override:"token,redact"`
	// The default room, can be overridden per alert.
	Room string `toml:"room" override:"room"`
	// Whether all alerts should automatically post to HipChat
	Global bool `toml:"global" override:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only" override:"state-changes-only"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	return configerr
}
