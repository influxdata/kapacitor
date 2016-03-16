package hipchat

type Config struct {
	// Whether HipChat integration is enabled.
	Enabled bool `toml:"enabled"`
	// The HipChat API URL.
	URL string `toml:"url"`
	// The authentication token for this notification, can be overridden per alert.
	// https://www.hipchat.com/docs/apiv2/auth for info on obtaining a token.
	Token string `toml:"token"`
	// The default room, can be overridden per alert.
	Room string `toml:"room"`
	// Whether all alerts should automatically post to HipChat
	Global bool `toml:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only"`
}

func NewConfig() Config {
	return Config{}
}
