package slack

type Config struct {
	// Whether Slack integration is enabled.
	Enabled bool `toml:"enabled"`
	// The Slack webhook URL, can be obtained by adding Incoming Webhook integration.
	URL string `toml:"url"`
	// The default channel, can be overriden per alert.
	Channel string `toml:"channel"`
	// Whether all alerts should automatically post to slack
	Global bool `toml:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only"`
}

func NewConfig() Config {
	return Config{}
}
