package kafka

const DefaultTopic = "Important"
const DefaultSasl = false

type Config struct {
	Topics  []string `toml:"topics"`
	Enabled bool     `toml:"enabled"`
	// Comma separated kafka brokers
	Brokers string `toml:"brokers"`
	// Whether all alerts should trigger an email.

	//SASL enabled
	SaslEnabled bool	`toml:"sasl_enabled"`
	SaslUser string		`toml:"sasl_user"`
	SaslPassword string	`toml:"sasl_password"`

	Global bool `toml:"global"`
	// Whether all alerts should automatically use stateChangesOnly mode.
	// Only applies if global is also set.
	StateChangesOnly bool `toml:"state-changes-only"`
}

func NewConfig() Config {
	return Config{
		Topics: []string{DefaultTopic},
		SaslEnabled: DefaultSasl,
	}
}
