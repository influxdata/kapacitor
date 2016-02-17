package opsgenie

const DefaultOpsGenieAPIURL = "https://api.opsgenie.com/v1/json/alert"
const DefaultOpsGenieRecoveryURL = "https://api.opsgenie.com/v1/json/alert/note"

type Config struct {
	// Whether to enable OpsGenie integration.
	Enabled bool `toml:"enabled"`
	// The OpsGenie API key.
	APIKey string `toml:"api-key"`
	// The default Teams, can be overriden per alert.
	Teams []string `toml:"teams"`
	// The default Teams, can be overriden per alert.
	Recipients []string `toml:"recipients"`
	// The OpsGenie API URL, should not need to be changed.
	URL string `toml:"url"`
	// The OpsGenie Recovery URL, you can change this based on which behavior you want a recovery to trigger (Add Notes, Close Alert, etc.)
	RecoveryURL string `toml:"recovery_url"`
	// Whether every alert should automatically go to OpsGenie.
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{
		URL:         DefaultOpsGenieAPIURL,
		RecoveryURL: DefaultOpsGenieRecoveryURL,
	}
}
