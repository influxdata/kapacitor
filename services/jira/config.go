package jira

type Config struct {
	// Whether JIRA integration is enabled.
	Enabled bool `toml:"enabled"`
	// The JIRA API URL.
	URL string `toml:"url"`
	// Username
	Username string `toml:"username"`
	// Password
	Password string `toml:"password"`
	// JIRA project
	Project string `toml:"project"`
	// Issue type
	Issue_type string `toml:"issue_type"`
	// Warning level priority
	Priority_warn string `toml:"priority_warn"`
	// Critical level priority
	Priority_crit string `toml:"priority_crit"`
	// Whether every alert should automatically go to JIRA
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{}
}
