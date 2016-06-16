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
	IssueType string `toml:"issue-type"`
	// Warning level priority
	PriorityWarn string `toml:"priority-warn"`
	// Critical level priority
	PriorityCrit string `toml:"priority-crit"`
	// Whether every alert should automatically go to JIRA
	Global bool `toml:"global"`
}

func NewConfig() Config {
	return Config{}
}
