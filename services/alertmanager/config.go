package alertmanager

import "errors"

// Config declares the needed configuration options for the service alertmanager.
type Config struct {
	// Enabled indicates whether the service should be enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the alertmanager server.
	URL string `toml:"url" override:"url"`
	// tag name for alert in alertmanager
	AlertManagerTagName []string `toml:"alertManagerTagName" override:"alertManagerTagName"`
	// tag value of alertmanager
	AlertManagerTagValue []string `toml:"alertManagerTagValue" override:"alertManagerTagValue"`
	// annotation name for alert in alertmanager
	AlertManagerAnnotationName []string `toml:"alertManagerAnnotationName" override:"alertManagerAnnotationName"`
	// annotation value for alert in alertmanager
	AlertManagerAnnotationValue []string `toml:"alertManagerAnnotationName" override:"alertManagerAnnotationName"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("Must specify the alertmanager server URL")
	}
	if c.Enabled{
		if len(c.AlertManagerTagName) != len(c.AlertManagerTagValue){
			return errors.New("Length of tag name must equal with tag value")
		}
		if len(c.AlertManagerAnnotationName) != len(c.AlertManagerAnnotationValue){
			return errors.New("Length of annotaion name must equal with annotaion value")
		}
	}

	return nil
}
