package server

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/services/alert"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"

	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/opentsdb"
)

// Config represents the configuration format for the kapacitord binary.
type Config struct {
	HTTP           httpd.Config      `toml:"http"`
	Replay         replay.Config     `toml:"replay"`
	Storage        storage.Config    `toml:"storage"`
	Task           task_store.Config `toml:"task"`
	InfluxDB       []influxdb.Config `toml:"influxdb" override:"influxdb,element-key=name"`
	Logging        logging.Config    `toml:"logging"`
	ConfigOverride config.Config     `toml:"config-override"`
	Alert          alert.Config      `toml:"alert"`

	// Input services
	Graphites []graphite.Config `toml:"graphite"`
	Collectd  collectd.Config   `toml:"collectd"`
	OpenTSDB  opentsdb.Config   `toml:"opentsdb"`
	UDPs      []udp.Config      `toml:"udp"`

	// Alert handlers
	Alerta    alerta.Config    `toml:"alerta" override:"alerta"`
	HipChat   hipchat.Config   `toml:"hipchat" override:"hipchat"`
	OpsGenie  opsgenie.Config  `toml:"opsgenie" override:"opsgenie"`
	PagerDuty pagerduty.Config `toml:"pagerduty" override:"pagerduty"`
	Pushover  pushover.Config  `toml:"pushover" override:"pushover"`
	SMTP      smtp.Config      `toml:"smtp" override:"smtp"`
	SNMPTrap  snmptrap.Config  `toml:"snmptrap" override:"snmptrap"`
	Sensu     sensu.Config     `toml:"sensu" override:"sensu"`
	Slack     slack.Config     `toml:"slack" override:"slack"`
	Talk      talk.Config      `toml:"talk" override:"talk"`
	Telegram  telegram.Config  `toml:"telegram" override:"telegram"`
	VictorOps victorops.Config `toml:"victorops" override:"victorops"`

	// Third-party integrations
	Kubernetes k8s.Config `toml:"kubernetes" override:"kubernetes"`

	Reporting reporting.Config `toml:"reporting"`
	Stats     stats.Config     `toml:"stats"`
	UDF       udf.Config       `toml:"udf"`
	Deadman   deadman.Config   `toml:"deadman"`

	Hostname               string `toml:"hostname"`
	DataDir                string `toml:"data_dir"`
	SkipConfigOverrides    bool   `toml:"skip-config-overrides"`
	DefaultRetentionPolicy string `toml:"default-retention-policy"`

	Commander command.Commander `toml:"-"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{
		Hostname:  "localhost",
		Commander: command.ExecCommander,
	}

	c.HTTP = httpd.NewConfig()
	c.Storage = storage.NewConfig()
	c.Replay = replay.NewConfig()
	c.Task = task_store.NewConfig()
	c.InfluxDB = []influxdb.Config{influxdb.NewConfig()}
	c.Logging = logging.NewConfig()
	c.Kubernetes = k8s.NewConfig()
	c.ConfigOverride = config.NewConfig()
	c.Alert = alert.NewConfig()

	c.Collectd = collectd.NewConfig()
	c.OpenTSDB = opentsdb.NewConfig()

	c.Alerta = alerta.NewConfig()
	c.HipChat = hipchat.NewConfig()
	c.OpsGenie = opsgenie.NewConfig()
	c.PagerDuty = pagerduty.NewConfig()
	c.Pushover = pushover.NewConfig()
	c.SMTP = smtp.NewConfig()
	c.Sensu = sensu.NewConfig()
	c.Slack = slack.NewConfig()
	c.Talk = talk.NewConfig()
	c.SNMPTrap = snmptrap.NewConfig()
	c.Telegram = telegram.NewConfig()
	c.VictorOps = victorops.NewConfig()

	c.Reporting = reporting.NewConfig()
	c.Stats = stats.NewConfig()
	c.UDF = udf.NewConfig()
	c.Deadman = deadman.NewConfig()

	return c
}

// NewDemoConfig returns the config that runs when no config is specified.
func NewDemoConfig() (*Config, error) {
	c := NewConfig()

	var homeDir string
	// By default, store meta and data files in current users home directory
	u, err := user.Current()
	if err == nil {
		homeDir = u.HomeDir
	} else if os.Getenv("HOME") != "" {
		homeDir = os.Getenv("HOME")
	} else {
		return nil, fmt.Errorf("failed to determine current user for storage")
	}

	c.Replay.Dir = filepath.Join(homeDir, ".kapacitor", c.Replay.Dir)
	c.Task.Dir = filepath.Join(homeDir, ".kapacitor", c.Task.Dir)
	c.Storage.BoltDBPath = filepath.Join(homeDir, ".kapacitor", c.Storage.BoltDBPath)
	c.DataDir = filepath.Join(homeDir, ".kapacitor", c.DataDir)

	return c, nil
}

// Validate returns an error if the config is invalid.
func (c *Config) Validate() error {
	if c.Hostname == "" {
		return fmt.Errorf("must configure valid hostname")
	}
	if c.DataDir == "" {
		return fmt.Errorf("must configure valid data dir")
	}
	if err := c.Replay.Validate(); err != nil {
		return err
	}
	if err := c.Storage.Validate(); err != nil {
		return err
	}
	if err := c.HTTP.Validate(); err != nil {
		return err
	}
	if err := c.Task.Validate(); err != nil {
		return err
	}
	// Validate the set of InfluxDB configs.
	// All names should be unique.
	names := make(map[string]bool, len(c.InfluxDB))
	// Should be exactly one default if at least one configs is enabled.
	defaultInfluxDB := -1
	numEnabled := 0
	for i := range c.InfluxDB {
		c.InfluxDB[i].ApplyConditionalDefaults()
		config := c.InfluxDB[i]
		if names[config.Name] {
			return fmt.Errorf("duplicate name %q for influxdb configs", config.Name)
		}
		names[config.Name] = true
		if err := config.Validate(); err != nil {
			return err
		}
		if config.Enabled && config.Default {
			if defaultInfluxDB != -1 {
				return fmt.Errorf("More than one InfluxDB default was specified: %s %s", config.Name, c.InfluxDB[defaultInfluxDB].Name)
			}
			defaultInfluxDB = i
		}
		if config.Enabled {
			numEnabled++
		}
	}
	if numEnabled > 1 && defaultInfluxDB == -1 {
		return errors.New("at least one of the enabled InfluxDB clusters must be marked as default.")
	}

	// Validate inputs
	for _, g := range c.Graphites {
		if err := g.Validate(); err != nil {
			return fmt.Errorf("invalid graphite config: %v", err)
		}
	}

	// Validate alert handlers
	if err := c.Alerta.Validate(); err != nil {
		return err
	}
	if err := c.HipChat.Validate(); err != nil {
		return err
	}
	if err := c.OpsGenie.Validate(); err != nil {
		return err
	}
	if err := c.PagerDuty.Validate(); err != nil {
		return err
	}
	if err := c.Pushover.Validate(); err != nil {
		return err
	}
	if err := c.SMTP.Validate(); err != nil {
		return err
	}
	if err := c.SNMPTrap.Validate(); err != nil {
		return err
	}
	if err := c.Sensu.Validate(); err != nil {
		return err
	}
	if err := c.Slack.Validate(); err != nil {
		return err
	}
	if err := c.Talk.Validate(); err != nil {
		return err
	}
	if err := c.Telegram.Validate(); err != nil {
		return err
	}
	if err := c.VictorOps.Validate(); err != nil {
		return err
	}

	if err := c.UDF.Validate(); err != nil {
		return err
	}
	return nil
}

func (c *Config) ApplyEnvOverrides() error {
	return c.applyEnvOverrides("KAPACITOR", "", reflect.ValueOf(c))
}

func (c *Config) applyEnvOverrides(prefix string, fieldDesc string, spec reflect.Value) error {
	// If we have a pointer, dereference it
	s := spec
	if spec.Kind() == reflect.Ptr {
		s = spec.Elem()
	}

	var value string

	if s.Kind() != reflect.Struct {
		value = os.Getenv(prefix)
		// Skip any fields we don't have a value to set
		if value == "" {
			return nil
		}

		if fieldDesc != "" {
			fieldDesc = " to " + fieldDesc
		}
	}

	switch s.Kind() {
	case reflect.String:
		s.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

		var intValue int64

		// Handle toml.Duration
		if s.Type().Name() == "Duration" {
			dur, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)
			}
			intValue = dur.Nanoseconds()
		} else {
			var err error
			intValue, err = strconv.ParseInt(value, 0, s.Type().Bits())
			if err != nil {
				return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)
			}
		}

		s.SetInt(intValue)
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)

		}
		s.SetBool(boolValue)
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, s.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)

		}
		s.SetFloat(floatValue)
	case reflect.Struct:
		if err := c.applyEnvOverridesToStruct(prefix, s); err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) applyEnvOverridesToStruct(prefix string, s reflect.Value) error {
	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		// Get the toml tag to determine what env var name to use
		configName := typeOfSpec.Field(i).Tag.Get("toml")
		// Replace hyphens with underscores to avoid issues with shells
		configName = strings.Replace(configName, "-", "_", -1)
		fieldName := typeOfSpec.Field(i).Name

		// Skip any fields that we cannot set
		if f.CanSet() || f.Kind() == reflect.Slice {

			// Use the upper-case prefix and toml name for the env var
			key := strings.ToUpper(configName)
			if prefix != "" {
				key = strings.ToUpper(fmt.Sprintf("%s_%s", prefix, configName))
			}

			// If the type is s slice, apply to each using the index as a suffix
			// e.g. GRAPHITE_0
			if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
				for i := 0; i < f.Len(); i++ {
					if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), fieldName, f.Index(i)); err != nil {
						return err
					}
				}
			} else if err := c.applyEnvOverrides(key, fieldName, f); err != nil {
				return err
			}
		}
	}
	return nil
}
