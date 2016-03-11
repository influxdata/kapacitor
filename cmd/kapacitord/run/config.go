package run

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/logging"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"

	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/opentsdb"
)

// Config represents the configuration format for the kapacitord binary.
type Config struct {
	HTTP     httpd.Config      `toml:"http"`
	Replay   replay.Config     `toml:"replay"`
	Task     task_store.Config `toml:"task"`
	InfluxDB influxdb.Config   `toml:"influxdb"`
	Logging  logging.Config    `toml:"logging"`

	Graphites []graphite.Config `toml:"graphite"`
	Collectd  collectd.Config   `toml:"collectd"`
	OpenTSDB  opentsdb.Config   `toml:"opentsdb"`
	UDPs      []udp.Config      `toml:"udp"`
	SMTP      smtp.Config       `toml:"smtp"`
	OpsGenie  opsgenie.Config   `toml:"opsgenie"`
	VictorOps victorops.Config  `toml:"victorops"`
	PagerDuty pagerduty.Config  `toml:"pagerduty"`
	Sensu     sensu.Config      `toml:"sensu"`
	Slack     slack.Config      `toml:"slack"`
	HipChat   hipchat.Config    `toml:"hipchat"`
	Alerta    alerta.Config     `toml:"alerta"`
	Reporting reporting.Config  `toml:"reporting"`
	Stats     stats.Config      `toml:"stats"`
	UDF       udf.Config        `toml:"udf"`
	Deadman   deadman.Config    `toml:"deadman"`
	Talk      talk.Config       `toml:"talk"`

	Hostname string `toml:"hostname"`
	DataDir  string `toml:"data_dir"`
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{
		Hostname: "localhost",
	}

	c.HTTP = httpd.NewConfig()
	c.Replay = replay.NewConfig()
	c.Task = task_store.NewConfig()
	c.InfluxDB = influxdb.NewConfig()
	c.Logging = logging.NewConfig()

	c.Collectd = collectd.NewConfig()
	c.OpenTSDB = opentsdb.NewConfig()
	c.SMTP = smtp.NewConfig()
	c.OpsGenie = opsgenie.NewConfig()
	c.VictorOps = victorops.NewConfig()
	c.PagerDuty = pagerduty.NewConfig()
	c.Sensu = sensu.NewConfig()
	c.Slack = slack.NewConfig()
	c.HipChat = hipchat.NewConfig()
	c.Alerta = alerta.NewConfig()
	c.Reporting = reporting.NewConfig()
	c.Stats = stats.NewConfig()
	c.UDF = udf.NewConfig()
	c.Deadman = deadman.NewConfig()
	c.Talk = talk.NewConfig()

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
	err := c.Replay.Validate()
	if err != nil {
		return err
	}
	err = c.Task.Validate()
	if err != nil {
		return err
	}
	err = c.InfluxDB.Validate()
	if err != nil {
		return err
	}
	err = c.UDF.Validate()
	if err != nil {
		return err
	}
	err = c.Sensu.Validate()
	if err != nil {
		return err
	}
	err = c.Talk.Validate()
	if err != nil {
		return err
	}
	for _, g := range c.Graphites {
		if err := g.Validate(); err != nil {
			return fmt.Errorf("invalid graphite config: %v", err)
		}
	}
	return nil
}

func (c *Config) ApplyEnvOverrides() error {
	return c.applyEnvOverrides("KAPACITOR", reflect.ValueOf(c))
}

func (c *Config) applyEnvOverrides(prefix string, spec reflect.Value) error {
	// If we have a pointer, dereference it
	s := spec
	if spec.Kind() == reflect.Ptr {
		s = spec.Elem()
	}

	// Make sure we have struct
	if s.Kind() != reflect.Struct {
		return nil
	}

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
			value := os.Getenv(key)

			// If the type is s slice, apply to each using the index as a suffix
			// e.g. GRAPHITE_0
			if f.Kind() == reflect.Slice || f.Kind() == reflect.Array {
				for i := 0; i < f.Len(); i++ {
					if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), f.Index(i)); err != nil {
						return err
					}
				}
				continue
			}

			// If it's a sub-config, recursively apply
			if f.Kind() == reflect.Struct || f.Kind() == reflect.Ptr {
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
				continue
			}

			// Skip any fields we don't have a value to set
			if value == "" {
				continue
			}

			switch f.Kind() {
			case reflect.String:
				f.SetString(value)
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

				var intValue int64

				// Handle toml.Duration
				if f.Type().Name() == "Duration" {
					dur, err := time.ParseDuration(value)
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldName, f.Type().String(), value)
					}
					intValue = dur.Nanoseconds()
				} else {
					var err error
					intValue, err = strconv.ParseInt(value, 0, f.Type().Bits())
					if err != nil {
						return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldName, f.Type().String(), value)
					}
				}

				f.SetInt(intValue)
			case reflect.Bool:
				boolValue, err := strconv.ParseBool(value)
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldName, f.Type().String(), value)

				}
				f.SetBool(boolValue)
			case reflect.Float32, reflect.Float64:
				floatValue, err := strconv.ParseFloat(value, f.Type().Bits())
				if err != nil {
					return fmt.Errorf("failed to apply %v to %v using type %v and value '%v'", key, fieldName, f.Type().String(), value)

				}
				f.SetFloat(floatValue)
			default:
				if err := c.applyEnvOverrides(key, f); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
