package run

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
	InfluxDB []influxdb.Config `toml:"influxdb"`
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

	// The index of the default InfluxDB config
	defaultInfluxDB int
}

// NewConfig returns an instance of Config with reasonable defaults.
func NewConfig() *Config {
	c := &Config{
		Hostname: "localhost",
	}

	c.HTTP = httpd.NewConfig()
	c.Replay = replay.NewConfig()
	c.Task = task_store.NewConfig()
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

// Once the config has been created and decoded, you can call this method
// to initialize ARRAY attributes.
// All ARRAY attributes have to be init after toml decode
// See: https://github.com/BurntSushi/toml/pull/68
func (c *Config) PostInit() {
	if len(c.InfluxDB) == 0 {
		i := influxdb.NewConfig()
		c.InfluxDB = []influxdb.Config{i}
		c.InfluxDB[0].Name = "default"
		c.InfluxDB[0].URLs = []string{"http://localhost:8086"}
	} else if len(c.InfluxDB) == 1 && c.InfluxDB[0].Name == "" {
		c.InfluxDB[0].Name = "default"
	}
}

// NewDemoConfig returns the config that runs when no config is specified.
func NewDemoConfig() (*Config, error) {
	c := NewConfig()
	c.PostInit()

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
	c.defaultInfluxDB = -1
	names := make(map[string]bool, len(c.InfluxDB))
	for i := 0; i < len(c.InfluxDB); i++ {
		config := c.InfluxDB[i]
		if !config.Enabled {
			c.InfluxDB = append(c.InfluxDB[0:i], c.InfluxDB[i+1:]...)
			i--
			continue
		}
		if names[config.Name] {
			return fmt.Errorf("duplicate name %q for influxdb configs", config.Name)
		}
		names[config.Name] = true
		err = config.Validate()
		if err != nil {
			return err
		}
		if config.Default {
			if c.defaultInfluxDB != -1 {
				return fmt.Errorf("More than one InfluxDB default was specified: %s %s", config.Name, c.InfluxDB[c.defaultInfluxDB].Name)
			}
			c.defaultInfluxDB = i
		}
	}
	// Set default if it is the only one
	if len(c.InfluxDB) == 1 {
		c.defaultInfluxDB = 0
	}
	if len(c.InfluxDB) > 0 && c.defaultInfluxDB == -1 {
		return errors.New("at least one InfluxDB cluster must be marked as default.")
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
		c.applyEnvOverridesToStruct(prefix, s)
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
