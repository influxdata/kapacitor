package server

import (
	"encoding"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/kapacitor/command"
	"github.com/influxdata/kapacitor/services/alerta"
	"github.com/influxdata/kapacitor/services/azure"
	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/consul"
	"github.com/influxdata/kapacitor/services/deadman"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/dns"
	"github.com/influxdata/kapacitor/services/ec2"
	"github.com/influxdata/kapacitor/services/file_discovery"
	"github.com/influxdata/kapacitor/services/gce"
	"github.com/influxdata/kapacitor/services/hipchat"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/kafka"
	"github.com/influxdata/kapacitor/services/load"
	"github.com/influxdata/kapacitor/services/marathon"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/nerve"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/opsgenie2"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/pushover"
	"github.com/influxdata/kapacitor/services/replay"
	"github.com/influxdata/kapacitor/services/reporting"
	"github.com/influxdata/kapacitor/services/scraper"
	"github.com/influxdata/kapacitor/services/sensu"
	"github.com/influxdata/kapacitor/services/serverset"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/smtp"
	"github.com/influxdata/kapacitor/services/snmptrap"
	"github.com/influxdata/kapacitor/services/static_discovery"
	"github.com/influxdata/kapacitor/services/stats"
	"github.com/influxdata/kapacitor/services/storage"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/talk"
	"github.com/influxdata/kapacitor/services/task_store"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/triton"
	"github.com/influxdata/kapacitor/services/udf"
	"github.com/influxdata/kapacitor/services/udp"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/pkg/errors"

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
	Load           load.Config       `toml:"load"`
	InfluxDB       []influxdb.Config `toml:"influxdb" override:"influxdb,element-key=name"`
	Logging        diagnostic.Config `toml:"logging"`
	ConfigOverride config.Config     `toml:"config-override"`

	// Input services
	Graphite []graphite.Config `toml:"graphite"`
	Collectd collectd.Config   `toml:"collectd"`
	OpenTSDB opentsdb.Config   `toml:"opentsdb"`
	UDP      []udp.Config      `toml:"udp"`

	// Alert handlers
	Alerta     alerta.Config     `toml:"alerta" override:"alerta"`
	HipChat    hipchat.Config    `toml:"hipchat" override:"hipchat"`
	Kafka      kafka.Configs     `toml:"kafka" override:"kafka,element-key=id"`
	MQTT       mqtt.Configs      `toml:"mqtt" override:"mqtt,element-key=name"`
	OpsGenie   opsgenie.Config   `toml:"opsgenie" override:"opsgenie"`
	OpsGenie2  opsgenie2.Config  `toml:"opsgenie2" override:"opsgenie2"`
	PagerDuty  pagerduty.Config  `toml:"pagerduty" override:"pagerduty"`
	PagerDuty2 pagerduty2.Config `toml:"pagerduty2" override:"pagerduty2"`
	Pushover   pushover.Config   `toml:"pushover" override:"pushover"`
	HTTPPost   httppost.Configs  `toml:"httppost" override:"httppost,element-key=endpoint"`
	SMTP       smtp.Config       `toml:"smtp" override:"smtp"`
	SNMPTrap   snmptrap.Config   `toml:"snmptrap" override:"snmptrap"`
	Sensu      sensu.Config      `toml:"sensu" override:"sensu"`
	Slack      slack.Configs     `toml:"slack" override:"slack,element-key=workspace"`
	Talk       talk.Config       `toml:"talk" override:"talk"`
	Telegram   telegram.Config   `toml:"telegram" override:"telegram"`
	VictorOps  victorops.Config  `toml:"victorops" override:"victorops"`

	// Discovery for scraping
	Scraper         []scraper.Config          `toml:"scraper" override:"scraper,element-key=name"`
	Azure           []azure.Config            `toml:"azure" override:"azure,element-key=id"`
	Consul          []consul.Config           `toml:"consul" override:"consul,element-key=id"`
	DNS             []dns.Config              `toml:"dns" override:"dns,element-key=id"`
	EC2             []ec2.Config              `toml:"ec2" override:"ec2,element-key=id"`
	FileDiscovery   []file_discovery.Config   `toml:"file-discovery" override:"file-discovery,element-key=id"`
	GCE             []gce.Config              `toml:"gce" override:"gce,element-key=id"`
	Marathon        []marathon.Config         `toml:"marathon" override:"marathon,element-key=id"`
	Nerve           []nerve.Config            `toml:"nerve" override:"nerve,element-key=id"`
	Serverset       []serverset.Config        `toml:"serverset" override:"serverset,element-key=id"`
	StaticDiscovery []static_discovery.Config `toml:"static-discovery" override:"static-discovery,element-key=id"`
	Triton          []triton.Config           `toml:"triton" override:"triton,element-key=id"`

	// Third-party integrations
	Kubernetes k8s.Configs   `toml:"kubernetes" override:"kubernetes,element-key=id" env-config:"implicit-index"`
	Swarm      swarm.Configs `toml:"swarm" override:"swarm,element-key=id"`

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
	c.Logging = diagnostic.NewConfig()
	c.ConfigOverride = config.NewConfig()

	c.Collectd = collectd.NewConfig()
	c.OpenTSDB = opentsdb.NewConfig()

	c.Alerta = alerta.NewConfig()
	c.HipChat = hipchat.NewConfig()
	c.Kafka = kafka.Configs{kafka.NewConfig()}
	c.MQTT = mqtt.Configs{mqtt.NewConfig()}
	c.OpsGenie = opsgenie.NewConfig()
	c.OpsGenie2 = opsgenie2.NewConfig()
	c.PagerDuty = pagerduty.NewConfig()
	c.PagerDuty2 = pagerduty2.NewConfig()
	c.Pushover = pushover.NewConfig()
	c.HTTPPost = httppost.Configs{httppost.NewConfig()}
	c.SMTP = smtp.NewConfig()
	c.Sensu = sensu.NewConfig()
	c.Slack = slack.Configs{slack.NewDefaultConfig()}
	c.Talk = talk.NewConfig()
	c.SNMPTrap = snmptrap.NewConfig()
	c.Telegram = telegram.NewConfig()
	c.VictorOps = victorops.NewConfig()

	c.Reporting = reporting.NewConfig()
	c.Stats = stats.NewConfig()
	c.UDF = udf.NewConfig()
	c.Deadman = deadman.NewConfig()
	c.Load = load.NewConfig()

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
	c.Load.Dir = filepath.Join(homeDir, ".kapacitor", c.Load.Dir)

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
		return errors.Wrap(err, "replay")
	}
	if err := c.Storage.Validate(); err != nil {
		return errors.Wrap(err, "storage")
	}
	if err := c.HTTP.Validate(); err != nil {
		return errors.Wrap(err, "http")
	}
	if err := c.Task.Validate(); err != nil {
		return errors.Wrap(err, "task")
	}
	if err := c.Load.Validate(); err != nil {
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
	for _, g := range c.Graphite {
		if err := g.Validate(); err != nil {
			return errors.Wrap(err, "graphite")
		}
	}

	// Validate alert handlers
	if err := c.Alerta.Validate(); err != nil {
		return errors.Wrap(err, "alerta")
	}
	if err := c.HipChat.Validate(); err != nil {
		return errors.Wrap(err, "hipchat")
	}
	if err := c.Kafka.Validate(); err != nil {
		return errors.Wrap(err, "kafka")
	}
	if err := c.MQTT.Validate(); err != nil {
		return errors.Wrap(err, "mqtt")
	}
	if err := c.OpsGenie.Validate(); err != nil {
		return errors.Wrap(err, "opsgenie")
	}
	if err := c.OpsGenie2.Validate(); err != nil {
		return errors.Wrap(err, "opsgenie2")
	}
	if err := c.PagerDuty.Validate(); err != nil {
		return errors.Wrap(err, "pagerduty")
	}
	if err := c.PagerDuty2.Validate(); err != nil {
		return errors.Wrap(err, "pagerduty2")
	}
	if err := c.Pushover.Validate(); err != nil {
		return errors.Wrap(err, "pushover")
	}
	if err := c.HTTPPost.Validate(); err != nil {
		return errors.Wrap(err, "httppost")
	}
	if err := c.SMTP.Validate(); err != nil {
		return errors.Wrap(err, "smtp")
	}
	if err := c.SNMPTrap.Validate(); err != nil {
		return errors.Wrap(err, "snmptrap")
	}
	if err := c.Sensu.Validate(); err != nil {
		return errors.Wrap(err, "sensu")
	}
	if err := c.Slack.Validate(); err != nil {
		return errors.Wrap(err, "slack")
	}
	if err := c.Talk.Validate(); err != nil {
		return errors.Wrap(err, "talk")
	}
	if err := c.Telegram.Validate(); err != nil {
		return errors.Wrap(err, "telegram")
	}
	if err := c.VictorOps.Validate(); err != nil {
		return errors.Wrap(err, "victorops")
	}

	if err := c.UDF.Validate(); err != nil {
		return errors.Wrap(err, "udf")
	}

	// Validate scrapers
	for i := range c.Scraper {
		if err := c.Scraper[i].Validate(); err != nil {
			return errors.Wrapf(err, "scraper %q", c.Scraper[i].Name)
		}
	}

	for i := range c.Azure {
		if err := c.Azure[i].Validate(); err != nil {
			return errors.Wrapf(err, "azure %q", c.Azure[i].ID)
		}
	}

	for i := range c.Consul {
		if err := c.Consul[i].Validate(); err != nil {
			return errors.Wrapf(err, "consul %q", c.Consul[i].ID)
		}
	}

	for i := range c.DNS {
		if err := c.DNS[i].Validate(); err != nil {
			return errors.Wrapf(err, "dns %q", c.DNS[i].ID)
		}
	}

	for i := range c.EC2 {
		if err := c.EC2[i].Validate(); err != nil {
			return errors.Wrapf(err, "ec2 %q", c.EC2[i].ID)
		}
	}

	for i := range c.FileDiscovery {
		if err := c.FileDiscovery[i].Validate(); err != nil {
			return errors.Wrapf(err, "file discovery %q", c.FileDiscovery[i].ID)
		}
	}

	for i := range c.GCE {
		if err := c.GCE[i].Validate(); err != nil {
			return errors.Wrapf(err, "gce %q", c.GCE[i].ID)
		}
	}

	if err := c.Kubernetes.Validate(); err != nil {
		return errors.Wrap(err, "kubernetes")
	}

	for i := range c.Marathon {
		if err := c.Marathon[i].Validate(); err != nil {
			return errors.Wrapf(err, "marathon %q", c.Marathon[i].ID)
		}
	}

	for i := range c.Nerve {
		if err := c.Nerve[i].Validate(); err != nil {
			return errors.Wrapf(err, "nerve %q", c.Nerve[i].ID)
		}
	}

	for i := range c.Serverset {
		if err := c.Serverset[i].Validate(); err != nil {
			return errors.Wrapf(err, "serverset %q", c.Serverset[i].ID)
		}
	}

	for i := range c.StaticDiscovery {
		if err := c.StaticDiscovery[i].Validate(); err != nil {
			return errors.Wrapf(err, "static discovery %q", c.StaticDiscovery[i].ID)
		}
	}

	if err := c.Swarm.Validate(); err != nil {
		return errors.Wrap(err, "swarm")
	}

	for i := range c.Triton {
		if err := c.Triton[i].Validate(); err != nil {
			return errors.Wrapf(err, "triton %q", c.Triton[i].ID)
		}
	}

	return nil
}

func (c *Config) ApplyEnvOverrides() error {
	return c.applyEnvOverrides("KAPACITOR", "", reflect.ValueOf(c))
}

func (c *Config) applyEnvOverridesToMap(prefix string, fieldDesc string, mapValue, key, spec reflect.Value) error {
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
		mapValue.SetMapIndex(key, reflect.ValueOf(value))
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

		mapValue.SetMapIndex(key, reflect.ValueOf(intValue))
	case reflect.Bool:
		boolValue, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)

		}
		mapValue.SetMapIndex(key, reflect.ValueOf(boolValue))
	case reflect.Float32, reflect.Float64:
		floatValue, err := strconv.ParseFloat(value, s.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)

		}
		mapValue.SetMapIndex(key, reflect.ValueOf(floatValue))
	case reflect.Struct:
		if err := c.applyEnvOverridesToStruct(prefix, s); err != nil {
			return err
		}
	}
	return nil
}

var textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()

func (c *Config) applyEnvOverrides(prefix string, fieldDesc string, spec reflect.Value) error {
	// If we have a pointer, dereference it
	s := spec
	if spec.Kind() == reflect.Ptr {
		s = spec.Elem()
	}
	var addrSpec reflect.Value
	if i := reflect.Indirect(s); i.CanAddr() {
		addrSpec = i.Addr()
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

	// Check if the type is a test.Unmarshaler
	if addrSpec.Type().Implements(textUnmarshalerType) {
		um := addrSpec.Interface().(encoding.TextUnmarshaler)
		err := um.UnmarshalText([]byte(value))
		return errors.Wrap(err, "failed to unmarshal env var")
	}

	switch s.Kind() {
	case reflect.String:
		s.SetString(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		intValue, err := strconv.ParseInt(value, 0, s.Type().Bits())
		if err != nil {
			return fmt.Errorf("failed to apply %v%v using type %v and value '%v'", prefix, fieldDesc, s.Type().String(), value)
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

const (
	// envConfigTag is a struct tag key for specifying information to the apply env overrides configuration process.
	envConfigTag = "env-config"
	// implicitIndexTag is the name of the value of an env-config tag that instructs the process to allow implicit 0 indexes.
	implicitIndexTag = "implicit-index"
)

func (c *Config) applyEnvOverridesToStruct(prefix string, s reflect.Value) error {
	typeOfSpec := s.Type()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		// Get the toml tag to determine what env var name to use
		configName := typeOfSpec.Field(i).Tag.Get("toml")
		// Replace hyphens with underscores to avoid issues with shells
		configName = strings.Replace(configName, "-", "_", -1)
		fieldType := typeOfSpec.Field(i)
		fieldName := fieldType.Name

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
				// Determine if the field supports implicit indexes.
				implicitIndex := false
				fieldEnvConfigTags := strings.Split(fieldType.Tag.Get(envConfigTag), ",")
				for _, s := range fieldEnvConfigTags {
					if s == implicitIndexTag {
						implicitIndex = true
						break
					}
				}

				l := f.Len()
				for i := 0; i < l; i++ {
					// Also support an implicit 0 index, if there is only one entry and the slice supports it.
					// e.g. KAPACITOR_KUBERNETES_ENABLED=true
					if implicitIndex && l == 1 {
						if err := c.applyEnvOverrides(key, fieldName, f.Index(i)); err != nil {
							return err
						}
					}
					if err := c.applyEnvOverrides(fmt.Sprintf("%s_%d", key, i), fieldName, f.Index(i)); err != nil {
						return err
					}
				}
			} else if f.Kind() == reflect.Map {
				for _, k := range f.MapKeys() {
					if err := c.applyEnvOverridesToMap(fmt.Sprintf("%s_%v", key, k), fieldName, f, k, f.MapIndex(k)); err != nil {
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
