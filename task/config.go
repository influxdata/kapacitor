package task

import "fmt"

type Config struct {
	// Enabled determines if flux tasks are enabled
	Enabled bool `toml:"enabled"`

	// TaskRunInfluxDB is the name of the influxdb instance finished
	// task runs and logs are written to.
	// Leaving it blank will write to Kapacitor's default influxdb instance.
	// Setting it to 'none' will disable task logging.
	TaskRunInfluxDB string `toml:"task-run-influxdb"`

	// TaskRunBucket is the bucket (or influxdb 1.x database) to use for saving
	// task runs and logs
	TaskRunBucket string `toml:"task-run-bucket"`

	// TaskRunOrg is the org to use for saving task runs and logs
	// task runs and logs.
	// This is ignored if TaskRunInfluxDB is a 1.x database
	// Only one of TaskRunOrg and TaskRunOrgID should be set
	TaskRunOrg string `toml:"task-run-org"`

	// TaskRunOrgID is the org to use for saving task runs and logs
	// task runs and logs.
	// This is ignored if TaskRunInfluxDB is a 1.x database
	// Only one of TaskRunOrg and TaskRunOrgID should be set
	TaskRunOrgID string `toml:"task-run-orgid"`

	// TaskRunMeasurement is the measurement used for saving task runs
	// and logs.
	// The defaults is "runs"
	TaskRunMeasurement string `toml:"task-run-measurement"`

	// Secrets is the kapacitor provider for secrets as described at
	// https://docs.influxdata.com/influxdb/v2.0/security/secrets/
	Secrets map[string]string `toml:"secrets"`
}

func NewConfig() Config {
	return Config{
		TaskRunMeasurement: "runs",
	}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.TaskRunInfluxDB == "none" {
		return nil
	}
	if len(c.TaskRunOrgID) > 0 && len(c.TaskRunOrg) > 0 {
		return fmt.Errorf("only one of task-run-org and task-run-orgid should be set")
	}
	if len(c.TaskRunBucket) == 0 {
		return fmt.Errorf("task-run-bucket is required")
	}
	return nil
}
