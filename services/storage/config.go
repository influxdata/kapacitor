package storage

import "fmt"

const (
	DefaultBoltDBPath = "./kapacitor.db"
)

type Config struct {
	// Path to a boltdb database file.
	BoltDBPath string `toml:"boltdb"`
}

func NewConfig() Config {
	return Config{
		BoltDBPath: DefaultBoltDBPath,
	}
}

func (c Config) Validate() error {
	if c.BoltDBPath == "" {
		return fmt.Errorf("must specify storage 'boltdb' path")
	}
	return nil
}
