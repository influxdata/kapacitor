package udp

const (
	// The number of packets to buffer when reading off the socket.
	// A buffer of this size will be allocated for each instance of a UDP service.
	DefaultBuffer int = 1e3
)

type Config struct {
	Enabled     bool   `toml:"enabled"`
	BindAddress string `toml:"bind-address"`
	ReadBuffer  int    `toml:"read-buffer"`
	Buffer      int    `toml:"buffer"`

	Database        string `toml:"database"`
	RetentionPolicy string `toml:"retention-policy"`
}

// WithDefaults takes the given config and returns a new config with any required
// default values set.
func (c *Config) WithDefaults() *Config {
	d := *c
	d.Buffer = DefaultBuffer
	return &d
}
