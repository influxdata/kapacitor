package httpd

type Config struct {
	BindAddress      string `toml:"bind-address"`
	AuthEnabled      bool   `toml:"auth-enabled"`
	LogEnabled       bool   `toml:"log-enabled"`
	WriteTracing     bool   `toml:"write-tracing"`
	PprofEnabled     bool   `toml:"pprof-enabled"`
	HttpsEnabled     bool   `toml:"https-enabled"`
	HttpsCertificate string `toml:"https-certificate"`
}

func NewConfig() Config {
	return Config{
		BindAddress:      ":9092",
		LogEnabled:       true,
		HttpsCertificate: "/etc/ssl/kapacitor.pem",
	}
}
