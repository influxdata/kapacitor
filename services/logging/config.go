package logging

type Config struct {
	File     string `toml:"file"`
	Level    string `toml:"level"`
	Encoding string `toml:"encoding"`
}

func NewConfig() Config {
	return Config{
		File:     "STDERR",
		Level:    "INFO",
		Encoding: "logfmt",
	}
}
