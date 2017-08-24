package diagnostic

type Config struct {
	File  string `toml:"file"`
	Level string `toml:"level"`
}

func NewConfig() Config {
	return Config{
		File:  "STDERR",
		Level: "DEBUG",
	}
}
