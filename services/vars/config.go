package vars

type Config map[string]string

func NewConfig() Config {
	return make(Config)
}
