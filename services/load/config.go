package load

import (
	"errors"
	"path/filepath"
)

const taskDir = "tasks"
const templateDir = "templates"
const handlerDir = "handlers"

type Config struct {
	Enabled bool   `toml:"enabled"`
	Dir     string `toml:"dir"`
}

func NewConfig() Config {
	return Config{
		Enabled: false,
		Dir:     "./load",
	}
}

// Validates verifies that the directory specified is an absolute path
// and that it contains the directories /tasks and /handlers. The directory
// may contain additional files, but must at least contain /tasks and /handlers.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	// Verify that the path is absolute
	if !filepath.IsAbs(c.Dir) {
		return errors.New("dir must be an absolute path")
	}

	return nil
}

func (c Config) tasksDir() string {
	return filepath.Join(c.Dir, taskDir)
}

func (c Config) templatesDir() string {
	return filepath.Join(c.Dir, templateDir)
}

func (c Config) handlersDir() string {
	return filepath.Join(c.Dir, handlerDir)
}
