package httppost

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/url"
	"path"
	"text/template"

	"github.com/pkg/errors"
)

type BasicAuth struct {
	Username string `toml:"username" json:"username"`
	Password string `toml:"password" json:"password"`
}

func (b BasicAuth) valid() bool {
	return b.Username != "" && b.Password != ""
}

func (b BasicAuth) validate() error {
	if !b.valid() {
		return errors.New("basic-auth must set both \"username\" and \"password\" parameters")
	}

	return nil
}

// Config is the configuration for a single [[httppost]] section of the kapacitor
// configuration file.
type Config struct {
	Endpoint          string            `toml:"endpoint" override:"endpoint"`
	URL               string            `toml:"url" override:"url"`
	Headers           map[string]string `toml:"headers" override:"headers"`
	BasicAuth         BasicAuth         `toml:"basic-auth" override:"basic-auth,redact"`
	AlertTemplate     string            `toml:"alert-template" override:"alert-template"`
	AlertTemplateFile string            `toml:"alert-template-file" override:"alert-template-file"`
	RowTemplate       string            `toml:"row-template" override:"row-template"`
	RowTemplateFile   string            `toml:"row-template-file" override:"row-template-file"`
}

func NewConfig() Config {
	return Config{
		Endpoint: "example",
		URL:      "http://example.com",
	}
}

// Validate ensures that all configurations options are valid. The Endpoint,
// and URL parameters must be set to be considered valid.
func (c Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("must specify endpoint name")
	}

	if c.URL == "" {
		return errors.New("must specify url")
	}

	if _, err := url.Parse(c.URL); err != nil {
		return errors.Wrapf(err, "invalid URL %q", c.URL)
	}

	if c.AlertTemplate != "" && c.AlertTemplateFile != "" {
		return errors.New("must specify only one of alert-template and alert-template-file")
	}

	if c.AlertTemplateFile != "" && !path.IsAbs(c.AlertTemplateFile) {
		return errors.New("must use an absolute path for alert-template-file")
	}

	if c.RowTemplate != "" && c.RowTemplateFile != "" {
		return errors.New("must specify only one of row-template and row-template-file")
	}

	if c.RowTemplateFile != "" && !path.IsAbs(c.RowTemplateFile) {
		return errors.New("must use an absolute path for row-template-file")
	}

	return nil
}

func (c Config) getAlertTemplate() (*template.Template, error) {
	return getTemplate(c.AlertTemplate, c.AlertTemplateFile)
}
func (c Config) getRowTemplate() (*template.Template, error) {
	return getTemplate(c.RowTemplate, c.RowTemplateFile)
}

func getTemplate(tmpl, tpath string) (*template.Template, error) {
	if tmpl != "" {
		t, err := template.New("body").Funcs(template.FuncMap{
			"json": func(v interface{}) string {
				buf := bytes.Buffer{}
				_ = json.NewEncoder(&buf).Encode(v)
				return buf.String()
			},
		}).Parse(tmpl)
		return t, errors.Wrap(err, "failed to parse template")
	}
	if tpath != "" {
		data, err := ioutil.ReadFile(tpath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read template file %q", tpath)
		}

		t, err := template.New("body").Parse(string(data))
		return t, errors.Wrapf(err, "failed to parse template from file %q", tpath)
	}
	return nil, nil
}

// Configs is the configuration for all [[alertpost]] sections of the kapacitor
// configuration file.
type Configs []Config

// Validate calls config.Validate for each element in Configs
func (cs Configs) Validate() error {
	for _, c := range cs {
		err := c.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

// index generates a map from config.Endpoint to config
func (cs Configs) index() (map[string]*Endpoint, error) {
	m := map[string]*Endpoint{}

	for _, c := range cs {
		at, err := c.getAlertTemplate()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get alert-template for endpoint %q", c.Endpoint)
		}
		rt, err := c.getRowTemplate()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get row-template for endpoint %q", c.Endpoint)
		}
		m[c.Endpoint] = NewEndpoint(c.URL, c.Headers, c.BasicAuth, at, rt)
	}

	return m, nil
}
