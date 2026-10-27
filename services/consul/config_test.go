package consul

import (
	"testing"
)

func TestConfig_PromConfigBasicAuth(t *testing.T) {
	c := Config{}
	c.Init()
	c.Username = "user"
	c.Password = "pass"
	sd := c.PromConfig()
	if sd.HTTPClientConfig.BasicAuth == nil {
		t.Fatal("expected basic auth to be set")
	}
	if got, exp := sd.HTTPClientConfig.BasicAuth.Username, "user"; got != exp {
		t.Errorf("unexpected username: got %q exp %q", got, exp)
	}
	if got, exp := string(sd.HTTPClientConfig.BasicAuth.Password), "pass"; got != exp {
		t.Errorf("unexpected password: got %q exp %q", got, exp)
	}

	c = Config{}
	c.Init()
	if c.PromConfig().HTTPClientConfig.BasicAuth != nil {
		t.Error("expected no basic auth without username or password")
	}
}
