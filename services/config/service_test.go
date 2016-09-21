package config_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/kapacitor/services/config"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/httpd/httpdtest"
	"github.com/influxdata/kapacitor/services/storage/storagetest"
)

type SectionA struct {
	Option1 string `toml:"option-1"`
}

type SectionB struct {
	Option2  string `toml:"option-2"`
	Password string `toml:"password" override:",redact"`
}

type SectionC struct {
	Name    string `toml:"name"`
	Option3 int    `toml:"option-3"`
}

type TestConfig struct {
	SectionA  SectionA   `toml:"section-a" override:"section-a"`
	SectionB  SectionB   `toml:"section-b" override:"section-b"`
	SectionCs []SectionC `toml:"section-c" override:"section-c,element-key=name"`
}

func OpenNewSerivce(testConfig interface{}, updates chan<- config.ConfigUpdate) (*config.Service, *httpdtest.Server) {
	service := config.NewService(testConfig, log.New(os.Stderr, "[config] ", log.LstdFlags), updates)
	service.StorageService = storagetest.New()
	server := httpdtest.NewServer(testing.Verbose())
	service.HTTPDService = server
	if err := service.Open(); err != nil {
		panic(err)
	}
	return service, server
}

func TestService_UpdateSection(t *testing.T) {
	testCases := []struct {
		body    string
		path    string
		expName string
		exp     interface{}
	}{
		{
			body:    `{"set":{"option-1": "new-o1"}}`,
			path:    "/section-a",
			expName: "section-a",
			exp: []interface{}{
				SectionA{
					Option1: "new-o1",
				},
			},
		},
	}
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
	}
	updates := make(chan config.ConfigUpdate, len(testCases))
	service, server := OpenNewSerivce(testConfig, updates)
	defer server.Close()
	defer service.Close()
	basePath := server.Server.URL + httpd.BasePath + "/config"
	for _, tc := range testCases {
		resp, err := http.Post(basePath+tc.path, "application/json", strings.NewReader(tc.body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}

		// Validate response
		if got, exp := resp.StatusCode, http.StatusNoContent; got != exp {
			t.Errorf("unexpected code: got %d exp %d.\nBody:\n%s", got, exp, string(body))
		}

		// Validate we got the update over the chan
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case cu := <-updates:
			if got, exp := cu.Name, tc.expName; got != exp {
				t.Errorf("unexpected config update Name: got %s exp %s", got, exp)
			}
			if !reflect.DeepEqual(cu.NewConfig, tc.exp) {
				t.Errorf("unexpected new config: got %v exp %v", cu.NewConfig, tc.exp)
			}
		case <-timer.C:
			t.Fatal("expected to get config update")
		}
	}
}

func TestService_GetConfig(t *testing.T) {
	type update struct {
		Path string
		Body string
	}
	testCases := []struct {
		updates []update
		expName string
		exp     map[string][]map[string]interface{}
	}{
		{
			updates: []update{{
				Path: "/section-a",
				Body: `{"set":{"option-1": "new-o1"}}`,
			}},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "new-o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-a",
					Body: `{"delete":["option-1"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-b",
					Body: `{"set":{"option-2":"new-o2"},"delete":["option-nonexistant"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "new-o1",
				}},
				"section-b": {{
					"option-2": "new-o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-a",
					Body: `{"set":{"option-1": "new-o1"}}`,
				},
				{
					Path: "/section-a",
					Body: `{"set":{"option-1":"deletd"},"delete":["option-1"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-b",
					Body: `{"set":{"password": "secret"}}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": true,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c/x",
					Body: `{"set":{"option-3": 42}}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(42),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c/x",
					Body: `{"set":{"option-3": 42}}`,
				},
				{
					Path: "/section-c/x",
					Body: `{"delete":["option-3"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "w",
						"option-3": float64(42),
					},
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c",
					Body: `{"add":{"name":"q"}}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "q",
						"option-3": float64(0),
					},
					{
						"name":     "w",
						"option-3": float64(42),
					},
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c/w",
					Body: `{"set":{"option-3": 24}}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "w",
						"option-3": float64(24),
					},
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"add":{"name":"w", "option-3": 42}}`,
				},
				{
					Path: "/section-c/w",
					Body: `{"set":{"option-3": 24}}`,
				},
				{
					Path: "/section-c",
					Body: `{"remove":["w"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
		{
			// Only added overrides can be removed, not existing default elements
			updates: []update{
				{
					Path: "/section-c",
					Body: `{"remove":["x"]}`,
				},
			},
			exp: map[string][]map[string]interface{}{
				"section-a": {{
					"option-1": "o1",
				}},
				"section-b": {{
					"option-2": "o2",
					"password": false,
				}},
				"section-c": {
					{
						"name":     "x",
						"option-3": float64(1),
					},
					{
						"name":     "y",
						"option-3": float64(2),
					},
					{
						"name":     "z",
						"option-3": float64(3),
					},
				},
			},
		},
	}
	testConfig := &TestConfig{
		SectionA: SectionA{
			Option1: "o1",
		},
		SectionB: SectionB{
			Option2: "o2",
		},
		SectionCs: []SectionC{
			{
				Name:    "x",
				Option3: 1,
			},
			{
				Name:    "y",
				Option3: 2,
			},
			{
				Name:    "z",
				Option3: 3,
			},
		},
	}
	for _, tc := range testCases {
		updates := make(chan config.ConfigUpdate, len(testCases))
		service, server := OpenNewSerivce(testConfig, updates)
		defer server.Close()
		defer service.Close()
		basePath := server.Server.URL + httpd.BasePath + "/config"
		// Apply all updates
		for _, update := range tc.updates {
			resp, err := http.Post(basePath+update.Path, "application/json", strings.NewReader(update.Body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if got, exp := resp.StatusCode, http.StatusNoContent; got != exp {
				t.Fatalf("update failed: got: %d exp: %d\nBody:\n%s", got, exp, string(body))
			}

			// Validate we got the update over the chan.
			// This keeps the chan unblocked.
			timer := time.NewTimer(10 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-updates:
				// We got it, nothing more to do.
			case <-timer.C:
				t.Fatal("expected to get config update")
			}
		}

		// Get config
		resp, err := http.Get(basePath)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("update failed: %d", resp.StatusCode)
		}

		got := make(map[string][]map[string]interface{})
		if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, tc.exp) {
			t.Errorf("unexpected config:\ngot\n%v\nexp\n%v\n", got, tc.exp)
		}
	}
}
