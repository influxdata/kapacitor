package server_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/google/go-cmp/cmp"
	iclient "github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/toml"
	"github.com/influxdata/kapacitor/client/v1"
	"github.com/influxdata/kapacitor/server"
	"github.com/influxdata/kapacitor/services/httppost"
	"github.com/influxdata/kapacitor/services/k8s"
	"github.com/influxdata/kapacitor/services/mqtt"
	"github.com/influxdata/kapacitor/services/mqtt/mqtttest"
	"github.com/influxdata/kapacitor/services/opsgenie"
	"github.com/influxdata/kapacitor/services/pagerduty"
	"github.com/influxdata/kapacitor/services/pagerduty2"
	"github.com/influxdata/kapacitor/services/removed"
	"github.com/influxdata/kapacitor/services/slack"
	"github.com/influxdata/kapacitor/services/swarm"
	"github.com/influxdata/kapacitor/services/telegram"
	"github.com/influxdata/kapacitor/services/victorops"
	"github.com/influxdata/kapacitor/services/zenoss"
	"github.com/pkg/errors"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestServer_UpdateConfig(t *testing.T) {
	type updateAction struct {
		name         string
		element      string
		updateAction client.ConfigUpdateAction
		expSection   client.ConfigSection
		expElement   client.ConfigElement
	}
	db := NewInfluxDB(func(q string) *iclient.Response {
		return &iclient.Response{}
	})
	defMap := map[string]interface{}{
		"default":                     false,
		"disable-subscriptions":       false,
		"enabled":                     true,
		"excluded-subscriptions":      map[string]interface{}{"_kapacitor": []interface{}{"autogen"}},
		"http-port":                   float64(0),
		"insecure-skip-verify":        false,
		"kapacitor-hostname":          "",
		"http-shared-secret":          false,
		"name":                        "default",
		"password":                    true,
		"ssl-ca":                      "",
		"ssl-cert":                    "",
		"ssl-key":                     "",
		"startup-timeout":             "1h0m0s",
		"subscription-protocol":       "http",
		"subscription-mode":           "cluster",
		"subscription-path":           "",
		"subscriptions":               nil,
		"subscriptions-sync-interval": "1m0s",
		"timeout":                     "0s",
		"token":                       false,
		"udp-bind":                    "",
		"udp-buffer":                  float64(1e3),
		"udp-read-buffer":             float64(0),
		"urls":                        []interface{}{db.URL()},
		"username":                    "bob",
		"compression":                 "gzip",
	}

	deepCopyMapWithReplace := func(m map[string]interface{}) func(replacements ...map[string]interface{}) map[string]interface{} {
		var a interface{} = "" // we have to do this to prevent gob.Register from panicing
		gob.Register([]string{})
		gob.Register(map[string]interface{}{})
		gob.Register([]interface{}{})
		gob.Register(a)
		return func(replacements ...map[string]interface{}) map[string]interface{} {
			// we can't just use json here because json(foo) doesn't always equal decodedJson(foo)
			var buf bytes.Buffer        // Stand-in for a network connection
			enc := gob.NewEncoder(&buf) // Will write to network.
			dec := gob.NewDecoder(&buf) // Will read from network.
			if err := enc.Encode(m); err != nil {
				t.Fatal(err)
			}
			mCopy := map[string]interface{}{}

			if err := dec.Decode(&mCopy); err != nil {
				t.Fatal(err)
			}
			for i := range replacements {
				for k, v := range replacements[i] {
					v := v
					if err := enc.Encode(v); err != nil {
						t.Fatal(err)
					}
					vCopy := reflect.Indirect(reflect.New(reflect.TypeOf(v)))
					if err := dec.DecodeValue(vCopy); err != nil {
						t.Fatal(err)
					}
					mCopy[k] = vCopy.Interface()
				}
			}
			return mCopy
		}
	}
	if !cmp.Equal(deepCopyMapWithReplace(defMap)(), defMap) {
		t.Fatalf("deepCopyMapWithReplace is broken expected %v, got %v", defMap, deepCopyMapWithReplace(defMap)())
	}
	{ // new block to keep vars clean

		mapReplaceRes := deepCopyMapWithReplace(map[string]interface{}{"1": []string{"ok"}, "2": 3})(map[string]interface{}{"1": []string{"oks"}})
		if !cmp.Equal(mapReplaceRes, map[string]interface{}{"1": []string{"oks"}, "2": 3}) {
			t.Fatal(cmp.Diff(mapReplaceRes, map[string]interface{}{"1": []string{"oks"}, "2": 3}))
		}
	}

	testCases := []struct {
		section           string
		element           string
		setDefaults       func(*server.Config)
		expDefaultSection client.ConfigSection
		expDefaultElement client.ConfigElement
		updates           []updateAction
		expErr            error
	}{
		{
			section: "influxdb",
			element: "default",
			setDefaults: func(c *server.Config) {
				c.InfluxDB[0].Enabled = true
				c.InfluxDB[0].Username = "bob"
				c.InfluxDB[0].Password = "secret"
				c.InfluxDB[0].URLs = []string{db.URL()}
				// Set really long timeout since we shouldn't hit it
				c.InfluxDB[0].StartUpTimeout = toml.Duration(time.Hour)
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
				Elements: []client.ConfigElement{{
					Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
					Options: deepCopyMapWithReplace(defMap)(),
					Redacted: []string{
						"password",
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
				Options: deepCopyMapWithReplace(defMap)(),
				Redacted: []string{
					"password",
					"token",
				},
			},
			updates: []updateAction{
				{
					name: "update url",
					// Set Invalid URL to make sure we can fix it without waiting for connection timeouts
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"urls": []interface{}{"http://192.0.2.0:8086"},
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: deepCopyMapWithReplace(defMap)(map[string]interface{}{"urls": []interface{}{"http://192.0.2.0:8086"}}),
							Redacted: []string{
								"password",
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link:    client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: deepCopyMapWithReplace(defMap)(map[string]interface{}{"urls": []interface{}{"http://192.0.2.0:8086"}}),
						Redacted: []string{
							"password",
							"token",
						},
					},
				},
				{
					name: "update default,  subscription-protocol, subscriptions",
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"default":               true,
							"subscription-protocol": "https",
							"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: deepCopyMapWithReplace(defMap)(
								map[string]interface{}{"urls": []interface{}{"http://192.0.2.0:8086"}},
								map[string]interface{}{
									"default":               true,
									"subscription-protocol": "https",
									"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
								}),
							Redacted: []string{
								"password",
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: deepCopyMapWithReplace(defMap)(
							map[string]interface{}{"urls": []interface{}{"http://192.0.2.0:8086"}},
							map[string]interface{}{
								"default":               true,
								"subscription-protocol": "https",
								"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
							}),
						Redacted: []string{
							"password",
							"token",
						},
					},
				},
				{
					name: "delete urls",
					updateAction: client.ConfigUpdateAction{
						Delete: []string{"urls"},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
							Options: deepCopyMapWithReplace(defMap)(
								map[string]interface{}{
									"default":               true,
									"subscription-protocol": "https",
									"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
								}),
							Redacted: []string{
								"password",
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
						Options: deepCopyMapWithReplace(defMap)(
							map[string]interface{}{
								"default":               true,
								"subscription-protocol": "https",
								"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
							}),
						Redacted: []string{
							"password",
							"token",
						},
					},
				},
				{
					name: "new",
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"name": "new",
							"urls": []string{db.URL()},
						},
					},
					element: "new",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/default"},
								Options: deepCopyMapWithReplace(defMap)(
									map[string]interface{}{
										"default":               true,
										"subscription-protocol": "https",
										"subscriptions":         map[string]interface{}{"_internal": []interface{}{"monitor"}},
									}),
								Redacted: []string{
									"password",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/new"},
								Options: deepCopyMapWithReplace(defMap)(
									map[string]interface{}{
										"name":            "new",
										"enabled":         false,
										"password":        false,
										"startup-timeout": "5m0s",
										"username":        "",
									}),
								Redacted: []string{
									"password",
									"token",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/influxdb/new"},
						Options: deepCopyMapWithReplace(defMap)(
							map[string]interface{}{
								"name":            "new",
								"enabled":         false,
								"password":        false,
								"startup-timeout": "5m0s",
								"username":        "",
							}),
						Redacted: []string{
							"password",
							"token",
						},
					},
				},
			},
		},
		{
			section: "alerta",
			setDefaults: func(c *server.Config) {
				c.Alerta.URL = "http://alerta.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
					Options: map[string]interface{}{
						"enabled":              false,
						"environment":          "",
						"origin":               "",
						"token":                false,
						"token-prefix":         "",
						"url":                  "http://alerta.example.com",
						"insecure-skip-verify": false,
						"timeout":              "0s",
					},
					Redacted: []string{
						"token",
					}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
				Options: map[string]interface{}{
					"enabled":              false,
					"environment":          "",
					"origin":               "",
					"token":                false,
					"token-prefix":         "",
					"url":                  "http://alerta.example.com",
					"insecure-skip-verify": false,
					"timeout":              "0s",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token":   "token",
							"origin":  "kapacitor",
							"timeout": "3h",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
							Options: map[string]interface{}{
								"enabled":              false,
								"environment":          "",
								"origin":               "kapacitor",
								"token":                true,
								"token-prefix":         "",
								"url":                  "http://alerta.example.com",
								"insecure-skip-verify": false,
								"timeout":              "3h0m0s",
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/alerta/"},
						Options: map[string]interface{}{
							"enabled":              false,
							"environment":          "",
							"origin":               "kapacitor",
							"token":                true,
							"token-prefix":         "",
							"url":                  "http://alerta.example.com",
							"insecure-skip-verify": false,
							"timeout":              "3h0m0s",
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "httppost",
			element: "test",
			setDefaults: func(c *server.Config) {
				apc := httppost.Config{
					Endpoint:    "test",
					URLTemplate: "http://httppost.example.com",
					Headers: map[string]string{
						"testing": "works",
					},
				}
				c.HTTPPost = httppost.Configs{apc}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost"},
				Elements: []client.ConfigElement{
					{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
						Options: map[string]interface{}{
							"endpoint": "test",
							"url":      "http://httppost.example.com",
							"headers": map[string]interface{}{
								"testing": "works",
							},
							"basic-auth":          false,
							"alert-template":      "",
							"alert-template-file": "",
							"row-template":        "",
							"row-template-file":   "",
						},
						Redacted: []string{
							"basic-auth",
						}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
				Options: map[string]interface{}{
					"endpoint": "test",
					"url":      "http://httppost.example.com",
					"headers": map[string]interface{}{
						"testing": "works",
					},
					"basic-auth":          false,
					"alert-template":      "",
					"alert-template-file": "",
					"row-template":        "",
					"row-template-file":   "",
				},
				Redacted: []string{
					"basic-auth",
				},
			},
			updates: []updateAction{
				{
					element: "test",
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"headers": map[string]string{
								"testing": "more",
							},
							"basic-auth": httppost.BasicAuth{
								Username: "usr",
								Password: "pass",
							},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
							Options: map[string]interface{}{
								"endpoint": "test",
								"url":      "http://httppost.example.com",
								"headers": map[string]interface{}{
									"testing": "more",
								},
								"basic-auth":          true,
								"alert-template":      "",
								"alert-template-file": "",
								"row-template":        "",
								"row-template-file":   "",
							},
							Redacted: []string{
								"basic-auth",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/httppost/test"},
						Options: map[string]interface{}{
							"endpoint": "test",
							"url":      "http://httppost.example.com",
							"headers": map[string]interface{}{
								"testing": "more",
							},
							"basic-auth":          true,
							"alert-template":      "",
							"alert-template-file": "",
							"row-template":        "",
							"row-template-file":   "",
						},
						Redacted: []string{
							"basic-auth",
						},
					},
				},
			},
		},
		{
			section: "pushover",
			setDefaults: func(c *server.Config) {
				c.Pushover.URL = "http://pushover.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
					Options: map[string]interface{}{
						"enabled":  false,
						"token":    false,
						"user-key": false,
						"url":      "http://pushover.example.com",
					},
					Redacted: []string{
						"token",
						"user-key",
					}},
				},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
				Options: map[string]interface{}{
					"enabled":  false,
					"token":    false,
					"user-key": false,
					"url":      "http://pushover.example.com",
				},
				Redacted: []string{
					"token",
					"user-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token":    "token",
							"user-key": "kapacitor",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
							Options: map[string]interface{}{
								"enabled":  false,
								"user-key": true,
								"token":    true,
								"url":      "http://pushover.example.com",
							},
							Redacted: []string{
								"token",
								"user-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pushover/"},
						Options: map[string]interface{}{
							"enabled":  false,
							"user-key": true,
							"token":    true,
							"url":      "http://pushover.example.com",
						},
						Redacted: []string{
							"token",
							"user-key",
						},
					},
				},
			},
		},
		{
			section: "kubernetes",
			setDefaults: func(c *server.Config) {
				c.Kubernetes = k8s.Configs{k8s.NewConfig()}
				c.Kubernetes[0].APIServers = []string{"http://localhost:80001"}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
					Options: map[string]interface{}{
						"id":          "",
						"api-servers": []interface{}{"http://localhost:80001"},
						"ca-path":     "",
						"enabled":     false,
						"in-cluster":  false,
						"namespace":   "",
						"token":       false,
						"resource":    "",
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
				Options: map[string]interface{}{
					"id":          "",
					"api-servers": []interface{}{"http://localhost:80001"},
					"ca-path":     "",
					"enabled":     false,
					"in-cluster":  false,
					"namespace":   "",
					"token":       false,
					"resource":    "",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"token": "secret",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
							Options: map[string]interface{}{
								"id":          "",
								"api-servers": []interface{}{"http://localhost:80001"},
								"ca-path":     "",
								"enabled":     false,
								"in-cluster":  false,
								"namespace":   "",
								"token":       true,
								"resource":    "",
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/kubernetes/"},
						Options: map[string]interface{}{
							"id":          "",
							"api-servers": []interface{}{"http://localhost:80001"},
							"ca-path":     "",
							"enabled":     false,
							"in-cluster":  false,
							"namespace":   "",
							"token":       true,
							"resource":    "",
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "hipchat",
			setDefaults: func(c *server.Config) {
				c.HipChat.Enabled = false
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
					Options: map[string]interface{}{
						"enabled": false,
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
				Options: map[string]interface{}{
					"enabled": false,
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
							Options: map[string]interface{}{
								"enabled": false,
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/hipchat/"},
						Options: map[string]interface{}{
							"enabled": false,
						},
					},
				},
			},
			expErr: fmt.Errorf("failed to update configuration hipchat/: %s", removed.ErrHipChatRemoved),
		},
		{
			section: "mqtt",
			setDefaults: func(c *server.Config) {
				cfg := &mqtt.Config{
					Name: "default",
					URL:  "tcp://mqtt.example.com:1883",
				}
				cfg.SetNewClientF(mqtttest.NewClient)
				c.MQTT = mqtt.Configs{
					*cfg,
				}
			},
			element: "default",
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
					Options: map[string]interface{}{
						"enabled":              false,
						"name":                 "default",
						"default":              false,
						"url":                  "tcp://mqtt.example.com:1883",
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
						"client-id":            "",
						"username":             "",
						"password":             false,
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
				Options: map[string]interface{}{
					"enabled":              false,
					"name":                 "default",
					"default":              false,
					"url":                  "tcp://mqtt.example.com:1883",
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
					"client-id":            "",
					"username":             "",
					"password":             false,
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"client-id": "kapacitor-default",
							"password":  "super secret",
						},
					},
					element: "default",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
							Options: map[string]interface{}{
								"enabled":              false,
								"name":                 "default",
								"default":              false,
								"url":                  "tcp://mqtt.example.com:1883",
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
								"client-id":            "kapacitor-default",
								"username":             "",
								"password":             true,
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/mqtt/default"},
						Options: map[string]interface{}{
							"enabled":              false,
							"name":                 "default",
							"default":              false,
							"url":                  "tcp://mqtt.example.com:1883",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
							"client-id":            "kapacitor-default",
							"username":             "",
							"password":             true,
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "opsgenie",
			setDefaults: func(c *server.Config) {
				c.OpsGenie.URL = "http://opsgenie.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
					Options: map[string]interface{}{
						"api-key":      false,
						"enabled":      false,
						"global":       false,
						"recipients":   nil,
						"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
						"teams":        nil,
						"url":          "http://opsgenie.example.com",
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
				Options: map[string]interface{}{
					"api-key":      false,
					"enabled":      false,
					"global":       false,
					"recipients":   nil,
					"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
					"teams":        nil,
					"url":          "http://opsgenie.example.com",
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key": "token",
							"global":  true,
							"teams":   []string{"teamA", "teamB"},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
							Options: map[string]interface{}{
								"api-key":      true,
								"enabled":      false,
								"global":       true,
								"recipients":   nil,
								"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
								"teams":        []interface{}{"teamA", "teamB"},
								"url":          "http://opsgenie.example.com",
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie/"},
						Options: map[string]interface{}{
							"api-key":      true,
							"enabled":      false,
							"global":       true,
							"recipients":   nil,
							"recovery_url": opsgenie.DefaultOpsGenieRecoveryURL,
							"teams":        []interface{}{"teamA", "teamB"},
							"url":          "http://opsgenie.example.com",
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
		{
			section: "opsgenie2",
			setDefaults: func(c *server.Config) {
				c.OpsGenie2.URL = "http://opsgenie2.example.com"
				c.OpsGenie2.RecoveryAction = "notes"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
					Options: map[string]interface{}{
						"api-key":         false,
						"enabled":         false,
						"global":          false,
						"details":         false,
						"recipients":      nil,
						"teams":           nil,
						"url":             "http://opsgenie2.example.com",
						"recovery_action": "notes",
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
				Options: map[string]interface{}{
					"api-key":         false,
					"enabled":         false,
					"global":          false,
					"details":         false,
					"recipients":      nil,
					"teams":           nil,
					"url":             "http://opsgenie2.example.com",
					"recovery_action": "notes",
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key": "token",
							"global":  true,
							"teams":   []string{"teamA", "teamB"},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
							Options: map[string]interface{}{
								"api-key":         true,
								"enabled":         false,
								"global":          true,
								"details":         false,
								"recipients":      nil,
								"teams":           []interface{}{"teamA", "teamB"},
								"url":             "http://opsgenie2.example.com",
								"recovery_action": "notes",
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/opsgenie2/"},
						Options: map[string]interface{}{
							"api-key":         true,
							"enabled":         false,
							"global":          true,
							"details":         false,
							"recipients":      nil,
							"teams":           []interface{}{"teamA", "teamB"},
							"url":             "http://opsgenie2.example.com",
							"recovery_action": "notes",
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
		{
			section: "pagerduty",
			setDefaults: func(c *server.Config) {
				c.PagerDuty.ServiceKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"global":      false,
						"service-key": true,
						"url":         pagerduty.DefaultPagerDutyAPIURL,
					},
					Redacted: []string{
						"service-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"global":      false,
					"service-key": true,
					"url":         pagerduty.DefaultPagerDutyAPIURL,
				},
				Redacted: []string{
					"service-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"service-key": "",
							"enabled":     true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"global":      false,
								"service-key": false,
								"url":         pagerduty.DefaultPagerDutyAPIURL,
							},
							Redacted: []string{
								"service-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"global":      false,
							"service-key": false,
							"url":         pagerduty.DefaultPagerDutyAPIURL,
						},
						Redacted: []string{
							"service-key",
						},
					},
				},
			},
		},
		{
			section: "pagerduty2",
			setDefaults: func(c *server.Config) {
				c.PagerDuty2.RoutingKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"global":      false,
						"routing-key": true,
						"url":         pagerduty2.DefaultPagerDuty2APIURL,
					},
					Redacted: []string{
						"routing-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"global":      false,
					"routing-key": true,
					"url":         pagerduty2.DefaultPagerDuty2APIURL,
				},
				Redacted: []string{
					"routing-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"routing-key": "",
							"enabled":     true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"global":      false,
								"routing-key": false,
								"url":         pagerduty2.DefaultPagerDuty2APIURL,
							},
							Redacted: []string{
								"routing-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/pagerduty2/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"global":      false,
							"routing-key": false,
							"url":         pagerduty2.DefaultPagerDuty2APIURL,
						},
						Redacted: []string{
							"routing-key",
						},
					},
				},
			},
		},
		{
			section: "smtp",
			setDefaults: func(c *server.Config) {
				c.SMTP.Host = "smtp.example.com"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"from":               "",
						"global":             false,
						"host":               "smtp.example.com",
						"idle-timeout":       "30s",
						"no-verify":          false,
						"password":           false,
						"port":               float64(25),
						"state-changes-only": false,
						"to":                 nil,
						"to-templates":       nil,
						"username":           "",
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"from":               "",
					"global":             false,
					"host":               "smtp.example.com",
					"idle-timeout":       "30s",
					"no-verify":          false,
					"password":           false,
					"port":               float64(25),
					"state-changes-only": false,
					"to":                 nil,
					"to-templates":       nil,
					"username":           "",
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"idle-timeout": "1m0s",
							"global":       true,
							"password":     "secret",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
							Options: map[string]interface{}{
								"enabled":            false,
								"from":               "",
								"global":             true,
								"host":               "smtp.example.com",
								"idle-timeout":       "1m0s",
								"no-verify":          false,
								"password":           true,
								"port":               float64(25),
								"state-changes-only": false,
								"to-templates":       nil,
								"to":                 nil,
								"username":           "",
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/smtp/"},
						Options: map[string]interface{}{
							"enabled":            false,
							"from":               "",
							"global":             true,
							"host":               "smtp.example.com",
							"idle-timeout":       "1m0s",
							"no-verify":          false,
							"password":           true,
							"port":               float64(25),
							"state-changes-only": false,
							"to-templates":       nil,
							"to":                 nil,
							"username":           "",
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "sensu",
			setDefaults: func(c *server.Config) {
				c.Sensu.Addr = "sensu.example.com:3000"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
					Options: map[string]interface{}{
						"addr":     "sensu.example.com:3000",
						"enabled":  false,
						"source":   "Kapacitor",
						"handlers": nil,
					},
					Redacted: nil,
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
				Options: map[string]interface{}{
					"addr":     "sensu.example.com:3000",
					"enabled":  false,
					"source":   "Kapacitor",
					"handlers": nil,
				},
				Redacted: nil,
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"addr":    "sensu.local:3000",
							"enabled": true,
							"source":  "Kapacitor",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
							Options: map[string]interface{}{
								"addr":     "sensu.local:3000",
								"enabled":  true,
								"source":   "Kapacitor",
								"handlers": nil,
							},
							Redacted: nil,
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/sensu/"},
						Options: map[string]interface{}{
							"addr":     "sensu.local:3000",
							"enabled":  true,
							"source":   "Kapacitor",
							"handlers": nil,
						},
						Redacted: nil,
					},
				},
			},
		},
		{
			section: "servicenow",
			setDefaults: func(c *server.Config) {
				c.ServiceNow.URL = "https://instance.service-now.com/api/global/em/jsonv2"
				c.ServiceNow.Source = "Kapacitor"
				c.ServiceNow.Username = ""
				c.ServiceNow.Password = ""
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"global":             false,
						"state-changes-only": false,
						"url":                "https://instance.service-now.com/api/global/em/jsonv2",
						"source":             "Kapacitor",
						"username":           "",
						"password":           false,
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"global":             false,
					"state-changes-only": false,
					"url":                "https://instance.service-now.com/api/global/em/jsonv2",
					"source":             "Kapacitor",
					"username":           "",
					"password":           false,
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":  true,
							"url":      "https://dev12345.service-now.com/api/global/em/jsonv2",
							"username": "dev",
							"password": "12345",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow/"},
							Options: map[string]interface{}{
								"enabled":            true,
								"global":             false,
								"state-changes-only": false,
								"url":                "https://dev12345.service-now.com/api/global/em/jsonv2",
								"source":             "Kapacitor",
								"username":           "dev",
								"password":           true,
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/servicenow/"},
						Options: map[string]interface{}{
							"enabled":            true,
							"global":             false,
							"state-changes-only": false,
							"url":                "https://dev12345.service-now.com/api/global/em/jsonv2",
							"source":             "Kapacitor",
							"username":           "dev",
							"password":           true,
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
		{
			section: "bigpanda",
			setDefaults: func(c *server.Config) {
				c.BigPanda.URL = "https://api.bigpanda.io/data/v2/alerts"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda/"},
					Options: map[string]interface{}{
						"enabled":              false,
						"global":               false,
						"state-changes-only":   false,
						"url":                  "https://api.bigpanda.io/data/v2/alerts",
						"insecure-skip-verify": false,
						"token":                false,
						"app-key":              "",
						"auto-attributes":      "tags,fields",
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda/"},
				Options: map[string]interface{}{
					"enabled":              false,
					"global":               false,
					"state-changes-only":   false,
					"url":                  "https://api.bigpanda.io/data/v2/alerts",
					"insecure-skip-verify": false,
					"token":                false,
					"app-key":              "",
					"auto-attributes":      "tags,fields",
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":         true,
							"url":             "https://dev123456.bigpanda.io/data/v2/alerts",
							"app-key":         "appkey-123",
							"token":           "token-123",
							"auto-attributes": "",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda/"},
							Options: map[string]interface{}{
								"enabled":              true,
								"global":               false,
								"state-changes-only":   false,
								"url":                  "https://dev123456.bigpanda.io/data/v2/alerts",
								"token":                true,
								"app-key":              "appkey-123",
								"auto-attributes":      "",
								"insecure-skip-verify": false,
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/bigpanda/"},
						Options: map[string]interface{}{
							"enabled":              true,
							"global":               false,
							"state-changes-only":   false,
							"url":                  "https://dev123456.bigpanda.io/data/v2/alerts",
							"app-key":              "appkey-123",
							"auto-attributes":      "",
							"token":                true,
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "slack",
			setDefaults: func(c *server.Config) {
				cfg := &slack.Config{
					Global:   true,
					Default:  true,
					Username: slack.DefaultUsername,
				}

				c.Slack = slack.Configs{
					*cfg,
				}
			},
			element: "",
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
					Options: map[string]interface{}{
						"workspace":            "",
						"default":              true,
						"channel":              "",
						"enabled":              false,
						"global":               true,
						"icon-emoji":           "",
						"state-changes-only":   false,
						"token":                false,
						"url":                  false,
						"username":             "kapacitor",
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
					},
					Redacted: []string{
						"url",
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
				Options: map[string]interface{}{
					"workspace":            "",
					"default":              true,
					"channel":              "",
					"enabled":              false,
					"global":               true,
					"icon-emoji":           "",
					"state-changes-only":   false,
					"url":                  false,
					"token":                false,
					"username":             "kapacitor",
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
				},
				Redacted: []string{
					"url",
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"workspace": "company_private",
							"enabled":   true,
							"global":    false,
							"channel":   "#general",
							"username":  slack.DefaultUsername,
							"url":       "http://slack.example.com/secret-token",
							"token":     "my_other_secret",
						},
					},
					element: "company_private",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
							Options: map[string]interface{}{
								"workspace":            "",
								"default":              true,
								"channel":              "",
								"enabled":              false,
								"global":               true,
								"icon-emoji":           "",
								"state-changes-only":   false,
								"url":                  false,
								"token":                false,
								"username":             "kapacitor",
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
							},
							Redacted: []string{
								"url",
								"token",
							},
						},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
						Options: map[string]interface{}{
							"workspace":            "company_private",
							"channel":              "#general",
							"default":              false,
							"enabled":              true,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"token":                true,
							"username":             "kapacitor",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
							"token",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Add: map[string]interface{}{
							"workspace": "company_public",
							"enabled":   true,
							"global":    false,
							"channel":   "#general",
							"username":  slack.DefaultUsername,
							"url":       "http://slack.example.com/secret-token",
						},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"token":                false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              true,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"token":                false,
							"username":             "kapacitor",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
							"token",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":  false,
							"username": "testbot",
						},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"token":                false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              false,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                false,
									"username":             "testbot",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              false,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"token":                false,
							"username":             "testbot",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
							"token",
						},
					},
				},
				{
					updateAction: client.ConfigUpdateAction{
						Delete: []string{"username"},
					},
					element: "company_public",
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack"},
						Elements: []client.ConfigElement{
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/"},
								Options: map[string]interface{}{
									"workspace":            "",
									"default":              true,
									"channel":              "",
									"enabled":              false,
									"global":               true,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  false,
									"token":                false,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_private"},
								Options: map[string]interface{}{
									"workspace":            "company_private",
									"channel":              "#general",
									"default":              false,
									"enabled":              true,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                true,
									"username":             "kapacitor",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
							{
								Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
								Options: map[string]interface{}{
									"workspace":            "company_public",
									"channel":              "#general",
									"default":              false,
									"enabled":              false,
									"global":               false,
									"icon-emoji":           "",
									"state-changes-only":   false,
									"url":                  true,
									"token":                false,
									"username":             "",
									"ssl-ca":               "",
									"ssl-cert":             "",
									"ssl-key":              "",
									"insecure-skip-verify": false,
								},
								Redacted: []string{
									"url",
									"token",
								},
							},
						},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/slack/company_public"},
						Options: map[string]interface{}{
							"workspace":            "company_public",
							"channel":              "#general",
							"default":              false,
							"enabled":              false,
							"global":               false,
							"icon-emoji":           "",
							"state-changes-only":   false,
							"url":                  true,
							"token":                false,
							"username":             "",
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: []string{
							"url",
							"token",
						},
					},
				},
			},
		},
		{
			section: "snmptrap",
			setDefaults: func(c *server.Config) {
				c.SNMPTrap.Community = "test"
				c.SNMPTrap.Retries = 2.0
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
					Options: map[string]interface{}{
						"addr":      "localhost:162",
						"enabled":   false,
						"community": true,
						"retries":   2.0,
					},
					Redacted: []string{
						"community",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
				Options: map[string]interface{}{
					"addr":      "localhost:162",
					"enabled":   false,
					"community": true,
					"retries":   2.0,
				},
				Redacted: []string{
					"community",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":   true,
							"addr":      "snmptrap.example.com:162",
							"community": "public",
							"retries":   1.0,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
							Options: map[string]interface{}{
								"addr":      "snmptrap.example.com:162",
								"enabled":   true,
								"community": true,
								"retries":   1.0,
							},
							Redacted: []string{
								"community",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/snmptrap/"},
						Options: map[string]interface{}{
							"addr":      "snmptrap.example.com:162",
							"enabled":   true,
							"community": true,
							"retries":   1.0,
						},
						Redacted: []string{
							"community",
						},
					},
				},
			},
		},
		{
			section: "swarm",
			setDefaults: func(c *server.Config) {
				c.Swarm = swarm.Configs{swarm.Config{
					Servers: []string{"http://localhost:80001"},
				}}
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
					Options: map[string]interface{}{
						"id":                   "",
						"enabled":              false,
						"servers":              []interface{}{"http://localhost:80001"},
						"ssl-ca":               "",
						"ssl-cert":             "",
						"ssl-key":              "",
						"insecure-skip-verify": false,
					},
					Redacted: nil,
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
				Options: map[string]interface{}{
					"id":                   "",
					"enabled":              false,
					"servers":              []interface{}{"http://localhost:80001"},
					"ssl-ca":               "",
					"ssl-cert":             "",
					"ssl-key":              "",
					"insecure-skip-verify": false,
				},
				Redacted: nil,
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
							Options: map[string]interface{}{
								"id":                   "",
								"enabled":              true,
								"servers":              []interface{}{"http://localhost:80001"},
								"ssl-ca":               "",
								"ssl-cert":             "",
								"ssl-key":              "",
								"insecure-skip-verify": false,
							},
							Redacted: nil,
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/swarm/"},
						Options: map[string]interface{}{
							"id":                   "",
							"enabled":              true,
							"servers":              []interface{}{"http://localhost:80001"},
							"ssl-ca":               "",
							"ssl-cert":             "",
							"ssl-key":              "",
							"insecure-skip-verify": false,
						},
						Redacted: nil,
					},
				},
			},
		},
		{
			section: "talk",
			setDefaults: func(c *server.Config) {
				c.Talk.AuthorName = "Kapacitor"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
					Options: map[string]interface{}{
						"enabled":     false,
						"url":         false,
						"author_name": "Kapacitor",
					},
					Redacted: []string{
						"url",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
				Options: map[string]interface{}{
					"enabled":     false,
					"url":         false,
					"author_name": "Kapacitor",
				},
				Redacted: []string{
					"url",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
							"url":     "http://talk.example.com/secret-token",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
							Options: map[string]interface{}{
								"enabled":     true,
								"url":         true,
								"author_name": "Kapacitor",
							},
							Redacted: []string{
								"url",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/talk/"},
						Options: map[string]interface{}{
							"enabled":     true,
							"url":         true,
							"author_name": "Kapacitor",
						},
						Redacted: []string{
							"url",
						},
					},
				},
			},
		},
		{
			section: "teams",
			setDefaults: func(c *server.Config) {
				c.Teams.ChannelURL = "http://teams.example.com/abcde"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"global":             false,
						"state-changes-only": false,
						"channel-url":        "http://teams.example.com/abcde",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"global":             false,
					"state-changes-only": false,
					"channel-url":        "http://teams.example.com/abcde",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"global":             true,
							"state-changes-only": true,
							"channel-url":        "http://teams.example.com/12345",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams/"},
							Options: map[string]interface{}{
								"enabled":            false,
								"global":             true,
								"state-changes-only": true,
								"channel-url":        "http://teams.example.com/12345",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/teams/"},
						Options: map[string]interface{}{
							"enabled":            false,
							"global":             true,
							"state-changes-only": true,
							"channel-url":        "http://teams.example.com/12345",
						},
					},
				},
			},
		},
		{
			section: "telegram",
			setDefaults: func(c *server.Config) {
				c.Telegram.ChatId = "kapacitor"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
					Options: map[string]interface{}{
						"chat-id":                  "kapacitor",
						"disable-notification":     false,
						"disable-web-page-preview": false,
						"enabled":                  false,
						"global":                   false,
						"parse-mode":               "",
						"state-changes-only":       false,
						"token":                    false,
						"url":                      telegram.DefaultTelegramURL,
					},
					Redacted: []string{
						"token",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
				Options: map[string]interface{}{
					"chat-id":                  "kapacitor",
					"disable-notification":     false,
					"disable-web-page-preview": false,
					"enabled":                  false,
					"global":                   false,
					"parse-mode":               "",
					"state-changes-only":       false,
					"token":                    false,
					"url":                      telegram.DefaultTelegramURL,
				},
				Redacted: []string{
					"token",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled": true,
							"token":   "token",
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
							Options: map[string]interface{}{
								"chat-id":                  "kapacitor",
								"disable-notification":     false,
								"disable-web-page-preview": false,
								"enabled":                  true,
								"global":                   false,
								"parse-mode":               "",
								"state-changes-only":       false,
								"token":                    true,
								"url":                      telegram.DefaultTelegramURL,
							},
							Redacted: []string{
								"token",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/telegram/"},
						Options: map[string]interface{}{
							"chat-id":                  "kapacitor",
							"disable-notification":     false,
							"disable-web-page-preview": false,
							"enabled":                  true,
							"global":                   false,
							"parse-mode":               "",
							"state-changes-only":       false,
							"token":                    true,
							"url":                      telegram.DefaultTelegramURL,
						},
						Redacted: []string{
							"token",
						},
					},
				},
			},
		},
		{
			section: "victorops",
			setDefaults: func(c *server.Config) {
				c.VictorOps.RoutingKey = "test"
				c.VictorOps.APIKey = "secret"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
					Options: map[string]interface{}{
						"api-key":     true,
						"enabled":     false,
						"global":      false,
						"routing-key": "test",
						"url":         victorops.DefaultVictorOpsAPIURL,
						"json-data":   false,
					},
					Redacted: []string{
						"api-key",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
				Options: map[string]interface{}{
					"api-key":     true,
					"enabled":     false,
					"global":      false,
					"routing-key": "test",
					"url":         victorops.DefaultVictorOpsAPIURL,
					"json-data":   false,
				},
				Redacted: []string{
					"api-key",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"api-key":   "",
							"global":    true,
							"json-data": true,
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
							Options: map[string]interface{}{
								"api-key":     false,
								"enabled":     false,
								"global":      true,
								"routing-key": "test",
								"url":         victorops.DefaultVictorOpsAPIURL,
								"json-data":   true,
							},
							Redacted: []string{
								"api-key",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/victorops/"},
						Options: map[string]interface{}{
							"api-key":     false,
							"enabled":     false,
							"global":      true,
							"routing-key": "test",
							"url":         victorops.DefaultVictorOpsAPIURL,
							"json-data":   true,
						},
						Redacted: []string{
							"api-key",
						},
					},
				},
			},
		},
		{
			section: "zenoss",
			setDefaults: func(c *server.Config) {
				c.Zenoss.URL = "https://tenant.zenoss.io:8080/zport/dmd/evconsole_router"
				c.Zenoss.Collector = "Kapacitor"
			},
			expDefaultSection: client.ConfigSection{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss"},
				Elements: []client.ConfigElement{{
					Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss/"},
					Options: map[string]interface{}{
						"enabled":            false,
						"global":             false,
						"state-changes-only": false,
						"url":                "https://tenant.zenoss.io:8080/zport/dmd/evconsole_router",
						"username":           "",
						"password":           false,
						"action":             "EventsRouter",
						"method":             "add_event",
						"type":               "rpc",
						"tid":                float64(1),
						"collector":          "Kapacitor",
						"severity-map": map[string]interface{}{
							"ok": "Clear", "info": "Info", "warning": "Warning", "critical": "Critical",
						},
					},
					Redacted: []string{
						"password",
					},
				}},
			},
			expDefaultElement: client.ConfigElement{
				Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss/"},
				Options: map[string]interface{}{
					"enabled":            false,
					"global":             false,
					"state-changes-only": false,
					"url":                "https://tenant.zenoss.io:8080/zport/dmd/evconsole_router",
					"username":           "",
					"password":           false,
					"action":             "EventsRouter",
					"method":             "add_event",
					"type":               "rpc",
					"tid":                float64(1),
					"collector":          "Kapacitor",
					"severity-map": map[string]interface{}{
						"ok": "Clear", "info": "Info", "warning": "Warning", "critical": "Critical",
					},
				},
				Redacted: []string{
					"password",
				},
			},
			updates: []updateAction{
				{
					updateAction: client.ConfigUpdateAction{
						Set: map[string]interface{}{
							"enabled":  true,
							"url":      "https://dev12345.zenoss.io:8080/zport/dmd/evconsole_router",
							"username": "dev",
							"password": "12345",
							"action":   "ScriptsRouter",
							"method":   "kapa_handler",
							"severity-map": zenoss.SeverityMap{
								OK: 0, Info: 2, Warning: 3, Critical: 5,
							},
						},
					},
					expSection: client.ConfigSection{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss"},
						Elements: []client.ConfigElement{{
							Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss/"},
							Options: map[string]interface{}{
								"enabled":            true,
								"global":             false,
								"state-changes-only": false,
								"url":                "https://dev12345.zenoss.io:8080/zport/dmd/evconsole_router",
								"username":           "dev",
								"password":           true,
								"action":             "ScriptsRouter",
								"method":             "kapa_handler",
								"type":               "rpc",
								"tid":                float64(1),
								"collector":          "Kapacitor",
								"severity-map": map[string]interface{}{
									"ok": float64(0), "info": float64(2), "warning": float64(3), "critical": float64(5),
								},
							},
							Redacted: []string{
								"password",
							},
						}},
					},
					expElement: client.ConfigElement{
						Link: client.Link{Relation: client.Self, Href: "/kapacitor/v1/config/zenoss/"},
						Options: map[string]interface{}{
							"enabled":            true,
							"global":             false,
							"state-changes-only": false,
							"url":                "https://dev12345.zenoss.io:8080/zport/dmd/evconsole_router",
							"username":           "dev",
							"password":           true,
							"action":             "ScriptsRouter",
							"method":             "kapa_handler",
							"type":               "rpc",
							"tid":                float64(1),
							"collector":          "Kapacitor",
							"severity-map": map[string]interface{}{
								"ok": float64(0), "info": float64(2), "warning": float64(3), "critical": float64(5),
							},
						},
						Redacted: []string{
							"password",
						},
					},
				},
			},
		},
	}

	compareElements := func(got, exp client.ConfigElement) error {
		if got.Link != exp.Link {
			return fmt.Errorf("elements have different links, got %v exp %v", got.Link, exp.Link)
		}
		if !cmp.Equal(exp.Options, got.Options) {
			return fmt.Errorf("unexpected config option difference \n %s", cmp.Diff(exp.Options, got.Options))
		}
		if len(got.Redacted) != len(exp.Redacted) {
			return fmt.Errorf("unexpected element redacted lists: %s, \n%s", got.Redacted, cmp.Diff(got.Redacted, exp.Redacted))
		}
		sort.Strings(got.Redacted)
		sort.Strings(exp.Redacted)
		for i := range exp.Redacted {
			if got.Redacted[i] != exp.Redacted[i] {
				return fmt.Errorf("unexpected element redacted lists: %s, \n%s", got.Redacted, cmp.Diff(got.Redacted, exp.Redacted))
			}
		}
		return nil
	}
	compareSections := func(got, exp client.ConfigSection) error {
		if got.Link != exp.Link {
			return fmt.Errorf("sections have different links, got %v exp %v", got.Link, exp.Link)
		}
		if len(got.Elements) != len(exp.Elements) {
			return fmt.Errorf("sections are different lengths, got %d exp %d", len(got.Elements), len(exp.Elements))
		}
		for i := range exp.Elements {
			if err := compareElements(got.Elements[i], exp.Elements[i]); err != nil {
				return errors.Wrapf(err, "section element %d are not equal", i)
			}
		}
		return nil
	}

	validate := func(
		cli *client.Client,
		section,
		element string,
		expSection client.ConfigSection,
		expElement client.ConfigElement,
	) error {
		// Get all sections
		if config, err := cli.ConfigSections(); err != nil {
			return errors.Wrap(err, "failed to get sections")
		} else {
			if err := compareSections(config.Sections[section], expSection); err != nil {
				return fmt.Errorf("%s: %v", section, err)
			}
		}
		// Get the specific section
		sectionLink := cli.ConfigSectionLink(section)
		if got, err := cli.ConfigSection(sectionLink); err != nil {
			return err
		} else {
			if err := compareSections(got, expSection); err != nil {
				return fmt.Errorf("%s: %v", section, err)
			}
		}
		elementLink := cli.ConfigElementLink(section, element)
		// Get the specific element
		if got, err := cli.ConfigElement(elementLink); err != nil {
			return err
		} else {
			if err := compareElements(got, expElement); err != nil {
				return fmt.Errorf("%s/%s: %v", section, element, err)
			}
		}
		return nil
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s-%d", tc.section, tc.element, i), func(t *testing.T) {
			// Create default config
			c := NewConfig()
			if tc.setDefaults != nil {
				tc.setDefaults(c)
			}
			s := OpenServer(c)
			cli := Client(s)
			defer s.Close()

			if err := validate(cli, tc.section, tc.element, tc.expDefaultSection, tc.expDefaultElement); err != nil {
				t.Errorf("unexpected defaults for %s/%s: %v", tc.section, tc.element, err)
			}

			for i, ua := range tc.updates {
				t.Run(ua.name, func(t *testing.T) {
					link := cli.ConfigElementLink(tc.section, ua.element)

					if len(ua.updateAction.Add) > 0 ||
						len(ua.updateAction.Remove) > 0 {
						link = cli.ConfigSectionLink(tc.section)
					}

					if err := cli.ConfigUpdate(link, ua.updateAction); err != nil && (tc.expErr == nil || (tc.expErr.Error() != err.Error())) {
						t.Errorf("unexepected result from update got:%q expected:%q", err, tc.expErr)
					} else if err == nil && tc.expErr != nil {
						t.Error("did not recieve expected error from update")
					}
					if err := validate(cli, tc.section, ua.element, ua.expSection, ua.expElement); err != nil {
						t.Errorf("unexpected update result %d for %s/%s: %v", i, tc.section, ua.element, err)
					}
				})
			}
		})
	}
}
