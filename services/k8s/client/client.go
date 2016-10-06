package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sync"

	"github.com/pkg/errors"
)

const (
	apiBasePath = "/apis"

	extensionsPath          = apiBasePath + "/extensions/v1beta1"
	extensionsNamespacePath = extensionsPath + "/namespaces"
	scaleEndpoint           = "/scale"
)

type Config struct {
	URLs []string
}

type Client interface {
	Scales(namespace string) ScalesInterface
}

// httpClient is a lightweight HTTP client for k8s resources.
// It emulates the same structure as the package k8s.io/client-go/
// so as to make replacing this client with an official client simpler
// once https://github.com/kubernetes/kubernetes/issues/5660 is fixed
type httpClient struct {
	mu      sync.Mutex
	urls    []*url.URL
	client  *http.Client
	current int
}

func New(c Config) (Client, error) {
	if len(c.URLs) == 0 {
		return nil, fmt.Errorf("must provide at least one URL")
	}
	urls := make([]*url.URL, len(c.URLs))
	for i := range c.URLs {
		u, err := url.Parse(c.URLs[i])
		if err != nil {
			return nil, errors.Wrapf(err, "invalid url %q", c.URLs[i])
		}
		urls[i] = u
	}
	return &httpClient{
		urls:   urls,
		client: http.DefaultClient,
	}, nil
}

func (c *httpClient) nextURL() *url.URL {
	c.mu.Lock()
	u := c.urls[c.current]
	c.current = (c.current + 1) % len(c.urls)
	c.mu.Unlock()
	return u
}

func (c *httpClient) Do(r http.Request) (*http.Response, error) {
	u := c.nextURL()
	r.URL.Host = u.Host
	r.URL.Scheme = u.Scheme
	resp, err := c.client.Do(&r)
	return resp, errors.Wrap(err, "k8s client request failed")
}

func (c *httpClient) Get(p string, response interface{}, successfulCodes ...int) error {
	r, err := http.NewRequest("GET", p, nil)
	if err != nil {
		return errors.Wrapf(err, "failed create GET request for %q", p)
	}
	resp, err := c.Do(*r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, response, successfulCodes)
}

func (c *httpClient) Patch(p string, patch JSONPatch, successfulCodes ...int) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	err := enc.Encode([]JSONPatch{patch})
	if err != nil {
		return errors.Wrap(err, "failed to json encode patch")
	}
	r, err := http.NewRequest("PATCH", p, &buf)
	if err != nil {
		return errors.Wrapf(err, "failed create PATCH request for %q", p)
	}
	r.Header.Set("Content-Type", "application/json-patch+json")
	resp, err := c.Do(*r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.decodeResponse(resp, nil, successfulCodes)
}

func (c *httpClient) decodeResponse(resp *http.Response, response interface{}, successfulCodes []int) error {
	dec := json.NewDecoder(resp.Body)
	successful := false
	for _, code := range successfulCodes {
		if code == resp.StatusCode {
			successful = true
			break
		}
	}
	// Unsuccessful response code, decode status result
	if !successful {
		var status Status
		err := dec.Decode(&status)
		if err != nil {
			return errors.Wrapf(err, "failed to understand k8s server response: Code: %d", resp.StatusCode)
		}
		return status
	}
	if response != nil {
		// Decode response body into provided response object
		if err := dec.Decode(response); err != nil {
			return errors.Wrapf(err, "failed to decode k8s server response into %T: Code: %d", response, resp.StatusCode)
		}
	}
	return nil
}

type ScalesInterface interface {
	Get(kind, name string) (*Scale, error)
	Update(kind string, scale *Scale) error
}

type Scales struct {
	c         *httpClient
	namespace string
}

func (c *httpClient) Scales(namespace string) ScalesInterface {
	return Scales{c: c, namespace: namespace}
}

func (s Scales) Get(kind, name string) (*Scale, error) {
	p := path.Join(extensionsNamespacePath, s.namespace, string(kind), name, scaleEndpoint)
	scale := &Scale{}
	err := s.c.Get(p, scale, http.StatusOK)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get scale %s/%s/%s", s.namespace, kind, name)
	}
	return scale, nil
}

func (s Scales) Update(kind string, scale *Scale) error {
	patch := JSONPatch{
		Operation: "replace",
		Path:      "/spec/replicas",
		Value:     scale.Spec.Replicas,
	}
	err := s.c.Patch(scale.SelfLink, patch, http.StatusOK)
	if err != nil {
		return errors.Wrapf(err, "failed to update scale %s/%s/%s", s.namespace, kind, scale.Name)
	}
	return nil
}
