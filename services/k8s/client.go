package k8s

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"path"

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

// Client is a lighweight HTTP client for k8s resources.
type Client struct {
	urls    []*url.URL
	client  *http.Client
	current int
}

func NewClient(c Config) (*Client, error) {
	urls := make([]*url.URL, len(c.URLs))
	for i := range c.URLs {
		u, err := url.Parse(c.URLs[i])
		if err != nil {
			return nil, errors.Wrapf(err, "invalid url %q", c.URLs[i])
		}
		urls[i] = u
	}
	return &Client{
		urls:   urls,
		client: http.DefaultClient,
	}, nil
}

func (c *Client) Do(r http.Request) (*http.Response, error) {
	u := c.urls[c.current]
	r.URL.Host = u.Host
	r.URL.Scheme = u.Scheme
	log.Println("D! request:", r)
	c.current = (c.current + 1) % len(c.urls)
	resp, err := c.client.Do(&r)
	return resp, errors.Wrap(err, "k8s client request failed")
}

func (c *Client) Get(p string, response interface{}, successfulCodes ...int) error {
	log.Println("D! GET", p)
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

func (c *Client) Patch(p string, patch JSONPatch, successfulCodes ...int) error {
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

func (c *Client) decodeResponse(resp *http.Response, response interface{}, successfulCodes []int) error {
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
	Get(kind Kind, name string) (*Scale, error)
	Update(kind Kind, scale *Scale) error
}

type Scales struct {
	c         *Client
	namespace string
}

func (c *Client) Scales(namespace string) ScalesInterface {
	return Scales{c: c, namespace: namespace}
}

func (s Scales) Get(kind Kind, name string) (*Scale, error) {
	p := path.Join(extensionsNamespacePath, s.namespace, string(kind), name, scaleEndpoint)
	scale := &Scale{}
	err := s.c.Get(p, scale, http.StatusOK)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get scale %s/%s/%s", s.namespace, kind, name)
	}
	return scale, nil
}

func (s Scales) Update(kind Kind, scale *Scale) error {
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
