package client

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
)

const (
	apiBasePath = "/apis"

	extensionsPath          = apiBasePath + "/extensions/v1beta1"
	extensionsNamespacePath = extensionsPath + "/namespaces"
	scaleEndpoint           = "/scale"

	// Secrets
	secretsPath     = "/var/run/secrets/kubernetes.io/serviceaccount"
	namespaceSecret = "namespace"
	tokenSecret     = "token"
	caCertSecret    = "ca.crt"
)

type Config struct {
	URLs      []string
	Namespace string
	Token     string
	TLSConfig *tls.Config
}

// loadPodSecret returns the string value for a given secret.
// The secret is expected to be stored under secretsPath.
func loadPodSecret(secret string) (value []byte, err error) {
	p := filepath.Join(secretsPath, secret)
	value, err = ioutil.ReadFile(p)
	return
}

type Client interface {
	// Scales returns an interface for interactive with Scale resources.
	// If namespace is empty the default client namespace will be used.
	Scales(namespace string) ScalesInterface
}

// httpClient is a lightweight HTTP client for k8s resources.
// It emulates the same structure as the package k8s.io/client-go/
// so as to make replacing this client with an official client simpler
// once https://github.com/kubernetes/kubernetes/issues/5660 is fixed
type httpClient struct {
	mu         sync.Mutex
	urls       []*url.URL
	namespace  string
	client     *http.Client
	current    int
	authHeader string
}

func NewConfigInCluster() (Config, error) {
	// Create a config based off the expected config from within a pod.
	namespaceBytes, err := loadPodSecret(namespaceSecret)
	if err != nil {
		return Config{}, errors.Wrap(err, "could not load namespace")
	}
	tokenBytes, err := loadPodSecret(tokenSecret)
	if err != nil {
		return Config{}, errors.Wrap(err, "could not load token")
	}

	caCert, err := loadPodSecret(caCertSecret)
	if err != nil {
		return Config{}, errors.Wrap(err, "could not load ca.crt")
	}
	// Construct TLSConfig from caCert
	t := &tls.Config{}
	caCertPool := x509.NewCertPool()
	successful := caCertPool.AppendCertsFromPEM(caCert)
	if !successful {
		return Config{}, errors.New("failed to parse ca certificate as PEM encoded content")
	}
	t.RootCAs = caCertPool

	config := Config{
		URLs:      []string{"https://kubernetes"},
		Namespace: string(namespaceBytes),
		Token:     string(tokenBytes),
		TLSConfig: t,
	}
	return config, nil
}

func NewInCluster() (Client, error) {
	config, err := NewConfigInCluster()
	if err != nil {
		return nil, err
	}
	return New(config)
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
	tr := &http.Transport{
		TLSClientConfig: c.TLSConfig,
	}
	return &httpClient{
		urls: urls,
		client: &http.Client{
			Transport: tr,
		},
		namespace:  c.Namespace,
		authHeader: fmt.Sprintf("Bearer %s", c.Token),
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
	r.Header.Set("Authorization", c.authHeader)
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
	if namespace == "" {
		if c.namespace != "" {
			namespace = c.namespace
		} else {
			namespace = NamespaceDefault
		}
	}
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
