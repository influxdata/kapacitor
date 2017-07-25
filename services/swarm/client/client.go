package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/swarm"
	"github.com/pkg/errors"
)

// Version is the Docker Engine API version needed for this package.
// Since docker versions their API this can be a constant value and newer docker daemons should still support this version once it becomes older.
// If at some point Docker removes this version then we will need to update this code.
const version = "v1.30"

type Config struct {
	URLs      []string
	TLSConfig *tls.Config
}

type Client interface {
	Update(c Config) error
	Version() (string, error)
	Service(name string) (*swarm.Service, error)
	UpdateService(service *swarm.Service) error
}

type httpClient struct {
	mu     sync.RWMutex
	config Config
	urls   []url.URL
	client *http.Client
	index  int32
}

func New(c Config) (Client, error) {
	urls, err := parseURLs(c.URLs)
	if err != nil {
		return nil, err
	}
	return &httpClient{
		config: c,
		urls:   urls,
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: c.TLSConfig,
			},
		},
	}, nil
}

func (c *httpClient) pickURL(urls []url.URL) url.URL {
	i := atomic.LoadInt32(&c.index)
	i = (i + 1) % int32(len(urls))
	atomic.StoreInt32(&c.index, i)
	return urls[i]
}

func parseURLs(urlStrs []string) ([]url.URL, error) {
	urls := make([]url.URL, len(urlStrs))
	for i, urlStr := range urlStrs {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid url %q", urlStr)
		}
		urls[i] = *u
	}
	return urls, nil
}

func (c *httpClient) Update(new Config) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	old := c.config
	c.config = new
	urls, err := parseURLs(new.URLs)
	if err != nil {
		return err
	}
	c.urls = urls

	if old.TLSConfig != new.TLSConfig {
		c.client = &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: new.TLSConfig,
			},
		}
	}
	return nil
}

func (c *httpClient) Do(r http.Request) (*http.Response, error) {
	c.mu.RLock()
	u := c.pickURL(c.urls)
	client := c.client
	c.mu.RUnlock()

	r.URL.Host = u.Host
	r.URL.Scheme = u.Scheme

	resp, err := client.Do(&r)
	return resp, errors.Wrap(err, "swarm client request failed")
}

func (c *httpClient) decodeResponse(resp *http.Response, response interface{}, successfulCodes ...int) error {
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
		var e types.ErrorResponse
		err := dec.Decode(&e)
		if err != nil {
			return errors.Wrapf(err, "failed to understand swarm server response: Code: %d", resp.StatusCode)
		}
		if e.Message == "" {
			return fmt.Errorf("failed to understand swarm server error response: Code: %d", resp.StatusCode)
		}
		return errors.New(e.Message)
	}
	if response != nil {
		// Decode response body into provided response object
		if err := dec.Decode(response); err != nil {
			return errors.Wrapf(err, "failed to decode swarm server response into %T: Code: %d", response, resp.StatusCode)
		}
	}
	return nil
}
func (c *httpClient) Version() (string, error) {
	r, err := http.NewRequest("GET", "/info", nil)
	if err != nil {
		return "", err
	}

	resp, err := c.Do(*r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	info := new(types.Info)
	if err := c.decodeResponse(resp, info, http.StatusOK); err != nil {
		return "", err
	}
	return info.ServerVersion, nil
}
func (c *httpClient) Service(id string) (*swarm.Service, error) {
	p := path.Join("/", version, "services", id)
	r, err := http.NewRequest("GET", p, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed create GET request for %q", p)
	}

	resp, err := c.Do(*r)
	if err != nil {
		return nil, errors.Wrapf(err, "failed create GET request for %q", p)
	}
	defer resp.Body.Close()

	service := new(swarm.Service)
	if err := c.decodeResponse(resp, service, http.StatusOK); err != nil {
		return nil, err
	}
	return service, nil
}

func (c *httpClient) UpdateService(service *swarm.Service) error {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(service.Spec)
	if err != nil {
		return errors.Wrapf(err, "failed to json encode service %q", service.ID)
	}

	p := path.Join("/", version, "services", service.ID, "update")
	params := &url.Values{}
	params.Set("version", strconv.Itoa(int(service.Version.Index)))
	u := url.URL{
		Path:     p,
		RawQuery: params.Encode(),
	}
	log.Println("D!", buf.String())

	r, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return errors.Wrapf(err, "failed to create POST request for %q", service.ID)
	}

	resp, err := c.Do(*r)
	if err != nil {
		return errors.Wrapf(err, "update service request failed for %q", service.ID)
	}
	defer resp.Body.Close()

	return c.decodeResponse(resp, nil, http.StatusOK)
}
