package k8stest

import "github.com/influxdata/kapacitor/services/k8s/client"

type Client struct {
	ScalesGetFunc    func(kind, name string) (*client.Scale, error)
	ScalesUpdateFunc func(kind string, scale *client.Scale) error
}

// Client returns itself.
func (c Client) Client(string) (client.Client, error) {
	return c, nil
}
func (c Client) Scales(namespace string) client.ScalesInterface {
	return Scales{
		ScalesGetFunc:    c.ScalesGetFunc,
		ScalesUpdateFunc: c.ScalesUpdateFunc,
	}
}

func (c Client) Versions() (client.APIVersions, error) {
	return client.APIVersions{}, nil
}

func (Client) Update(client.Config) error {
	return nil
}

type Scales struct {
	ScalesGetFunc    func(kind, name string) (*client.Scale, error)
	ScalesUpdateFunc func(kind string, scale *client.Scale) error
}

func (s Scales) Get(kind, name string) (*client.Scale, error) {
	return s.ScalesGetFunc(kind, name)
}
func (s Scales) Update(kind string, scale *client.Scale) error {
	return s.ScalesUpdateFunc(kind, scale)
}
