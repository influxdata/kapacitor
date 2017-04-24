# Usage Client [![GoDoc](https://godoc.org/github.com/influxdb/usage-client/v1?status.svg)](https://godoc.org/github.com/influxdb/usage-client/v1)

The `usage-client` package is used to speak with the Usage API in a simple and straight forward way. No muss, no fuss!

## V1

### Installation

```go
$ go get github.com/influxdb/usage-client/v1
```

### An Important Developers Note

When testing and developing your applications to work with the Usage API, please make sure to change the `client.URL` to be something other than it's default, which is production. Might I recommend the following:

```go
client.URL = "https://usage.staging.influxdata.com"
```

More info can be found in the documentation [here](https://godoc.org/github.com/influxdb/usage-client/v1#pkg-variables).

__NOTE__: Applications should make this URL configurable.

## Registering a Customer

Customer's need to register with Enterprise and will receive a token in return. This token should be saved and sent along on all subsequent requests, more on this in a minute.

Before sending a customer to Enterprise to register you will need to generate a URL to give to that customer.

```go
c := client.New("")
r := client.Registration{
  ClusterID:   "clus1",
  Product:     "chronograf",
  RedirectURL: "http://example.com",
}

u, err := c.RegistrationURL(r)
```

If a `RedirectURL` was specified then the customer will be redirected to that URL after successfully completing their registration. There will also be a `token` query parameter attached to the URL, please retrieve and store this token for future use!

__NOTE__: If there is no token stored locally (it is up to each app to store this token), when the application starts up they user should be prompted to register with Enterprise.

## Registering a Server

When an app wakes up (influxdb, chronograf, etc…) it attempt to register itself with Enterprise:


```go
c := client.New("token-goes-here")

s := client.Server{
  ClusterID: "clus1",
  Host:      "example.com",
  Product:   "influxdb",
  Version:   "1.0",
  ServerID:  "serv1",
}

res, err := c.Save(s)
```

This should happen every time the application starts.

## Posting Product Stats:

Ideally when posting product stats data to Enterprise you will pass the "token". If no token is set then the data will __NOT__ be associated with any organization.

```go
c := client.New("token-goes-here")

st := client.Stats{
  ClusterID: "clus1",
  ServerID:  "serv1",
  Product:   "influxdb",
  Data: []client.StatsData{
    client.StatsData{
      Name: "engine",
      Tags: client.Tags{
        "path":    "/home/philip/.influxdb/data/_internal/monitor/1",
        "version": "bz1",
      },
      Values: client.Values{
        "blks_write":          39,
        "blks_write_bytes":    2421,
        "blks_write_bytes_c":  2202,
        "points_write":        39,
        "points_write_dedupe": 39,
      },
    },
  },
}

res, err := c.Save(st)
```

## Posting Usage Stats:

Usage stats are sent to Enterprise every 12 hours.

```go
c := client.New("token-goes-here")

u := client.Usage{
  Product: "influxdb",
  Data: []client.UsageData{
    {
      Tags: client.Tags{
        "version": "0.9.5",
        "arch":    "amd64",
        "os":      "linux",
      },
      Values: client.Values{
        "cluster_id":       "23423",
        "server_id":        "1",
        "num_databases":    3,
        "num_measurements": 2342,
        "num_series":       87232,
      },
    },
  },
}

res, err := c.Save(u)
```

## API Errors

As you probably know [Go doesn't return error codes for non-2xx HTTP requests](http://metabates.com/2015/10/15/handling-http-request-errors-in-go/), but thankfully the Enterprise Client does!

The API will return one of two errors back to you if there are any problems:

* `SimpleError` - This type of error is common for status codes such as `401`, `404`, and `500`. [https://godoc.org/github.com/influxdb/usage-client/v1#SimpleError](https://godoc.org/github.com/influxdb/usage-client/v1#SimpleError)
* `ValidationErrors` - Type type of error is common for status codes such as `422` and will return specific errors related to the validity of the payload sent to the API. [https://godoc.org/github.com/influxdb/usage-client/v1#ValidationErrors](https://godoc.org/github.com/influxdb/usage-client/v1#ValidationErrors)

It is recommended to check for these errors and handle them appropriately.
