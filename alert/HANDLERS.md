# Alert Handlers

This document lays out how to implement an alert handler within Kapacitor.

## Components of a Handler

The Handler interface in this package is simple:

```go
type Handler interface {
	// Handle is responsible for taking action on the event.
	Handle(event Event)
}
```

In order to implement a handler you must implement the above interface.
But there is much more to a handler beyond its implementation.
A complete handler implementation needs to provide several components, listed below:

* An implementation of the Handler interface
* A service for creating instances of the handler implementation.
* A configuration struct for configuring the service via configuration files.
* A definition struct for how a handler is defined via a TICKscript
* A configuration struct for how a handler is defined via the HTTP API.
* A test options struct for testing the handler.

Most of these components are defined in a single package named after the handler under the `services` parent package.

## Example

Let's walk through writing a simple example handler for the `Foo` alerting service.
The Foo service is a simple chat room application.
Messages can be sent to a specific room via an HTTP API.

### The Foo Service

First steps are to create a package where most of the implementation will live.
Create a directory relative to the root of the Kapacitor repo named `services/foo`.

Next create a file for the configuration of the service named `services/foo/config.go`.
In the file create a struct for now named `Config`.

```go
package foo

import "errors"

// Config declares the needed configuration options for the service Foo.
type Config struct {
	// Enabled indicates whether the service should be enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// URL of the Foo server.
	URL string `toml:"url" override:"url"`
	// Room specifies the default room to use for all handlers.
	Room string `toml:"room" override:"room"`
}

func NewConfig() Config {
	return Config{}
}

func (c Config) Validate() error {
	if c.Enabled && c.URL == "" {
		return errors.New("must specify the Foo server URL")
	}
	return nil
}
```

The two field tags `toml` and `override` are used for fields of the Config structure to allow the structure to be decoded from a toml file and overriden using the Kapacitor config HTTP API.
You will see that many structs make use of field tags, do not worry to much about how they are implemented at the moment.

Create a file for the service implementation named `services/foo/service.go`.
A service is a type in go that can be opened, closed and updated; while providing whatever other API is needed for the service.

Place the skeleton service type and method below in the service.go file.

```go
package foo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/influxdata/kapacitor/alert"
)

type Service struct {
	configValue atomic.Value
	logger      *log.Logger
}

func NewService(c Config, l *log.Logger) *Service {
	s := &Service{
		logger: l,
	}
	s.configValue.Store(c)
	return s
}

func (s *Service) Open() error {
	// Perform any initialization needed here
	return nil
}

func (s *Service) Close() error {
	// Perform any actions needed to properly close the service here.
	// For example signal and wait for all go routines to finish.
	return nil
}

func (s *Service) Update(newConfig []interface{}) error {
	if l := len(newConfig); l != 1 {
		return fmt.Errorf("expected only one new config object, got %d", l)
	}
	if c, ok := newConfig[0].(Config); !ok {
		return fmt.Errorf("expected config object to be of type %T, got %T", c, newConfig[0])
	} else {
		s.configValue.Store(c)
	}
	return nil
}

// config loads the config struct stored in the configValue field.
func (s *Service) config() Config {
	return s.configValue.Load().(Config)
}

// Alert sends a message to the specified room.
func (s *Service) Alert(room, message string) error {
	c := s.config()
	if !c.Enabled {
		return errors.New("service is not enabled")
	}
	type postMessage struct {
		Room    string `json:"room"`
		Message string `json:"message"`
	}
	data, err := json.Marshal(postMessage{
		Room:    room,
		Message: message,
	})
	if err != nil {
		return err
	}
	r, err := http.Post(c.URL, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response code %d from Foo service", r.StatusCode)
	}
	return nil
}
```


At this point we have a minimal Foo service that can be used to send message to a Foo server.
Since the intent of our service is to provide an alert Handler we need to define a method for creating new handlers.
A good practice is to define a struct that contains all needed information to create a new handler.
Then provide a method on the service that accepts that configuration struct and returns a handler.

Add the following snippet to the `service.go` file.

```go
type HandlerConfig struct {
	//Room specifies the destination room for the chat messages.
	Room string `mapstructure:"room"`
}

// handler provides the implementation of the alert.Handler interface for the Foo service.
type handler struct {
	s      *Service
	c      HandlerConfig
	logger *log.Logger
}

// DefaultHandlerConfig returns a HandlerConfig struct with defaults applied.
func (s *Service) DefaultHandlerConfig() HandlerConfig {
	// return a handler config populated with the default room from the service config.
	c := s.config()
	return HandlerConfig{
		Room: c.Room,
	}
}

// Handler creates a handler from the config.
func (s *Service) Handler(c HandlerConfig, l *log.Logger) alert.Handler {
	// handlers can operate in differing contexts, as a result a logger is passed
	// in so that logs from this handler can be correctly associatied with a given context.
	return &handler{
		s:      s,
		c:      c,
		logger: l,
	}
}
```

Finally we need to implement the Handler interface on the handler type.
Add the following snippet to the `service.go` file.

```go
// Handle takes an event and posts its message to the Foo service chat room.
func (h *handler) Handle(event alert.Event) {
	if err := h.s.Alert(h.c.Room, event.State.Message); err != nil {
		h.logger.Println("E! failed to handle event", err)
	}
}
```

At this point the Foo service can post message to a Foo server.
The last bit required to complete the service is to enable the service to be tested dynamically by a user.
Kapacitor contains a service-tests API endpoint that enables a user to perform basic "hello world" tests against a service.
To leverage this system we need to implement a few methods on the service.

Add the following snippet to the `service.go` file.

```go
type testOptions struct {
	Room    string `json:"room"`
	Message string `json:"message"`
}

func (s *Service) TestOptions() interface{} {
	c := s.config()
	return &testOptions{
		Room: c.Room,
		Message: "test foo message",
	}
}

func (s *Service) Test(o interface{}) error {
	options, ok := o.(*testOptions)
	if !ok {
		return fmt.Errorf("unexpected options type %T", options)
	}
	return s.Alert(options.Room, options.Message)
}
```

With that we have a functioning Foo service that can be configured, tested and  consumed as an alert handler.
Now we need to let the rest of the Kapacitor code know that our service exsists.

### Integrating the service

There are a few integration points that need to be addressed:

* The Kapacitor server needs to know about the service.
* The TICKscript syntax needs to know how to define a Foo handler.
* The alert node needs to know how to create a handler from the TICKscript definition.
* The alert service needs to know how to create a handler from the HTTP API.

We will address this integration points one at a time.

#### Server

To tell the Kapacitor server about our service we need to first add its config to the main server configuration struct.
In the alert handlers section of the Config struct in `server/config.go` add the following line.

```
Foo foo.Config `toml:"foo" override:"foo"`
```

In the `NewConfig` function in `server/config.go` add the following line.

```
c.Foo = foo.NewConfig()
```

In the `Validate()` method add the following lines.

```
if err := c.Foo.Validate(); err != nil {
    return err
}
```


That should do it for the configuration integration.
Next we need to add the Foo service to the list of services.
In the file `server/server.go` in the `NewServer` method add the following line after the existing alert handlers.

```
s.appendFooService()
```

Then later in the file define the `appendFooService` method.

```go
func (s *Server) appendFooService() {
	c := s.config.Foo
	l := s.LogService.NewLogger("[foo] ", log.LstdFlags)
	srv := foo.NewService(c, l)

	s.TaskMaster.FooService = srv
	s.AlertService.FooService = srv

	s.SetDynamicService("foo", srv)
	s.AppendService("foo", srv)
}
```

You may have noticed that we set the `FooService` field value in the above method.
Let's define those fields in the `TaskMaster` and `AlertService` types.
In `task_master.go` add the following line after the other services.

```
FooService interface {
	DefaultHandlerConfig() foo.HandlerConfig
	Handler(foo.HandlerConfig, *log.Logger) alert.Handler
}
```

And update the `New` method to copy over your service.

```
n.FooService = tm.FooService
```

In `services/alert/service.go` add the following lines after the other serivces.

```
FooService interface {
	DefaultHandlerConfig() foo.HandlerConfig
	Handler(foo.HandlerConfig, *log.Logger) alert.Handler
}
```


With those additions the server now knows about the Foo service and will start it up during Kapacitor's startup procedure.

#### TICKscript

In order for your handler to be defined in TICKscripts we need to define a new `foo` property on the alert node.
In `pipeline/alert.go` a description of your service to the comment and add these line to the AlertNode struct after the other handlers.


```
// Send alert to Foo.
// tick:ignore
FooHandlers []*FooHandler `tick:"Foo"`
```

Add these lines later on in the file `pipeline/alert.go`:

```
// Send alert to a Foo server.
// tick:property
func (a *AlertNode) Foo() *FooHandler {
	f := &FooHandler{
		AlertNode: a,
	}
	a.FooHandlers = append(a.FooHandlers, f)
	return f
}

// tick:embedded:AlertNode.Foo
type FooHandler struct {
	*AlertNode

	// The room for the messages.
	// Defaults to the room in the configuration if empty.
	Room string
}
```

With those types in TICKscript you can now do the following:

```go
|alert()
    .foo()
        .room('alerts')
```

#### Alert Node

Now we need to instruct the alert node on how to create a Foo handler from the TICKscript definition.

In file `alert.go` in the `newAlertNode` function after the other handlers add these lines:

```go
for _, f := range n.FooHandlers {
	c := et.tm.FooService.DefaultHandlerConfig()
	if f.Room != "" {
		c.Room = f.Room
	}
	h := et.tm.FooService.Handler(c, l)
	an.handlers = append(an.handlers, h)
}
```

#### Alert Service

In addition to TICKscript a user can use the Kapacitor HTTP API to define alert handlers.
We need to define a mapping in the alert service for the new Foo handler.

In the file `services/alert/service.go` add the following case to the switch statement in the `createHandlerActionFromSpec` method.

```
case "foo":
	c := s.FooService.DefaultHandlerConfig()
	err = decodeOptions(spec.Options, &c)
	if err != nil {
		return
	}
	h := s.FooService.Handler(c, s.logger)
	ha = newPassThroughHandler(h)
```


With that we are done!

### Example Branch

That is a lot to get right and there are quite a few touch points.
To make getting all the boiler plate code in place easier we have an example branch and PR up that contains all of the changes laid out in the document.

You can find the PR [here](https://github.com/influxdata/kapacitor/pull/1107) and the branch [here](https://github.com/influxdata/kapacitor/tree/example-alert-handler).
Feel free to checkout the branch and rebase to jump start your contribution.

### Tests

In all of this we haven't mentioned tests.
A new service will need to be tested before it is merged into master.
There are relevant tests for all aspects we have touched on.
There are two locations where the behavior of an alert handler service is verified.

* Integration tests can be found in the `integrations` package. Tests the specifics of using the service.
* End to end tests are found in the `server/server_test.go` file. Tests that the service has been properly integrated with the server.


For completeness the example branch does contain tests.


