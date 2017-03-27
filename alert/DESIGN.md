# Kapacitor Alerting

Kapacitor enables a user to define and trigger alerts.
Alerts can be sent to various backend handlers.

## Alerts vs Events

An alert is defined via an [AlertNode](https://docs.influxdata.com/kapacitor/latest/nodes/alert_node/) in a TICKscript.
Each alert generates multiple events.

## Topics

Each alert belongs to a `topic`, if no topic is specified an unique topic is generated for the alert.
A topic may contain multiple alerts, enabling you to group your alerts into various topics.


## Alert State

Kapacitor exposes the state of the alerts via topics in the HTTP API.
The maximum level of all events withing a topic as well as the state of each event within the topic can be queried.
See the API docs for more details.

## Two ways to setup alert handlers

There are two ways to setup handlers for your alerts in Kapacitor.
The first method is designed to be quick and easy to configure.
The second method takes a bit more setup but provides more control over the handlers.

### Direct Handlers

You can directly define handlers in TICKscript.
Doing so dynamically creates a topic and configures the defined handlers on the topic.

This method is useful if you already have a system that manages your alert events for you.


### Alert Events Subsystem

The alert event subsystem follows a publish/subscribe model giving you fine grained control over how alert events are handled.
This is where alert topics show their strength.
Alert publish events to their topics and handlers subscribe to the various topics.

The alert subsystem allows you to do various different actions with your alerts:

* Aggregate Alerts into a single alert containing summary information.
* Rate limit alerts
* Easily manage which handlers handle which alerts without modifying your Kapacitor tasks.


#### Using the Alert Event Subsystem

By specifying a `topic` for an alert, all events from the alert will be sent to that topic.

Example TICKscript:

```go
stream
    |from()
        .measurement('cpu')
    |window()
        .period(1m)
        .every(1m)
    |mean('usage')
    |alert()
        .topic('cpu')
        .warn(lambda: "mean" > 70)
        .crit(lambda: "mean" > 80)
        // Send this alert directly to slack.
        .slack()
```

Then alert handlers can be configured to subscribe to the events.
These alert handlers will be configured via the API.
Use yaml/json to define the alert handlers.

Here are a few examples:

```yaml
id: my_handler
kind: pagerDuty
options:
  serviceKey: XXX
```

```yaml
id: aggregate_by_1m
kind: aggregate
options:
  interval: 1m
  topic: aggregated
```

```yaml
id: publish_to_system
kind: publish
options:
  topics: [ system ]
```

```json
{
    "id": "my_handler",
    "kind": "pagerDuty",
    "options": {
      "serviceKey": "XXX"
    }
}
```

```json
{
    "id": "aggregate_by_1m",
    "kind": "aggregate",
    "options": {
      "interval": "1m"
    }
}
```

```json
{
    "id": "publish_to_system",
    "kind": "publish",
    "options": {
      "topics": ["system"]
    }
}
```



