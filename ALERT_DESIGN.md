# Kapacitor Alerting

Kapacitor enables a user to define and trigger alerts.
Alerts can be sent to various backend handlers.

## Alert State

Kapacitor exposes the state of all the alerts via its HTTP API.
See the API docs for more details.


## Two ways to work with alerts

### Direct Alerts

If you already have a system that manages your alerts then you can define your alerts directly in your TICKscripts.
This allows you to send alerts as they are triggered to any of the various alert handlers.


### Alert Events Subsystem

If you want to have more fine grained control over your alerts then an alert subsystem is available.
The alert subsystem allows you to various different actions with your alerts:

* Aggregate Alerts into a single alert containing summary information.
* Rate limit alerts
* Easily manage which handlers handle which alerts without modifying your Kapacitor tasks.

This subsystem is based on an event model.
When Kapacitor triggers an alert instead of directly sending it to the handlers, it is first sent to this subsystem as an event.
Then different handlers can listen for different events and take appropriate actions.

#### Using the Alert Event Subsystem

Add an alert handler called `alertEvent`, either globally in the config or on a task by task basis.

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
        .warn(lambda: "mean" > 70)
        .crit(lambda: "mean" > 80)
        // Send this alert to the alert event subsystem (not currently implemented)
        .alertEvent()
        // Send this alert directly to slack. (works today)
        .slack()
```

Then alert handlers can be configured to subscribe to the events.
These alert handlers will be configured via the API.
Use yaml/json to define the alert handlers.

```yaml
alert:
    - events:
        - cpu
        - mem
    - aggregate:
        groupBy: id
        interval: 1m
    - throttle:
        count: 10
        every: 5m
    - publish: throttled_agged
    - pagerDuty:
         serviceKey: XXX
```

```json
{
    "alert" : [
        {"events": ["cpu", "mem"]},
        {"aggregate": {"groupBy":"id","internal":"1m"}},
        {"throttle": {"count":10,"every":"5m"}},
        {"publish": ["throttled_aggreated"]},
        {"pagerDuty": {"serviceKey":"XXX"}}
    ]
}
```


#### Implementation

The underlying implementation will be a basic publish/subscribe system.
Various subscriber can be defined via the above definitions which can in turn publish back to the internal system or send alerts to third parties.

