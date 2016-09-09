# Kapacitor Alerting


Kapacitor should expose the current state of all alerts
Kapacitor should allow alerts to be throttled/aggregated and sent to varying alert handlers without having to modify all tasks.

## Implementation

Add new alert handler to AlertNode called `alertEvent`(TBD), which send the alert data struct to an internal pub/sub system. The current handlers will not be removed to allow existing TICKscripts to work as they are today. This enables users to quickly get started creating alerts as a single TICKscript is capable of defining alerts.

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
        // Send this alert to the pub/sub system (not currently implemented)
        .alertEvent()
        // Send this alert directly to slack. (works today)
        .slack()
```

Then alert handlers can be configured to subscribe to the events.
These alert handlers will be configured via the API, and will use a definition file to describe what the handler will do with the alert data.

There are two proposed formats for the alert handler definition files:

1. Use the TICKscript syntax but a new API for defining the alert handler

```go
alert
    |from('cpu', 'mem')
    |groupBy('id')
    |aggregate()
        .interval(1m)
    |groupBy()
    |throttle()
        .max(10, 5m)
    |pub('throttled_agged')
```

2. Use yaml to define the alert handler.

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

By using TICKscript the syntax remains familiar and there is no need for users to also know YAML. But it could be very confusing to a user to keep the Task TICKscript API separate from the Alert TICKscript API.

By using YAML, the user needs to know YAML, but the domain is clearly separated from Tasks. Using YAML for defining a sequence of tasks is not without precedent (see Ansible). YAML is familiar to the DevOps user base, most users will not need to learn it as they already have.

In either case we want the file format to be self contained, so that it is easily shareable and can be placed in version control.

