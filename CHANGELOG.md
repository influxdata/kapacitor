# Changelog

## unreleased

## v1.5.2 [2018-12-12]

### Features

- [#2095](https://github.com/influxdata/kapacitor/issues/2095): Add barrier node support to join node.
- [#1157](https://github.com/influxdata/kapacitor/issues/1157): Add ability to expire groups using the barrier node.
- [#2099](https://github.com/influxdata/kapacitor/issues/2099): Add `alert/persist-topics` to config
- [#2101](https://github.com/influxdata/kapacitor/issues/2101): Add multiple field support to the change detect node.
- [#1961](https://github.com/influxdata/kapacitor/pull/1961): Add links to pagerduty2 alerts
- [#1974](https://github.com/influxdata/kapacitor/issues/1974): Add additional metadata to Sensu alerts.

### Bugfixes

- [#2048](https://github.com/influxdata/kapacitor/pull/2048): Fix join not catching up fast enough after a pause in the data stream.

## v1.5.1 [2018-08-06]

### Bugfixes

- [#1938](https://github.com/influxdata/kapacitor/issues/1938): pagerduty2 should use routingKey rather than serviceKey
- [#1982](https://github.com/influxdata/kapacitor/pull/1982): Fix KafkaTopic not working from TICKscript
- [#1989](https://github.com/influxdata/kapacitor/pull/1989): Improve Kafka alert throughput.

## v1.5.0 [2018-05-17]

### Features

- [#1842](https://github.com/influxdata/kapacitor/pull/1842): Add alert inhibitors that allow an alert to suppress events from other matching alerts.
- [#1833](https://github.com/influxdata/kapacitor/pull/1833): Config format updated to allow for more than one slack configuration.  
- [#1844](https://github.com/influxdata/kapacitor/pull/1844): Added a new kapacitor node changeDetect that emits a value
    for each time a series field changes.
- [#1828](https://github.com/influxdata/kapacitor/pull/1828): Add recoverable field to JSON alert response to indicate whether the
alert will auto-recover.
- [#1823](https://github.com/influxdata/kapacitor/pull/1823): Update OpsGenie integration to use the v2 API.
    To upgrade to using the new API simply update your config and TICKscripts to use opsgenie2 instead of opsgenie.
    If your `opsgenie` config uses the `recovery_url` option, for `opsgenie2` you will need to change it to the `recovery_action` option.
    This is because the new v2 API is not structured with static URLs, and so only the action can be defined and not the entire URL.
- [#1690](https://github.com/influxdata/kapacitor/issues/1690): Add https-private-key option to httpd config.
- [#1561](https://github.com/influxdata/kapacitor/issues/1561): Add .quiet to all nodes to silence any errors reported by the node.
- [#1826](https://github.com/influxdata/kapacitor/issues/1826): Add Kafka alert handler.

### Bugfixes
- [#1794](https://github.com/influxdata/kapacitor/issues/1794): Kapacitor ticks generating a hash instead of their actual given name.
- [#1827](https://github.com/influxdata/kapacitor/pull/1827): Fix deadlock in load service when task has an error.
- [#1795](https://github.com/influxdata/kapacitor/pull/1795): Support PagerDuty API v2
- [#1776](https://github.com/influxdata/kapacitor/issues/1776): Fix bug where you could not delete a topic handler with the same name as its topic.
- [#1905](https://github.com/influxdata/kapacitor/pull/1905): Adjust PagerDuty v2 service-test names and capture detailed error messages.
- [#1913](https://github.com/influxdata/kapacitor/pull/1913): Fix Kafka configuration.

## v1.4.1 [2018-03-13]

### Bugfixes

- [#1834](https://github.com/influxdata/kapacitor/issues/1834): Fix bug where task type was invalid when using var for stream/batch

## v1.4.0 [2017-12-08]

The v1.4.0 release has many new features, here is a list of some of the highlights:

1. Load TICKscripts and alert handlers from a directory.
2. Structed Logging  with a logging API endpoints to be able to tail logs for given tasks.
3. Autoscale support for Docker Swarm and EC2 Autoscaling.
4. Sideload data into your TICKscript streams from external sources.
5. Fully customizable POST body for the alert POST handler and the httpPost node.

See the complete list of bug fixes and features below.

### Bugfixes

- [#1710](https://github.com/influxdata/kapacitor/issues/1710): Idle Barrier is dropping all messages when source has clock offset
- [#1719](https://github.com/influxdata/kapacitor/pull/1719): Fix oddly generated TOML for mqtt & httppost

## v1.4.0-rc3 [2017-12-04]

### Bugfixes

- [#1703](https://github.com/influxdata/kapacitor/pull/1703): Fix issues where log API checked the wrong header for the desired content type.

## v1.4.0-rc2 [2017-11-28]

### Features

- [#1622](https://github.com/influxdata/kapacitor/pull/1622): Add support for AWS EC2 autoscaling services.
- [#1566](https://github.com/influxdata/kapacitor/pull/1566): Add BarrierNode to emit BarrierMessage periodically

### Bugfixes

- [#1250](https://github.com/influxdata/kapacitor/issues/1250): Fix VictorOps "data" field being a string instead of actual JSON.
- [#1697](https://github.com/influxdata/kapacitor/issues/1697): Fix panic with MQTT toml configuration generation.

## v1.4.0-rc1 [2017-11-09]

### Features

- [#1408](https://github.com/influxdata/kapacitor/issues/1408): Add Previous state
- [#1575](https://github.com/influxdata/kapacitor/issues/1575): Add support to persist replay status after it finishes.
- [#1461](https://github.com/influxdata/kapacitor/issues/1461): alert.post and https_post timeouts needed.
- [#1413](https://github.com/influxdata/kapacitor/issues/1413): Add subscriptions modes to InfluxDB subscriptions.
- [#1436](https://github.com/influxdata/kapacitor/issues/1436): Add linear fill support for QueryNode.
- [#1345](https://github.com/influxdata/kapacitor/issues/1345): Add MQTT Alert Handler
- [#1390](https://github.com/influxdata/kapacitor/issues/1390): Add built in functions to convert timestamps to integers
- [#1425](https://github.com/influxdata/kapacitor/pull/1425): BREAKING: Change over internal API to use message passing semantics.
    The breaking change is that the Combine and Flatten nodes previously, but erroneously, operated across batch boundaries; this has been fixed.
- [#1497](https://github.com/influxdata/kapacitor/pull/1497): Add support for Docker Swarm autoscaling services.
- [#1485](https://github.com/influxdata/kapacitor/issues/1485): Add bools field types to UDFs.
- [#1549](https://github.com/influxdata/kapacitor/issues/1549): Add stateless now() function to get the current local time.
- [#1545](https://github.com/influxdata/kapacitor/pull/1545): Add support for timeout, tags and service template in the Alerta AlertNode
- [#1568](https://github.com/influxdata/kapacitor/issues/1568): Add support for custom HTTP Post bodies via a template system.
- [#1569](https://github.com/influxdata/kapacitor/issues/1569): Add support for add the HTTP status code as a field when using httpPost
- [#1535](https://github.com/influxdata/kapacitor/pull/1535): Add logfmt support and refactor logging.
- [#1481](https://github.com/influxdata/kapacitor/pull/1481): Add ability to load tasks/handlers from dir.
    TICKscript was extended to be able to describe a task exclusively through a tickscript.
      * tasks no longer need to specify their TaskType (Batch, Stream).
      * `dbrp` expressions were added to tickscript.
    Topic-Handler file format was modified to include the TopicID and HandlerID in the file.
    Load service was added; the service can load tasks/handlers from a directory.
- [#1606](https://github.com/influxdata/kapacitor/pull/1606): Update Go version to 1.9.1
- [#1578](https://github.com/influxdata/kapacitor/pull/1578): Add support for exposing logs via the API. API is released as a technical preview.
- [#1605](https://github.com/influxdata/kapacitor/issues/1605): Add support for {{ .Duration }} on Alert Message property.
- [#1644](https://github.com/influxdata/kapacitor/issues/1644): Add support for [JSON lines](https://en.wikipedia.org/wiki/JSON_Streaming#Line_delimited_JSON) for steaming HTTP logs.
- [#1637](https://github.com/influxdata/kapacitor/issues/1637): Add new node Sideload, that allows loading data from files into the stream of data. Data can be loaded using a hierarchy.
- [#1667](https://github.com/influxdata/kapacitor/pull/1667): Promote Alert API to stable v1 path.
- [#1668](https://github.com/influxdata/kapacitor/pull/1668): Change WARN level logs to INFO level.

### Bugfixes

- [#916](https://github.com/influxdata/kapacitor/issues/916): Crash of Kapacitor on Windows x64 when starting a recording
- [#1400](https://github.com/influxdata/kapacitor/issues/1400): Allow for `.yml` file extensions in `define-topic-handler`
- [#1402](https://github.com/influxdata/kapacitor/pull/1402): Fix http server error logging.
- [#1500](https://github.com/influxdata/kapacitor/pull/1500): Fix bugs with stopping running UDF agent.
- [#1470](https://github.com/influxdata/kapacitor/pull/1470): Fix error messages for missing fields which are arguments to functions are not clear
- [#1516](https://github.com/influxdata/kapacitor/pull/1516): Fix bad PagerDuty test the required server info.
- [#1581](https://github.com/influxdata/kapacitor/pull/1581): Add SNMP sysUpTime to SNMP Trap service
- [#1547](https://github.com/influxdata/kapacitor/issues/1547): Fix panic on recording replay with HTTPPostHandler.
- [#1623](https://github.com/influxdata/kapacitor/issues/1623): Fix k8s incluster master api dns resolution
- [#1630](https://github.com/influxdata/kapacitor/issues/1630): Remove the pidfile after the server has exited.
- [#1641](https://github.com/influxdata/kapacitor/issues/1641): Logs API writes multiple http headers.
- [#1657](https://github.com/influxdata/kapacitor/issues/1657): Fix missing dependency in rpm package.
- [#1660](https://github.com/influxdata/kapacitor/pull/1660): Force tar owner/group to be root.
- [#1663](https://github.com/influxdata/kapacitor/pull/1663): Fixed install/remove of kapacitor on non-systemd Debian/Ubuntu systems.
    Fixes packaging to not enable services on RHEL systems.
    Fixes issues with recusive symlinks on systemd systems.
- [#1662](https://github.com/influxdata/kapacitor/issues/1662): Fix invalid default MQTT config.

## v1.3.3 [2017-08-11]

### Bugfixes
- [#1520](https://github.com/influxdata/kapacitor/pull/1520): Expose pprof without authentication if enabled

## v1.3.2 [2017-08-08]

### Bugfixes
- [#1512](https://github.com/influxdata/kapacitor/pull/1512): Use details field from alert node in PagerDuty.

## v1.3.1 [2017-06-02]

### Bugfixes

- [#1415](https://github.com/influxdata/kapacitor/pull/1415): Proxy from environment for HTTP request to slack
- [#1414](https://github.com/influxdata/kapacitor/pull/1414): Fix derivative node preserving fields from previous point in stream tasks.

## v1.3.0 [2017-05-22]

### Release Notes

The v1.3.0 release has two major features.

1. Addition of scraping and discovering for Prometheus style data collection.
2. Updates to the Alert Topic system

Here is a quick example of how to configure Kapacitor to scrape discovered targets.
First configure a discoverer, here we use the file-discovery discoverer.
Next configure a scraper to use that discoverer.

>NOTE: The scraping and discovering features are released under technical preview,
meaning that the configuration or API around the feature may change in a future release.

```
# Configure file discoverer
[[file-discovery]]
 enabled = true
 id = "discover_files"
 refresh-interval = "10s"
 ##### This will look for prometheus json files
 ##### File format is here https://prometheus.io/docs/operating/configuration/#%3Cfile_sd_config%3E
 files = ["/tmp/prom/*.json"]

# Configure scraper
[[scraper]]
 enabled = true
 name = "node_exporter"
 discoverer-id = "discover_files"
 discoverer-service = "file-discovery"
 db = "prometheus"
 rp = "autogen"
 type = "prometheus"
 scheme = "http"
 metrics-path = "/metrics"
 scrape-interval = "2s"
 scrape-timeout = "10s"
```

Add the above snippet to your kapacitor.conf file.

Create the below snippet as the file `/tmp/prom/localhost.json`:

```
[{
 "targets": ["localhost:9100"]
}]
```

Start the Prometheus node_exporter locally.

Now startup Kapacitor and it will discover the `localhost:9100` node_exporter target and begin scrapping it for metrics.
For more details on the scraping and discovery systems see the full documentation [here](https://docs.influxdata.com/kapacitor/v1.3/scraping).

The second major feature with this release, are changes to the alert topic system.
The previous release introduce this new system as a technical preview, with this release the alerting service has been simplified.
Alert handlers now only ever have a single action and belong to a single topic.

The handler definition has been simplified as a result.
Here are some example alert handlers using the new structure:

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

To define a handler now you must specify which topic the handler belongs to.
For example to define the above aggregate handler on the system topic use this command:

```sh
kapacitor define-handler system aggregate_by_1m.yaml
```

For more details on the alerting system see the full documentation [here](https://docs.influxdata.com/kapacitor/v1.3/alerts).

# Bugfixes

- [#1396](https://github.com/influxdata/kapacitor/pull/1396): Fix broken ENV var config overrides for the kubernetes section.
- [#1397](https://github.com/influxdata/kapacitor/pull/1397): Update default configuration file to include sections for each discoverer service.

## v1.3.0-rc4 [2017-05-19]

# Bugfixes

- [#1379](https://github.com/influxdata/kapacitor/issues/1379): Copy batch points slice before modification, fixes potential panics and data corruption.
- [#1394](https://github.com/influxdata/kapacitor/pull/1394): Use the Prometheus metric name as the measurement name by default for scrape data.
- [#1392](https://github.com/influxdata/kapacitor/pull/1392): Fix possible deadlock for scraper configuration updating.

## v1.3.0-rc3 [2017-05-18]

### Bugfixes

- [#1369](https://github.com/influxdata/kapacitor/issues/1369): Fix panic with concurrent writes to same points in state tracking nodes.
- [#1387](https://github.com/influxdata/kapacitor/pull/1387): static-discovery configuration simplified
- [#1378](https://github.com/influxdata/kapacitor/issues/1378): Fix panic in InfluxQL node with missing field.

## v1.3.0-rc2 [2017-05-11]

### Bugfixes

- [#1370](https://github.com/influxdata/kapacitor/issues/1370): Fix missing working_cardinality stats on stateDuration and stateCount nodes.

## v1.3.0-rc1 [2017-05-08]

### Features

- [#1299](https://github.com/influxdata/kapacitor/pull/1299): Allowing sensu handler to be specified
- [#1284](https://github.com/influxdata/kapacitor/pull/1284): Add type signatures to Kapacitor functions.
- [#1203](https://github.com/influxdata/kapacitor/issues/1203): Add `isPresent` operator for verifying whether a value is present (part of [#1284](https://github.com/influxdata/kapacitor/pull/1284)).
- [#1354](https://github.com/influxdata/kapacitor/pull/1354): Add Kubernetes scraping support.
- [#1359](https://github.com/influxdata/kapacitor/pull/1359): Add groupBy exclude and Add dropOriginalFieldName to flatten.
- [#1360](https://github.com/influxdata/kapacitor/pull/1360): Add KapacitorLoopback node to be able to send data from a task back into Kapacitor.

### Bugfixes

- [#1329](https://github.com/influxdata/kapacitor/issues/1329): BREAKING: A bug was fixed around missing fields in the derivative node.
    The behavior of the node changes slightly in order to provide a consistent fix to the bug.
    The breaking change is that now, the time of the points returned are from the right hand or current point time, instead of the left hand or previous point time.
- [#1353](https://github.com/influxdata/kapacitor/issues/1353): Fix panic in scraping TargetManager.
- [#1238](https://github.com/influxdata/kapacitor/pull/1238): Use ProxyFromEnvironment for all outgoing HTTP traffic.

## v1.3.0-beta2 [2017-05-01]

### Features

- [#117](https://github.com/influxdata/kapacitor/issues/117): Add headers to alert POST requests.

### Bugfixes

- [#1294](https://github.com/influxdata/kapacitor/issues/1294): Fix bug where batch queries would be missing all fields after the first nil field.
- [#1343](https://github.com/influxdata/kapacitor/issues/1343): BREAKING: The UDF agent Go API has changed, the changes now make it so that the agent package is self contained.

## v1.3.0-beta1 [2017-04-29]

### Features

- [#1322](https://github.com/influxdata/kapacitor/pull/1322): TLS configuration in Slack service for Mattermost compatibility
- [#1330](https://github.com/influxdata/kapacitor/issues/1330): Generic HTTP Post node
- [#1159](https://github.com/influxdata/kapacitor/pulls/1159): Go version 1.7.4 -> 1.7.5
- [#1175](https://github.com/influxdata/kapacitor/pull/1175): BREAKING: Add generic error counters to every node type.
    Renamed `query_errors` to `errors` in batch node.
    Renamed `eval_errors` to `errors` in eval node.
- [#922](https://github.com/influxdata/kapacitor/issues/922): Expose server specific information in alert templates.
- [#1162](https://github.com/influxdata/kapacitor/pulls/1162): Add Pushover integration.
- [#1221](https://github.com/influxdata/kapacitor/pull/1221): Add `working_cardinality` stat to each node type that tracks the number of groups per node.
- [#1211](https://github.com/influxdata/kapacitor/issues/1211): Add StateDuration node.
- [#1209](https://github.com/influxdata/kapacitor/issues/1209): BREAKING: Refactor the Alerting service.
    The change is completely breaking for the technical preview alerting service, a.k.a. the new alert topic handler features.
    The change boils down to simplifying how you define and interact with topics.
    Alert handlers now only ever have a single action and belong to a single topic.
    An automatic migration from old to new handler definitions will be performed during startup.
    See the updated API docs.
- [#1286](https://github.com/influxdata/kapacitor/issues/1286): Default HipChat URL should be blank
- [#507](https://github.com/influxdata/kapacitor/issues/507): Add API endpoint for performing Kapacitor database backups.
- [#1132](https://github.com/influxdata/kapacitor/issues/1132): Adding source for sensu alert as parameter
- [#1346](https://github.com/influxdata/kapacitor/pull/1346): Add discovery and scraping services.

### Bugfixes

- [#1133](https://github.com/influxdata/kapacitor/issues/1133): Fix case-sensitivity for Telegram `parseMode` value.
- [#1147](https://github.com/influxdata/kapacitor/issues/1147): Fix pprof debug endpoint
- [#1164](https://github.com/influxdata/kapacitor/pull/1164): Fix hang in config API to update a config section.
    Now if the service update process takes too long the request will timeout and return an error.
    Previously the request would block forever.
- [#1165](https://github.com/influxdata/kapacitor/issues/1165): Make the alerta auth token prefix configurable and default it to Bearer.
- [#1184](https://github.com/influxdata/kapacitor/pull/1184): Fix logrotate file to correctly rotate error log.
- [#1200](https://github.com/influxdata/kapacitor/pull/1200): Fix bug with alert duration being incorrect after restoring alert state.
- [#1199](https://github.com/influxdata/kapacitor/pull/1199): BREAKING: Fix inconsistency with JSON data from alerts.
    The alert handlers Alerta, Log, OpsGenie, PagerDuty, Post and VictorOps allow extra opaque data to be attached to alert notifications.
    That opaque data was inconsistent and this change fixes that.
    Depending on how that data was consumed this could result in a breaking change, since the original behavior was inconsistent
    we decided it would be best to fix the issue now and make it consistent for all future builds.
    Specifically in the JSON result data the old key `Series` is always `series`, and the old key `Err` is now always `error` instead of for only some of the outputs.
- [#1181](https://github.com/influxdata/kapacitor/pull/1181): Fix bug parsing dbrp values with quotes.
- [#1228](https://github.com/influxdata/kapacitor/pull/1228): Fix panic on loading replay files without a file extension.
- [#1192](https://github.com/influxdata/kapacitor/issues/1192): Fix bug in Default Node not updating batch tags and groupID.
    Also empty string on a tag value is now a sufficient condition for the default conditions to be applied.
    See [#1233](https://github.com/influxdata/kapacitor/pull/1233) for more information.
- [#1068](https://github.com/influxdata/kapacitor/issues/1068): Fix dot view syntax to use xlabels and not create invalid quotes.
- [#1295](https://github.com/influxdata/kapacitor/issues/1295): Fix curruption of recordings list after deleting all recordings.
- [#1237](https://github.com/influxdata/kapacitor/issues/1237): Fix missing "vars" key when listing tasks.
- [#1271](https://github.com/influxdata/kapacitor/issues/1271): Fix bug where aggregates would not be able to change type.
- [#1261](https://github.com/influxdata/kapacitor/issues/1261): Fix panic when the process cannot stat the data dir.

## v1.2.1 [2017-04-13]

### Bugfixes

- [#1323](https://github.com/influxdata/kapacitor/pull/1323): Fix issue where credentials to InfluxDB could not be updated dynamically.

## v1.2.0 [2017-01-23]

### Release Notes

A new system for working with alerts has been introduced.
This alerting system allows you to configure topics for alert events and then configure handlers for various topics.
This way alert generation is decoupled from alert handling.

Existing TICKscripts will continue to work without modification.

To use this new alerting system remove any explicit alert handlers from your TICKscript and specify a topic.
Then configure the handlers for the topic.

```
stream
    |from()
      .measurement('cpu')
      .groupBy('host')
    |alert()
      // Specify the topic for the alert
      .topic('cpu')
      .info(lambda: "value" > 60)
      .warn(lambda: "value" > 70)
      .crit(lambda: "value" > 80)
      // No handlers are configured in the script, they are instead defined on the topic via the API.
```

The API exposes endpoints to query the state of each alert and endpoints for configuring alert handlers.
See the [API docs](https://docs.influxdata.com/kapacitor/latest/api/api/) for more details.
The kapacitor CLI has been updated with commands for defining alert handlers.

This release introduces a new feature where you can window based off the number of points instead of their time.
For example:

```
stream
    |from()
        .measurement('my-measurement')
    // Emit window for every 10 points with 100 points per window.
    |window()
        .periodCount(100)
        .everyCount(10)
    |mean('value')
    |alert()
         .crit(lambda: "mean" > 100)
         .slack()
         .channel('#alerts')
```


With this change alert nodes will have an anonymous topic created for them.
This topic is managed like all other topics preserving state etc. across restarts.
As a result existing alert nodes will now remember the state of alerts after restarts and disiabling/enabling a task.

>NOTE: The new alerting features are being released under technical preview.
This means breaking changes may be made in later releases until the feature is considered complete.
See the [API docs on technical preview](https://docs.influxdata.com/kapacitor/v1.2/api/api/#technical-preview) for specifics of how this effects the API.

### Features

- [#1110](https://github.com/influxdata/kapacitor/pull/1110): Add new query property for aligning group by intervals to start times.
- [#1095](https://github.com/influxdata/kapacitor/pull/1095): Add new alert API, with support for configuring handlers and topics.
- [#1052](https://github.com/influxdata/kapacitor/issues/1052): Move alerta api token to header and add option to skip TLS verification.
- [#929](https://github.com/influxdata/kapacitor/pull/929): Add SNMP trap service for alerting.
- [#913](https://github.com/influxdata/kapacitor/issues/913): Add fillPeriod option to Window node, so that the first emit waits till the period has elapsed before emitting.
- [#898](https://github.com/influxdata/kapacitor/issues/898): Now when the Window node every value is zero, the window will be emitted immediately for each new point.
- [#744](https://github.com/influxdata/kapacitor/issues/744): Preserve alert state across restarts and disable/enable actions.
- [#327](https://github.com/influxdata/kapacitor/issues/327): You can now window based on count in addition to time.
- [#251](https://github.com/influxdata/kapacitor/issues/251): Enable markdown in slack attachments.


### Bugfixes

- [#1100](https://github.com/influxdata/kapacitor/issues/1100): Fix issue with the Union node buffering more points than necessary.
- [#1087](https://github.com/influxdata/kapacitor/issues/1087): Fix panic during close of failed startup when connecting to InfluxDB.
- [#1045](https://github.com/influxdata/kapacitor/issues/1045): Fix panic during replays.
- [#1043](https://github.com/influxdata/kapacitor/issues/1043): logrotate.d ignores kapacitor configuration due to bad file mode.
- [#872](https://github.com/influxdata/kapacitor/issues/872): Fix panic during failed aggregate results.

## v1.1.1 [2016-12-02]

### Release Notes

No changes to Kapacitor, only upgrading to go 1.7.4 for security patches.

## v1.1.0 [2016-10-07]

### Release Notes

New K8sAutoscale node that allows you to auotmatically scale Kubernetes deployments driven by any metrics Kapacitor consumes.
For example, to scale a deployment `myapp` based off requests per second:

```
// The target requests per second per host
var target = 100.0

stream
    |from()
        .measurement('requests')
        .where(lambda: "deployment" == 'myapp')
    // Compute the moving average of the last 5 minutes
    |movingAverage('requests', 5*60)
        .as('mean_requests_per_second')
    |k8sAutoscale()
        .resourceName('app')
        .kind('deployments')
        .min(4)
        .max(100)
        // Compute the desired number of replicas based on target.
        .replicas(lambda: int(ceil("mean_requests_per_second" / target)))
```


New API endpoints have been added to be able to configure InfluxDB clusters and alert handlers dynamically without needing to restart the Kapacitor daemon.
Along with the ability to dynamically configure a service, API endpoints have been added to test the configurable services.
See the [API docs](https://docs.influxdata.com/kapacitor/latest/api/api/) for more details.

>NOTE: The `connect_errors` stat from the query node was removed since the client changed, all errors are now counted in the `query_errors` stat.

### Features

- [#931](https://github.com/influxdata/kapacitor/issues/931): Add a Kubernetes autoscaler node. You can now autoscale your Kubernetes deployments via Kapacitor.
- [#928](https://github.com/influxdata/kapacitor/issues/928): Add new API endpoint for dynamically overriding sections of the configuration.
- [#980](https://github.com/influxdata/kapacitor/pull/980): Upgrade to using go 1.7
- [#957](https://github.com/influxdata/kapacitor/issues/957): Add API endpoints for testing service integrations.
- [#958](https://github.com/influxdata/kapacitor/issues/958): Add support for Slack icon emojis and custom usernames.
- [#991](https://github.com/influxdata/kapacitor/pull/991): Bring Kapacitor up to parity with available InfluxQL functions in 1.1

### Bugfixes

- [#984](https://github.com/influxdata/kapacitor/issues/984): Fix bug where keeping a list of fields that where not referenced in the eval expressions would cause an error.
- [#955](https://github.com/influxdata/kapacitor/issues/955): Fix the number of subscriptions statistic.
- [#999](https://github.com/influxdata/kapacitor/issues/999): Fix inconsistency with InfluxDB by adding config option to set a default retention policy.
- [#1018](https://github.com/influxdata/kapacitor/pull/1018): Sort and dynamically adjust column width in CLI output. Fixes #785
- [#1019](https://github.com/influxdata/kapacitor/pull/1019): Adds missing strLength function.

## v1.0.2 [2016-10-06]

### Release Notes

### Features

### Bugfixes

- [#951](https://github.com/influxdata/kapacitor/pull/951): Fix bug where errors to save cluster/server ID files were ignored.
- [#954](https://github.com/influxdata/kapacitor/pull/954): Create data_dir on startup if it does not exist.

## v1.0.1 [2016-09-26]

### Release Notes

### Features

- [#873](https://github.com/influxdata/kapacitor/pull/873): Add TCP alert handler
- [#869](https://github.com/influxdata/kapacitor/issues/869): Add ability to set alert message as a field
- [#854](https://github.com/influxdata/kapacitor/issues/854): Add `.create` property to InfluxDBOut node, which when set will create the database
    and retention policy on task start.
- [#909](https://github.com/influxdata/kapacitor/pull/909): Allow duration / duration in TICKscript.
- [#777](https://github.com/influxdata/kapacitor/issues/777): Add support for string manipulation functions.
- [#886](https://github.com/influxdata/kapacitor/issues/886): Add ability to set specific HTTP port and hostname per configured InfluxDB cluster.

### Bugfixes

- [#889](https://github.com/influxdata/kapacitor/issues/889): Some typo in the default config file
- [#914](https://github.com/influxdata/kapacitor/pull/914): Change |log() output to be in JSON format so its self documenting structure.
- [#915](https://github.com/influxdata/kapacitor/pull/915): Fix issue with TMax and the Holt-Winters method.
- [#927](https://github.com/influxdata/kapacitor/pull/927): Fix bug with TMax and group by time.

## v1.0.0 [2016-09-02]

### Release Notes

Final release of v1.0.0.

## v1.0.0-rc3 [2016-09-01]

### Release Notes

### Features

### Bugfixes

- [#842](https://github.com/influxdata/kapacitor/issues/842): Fix side-effecting modification in batch WhereNode.

## v1.0.0-rc2 [2016-08-29]

### Release Notes

### Features

- [#827](https://github.com/influxdata/kapacitor/issues/827): Bring Kapacitor up to parity with available InfluxQL functions in 1.0

### Bugfixes

- [#763](https://github.com/influxdata/kapacitor/issues/763): Fix NaNs begin returned from the `sigma` stateful function.
- [#468](https://github.com/influxdata/kapacitor/issues/468): Fix tickfmt munging escaped slashes in regexes.

## v1.0.0-rc1 [2016-08-22]

### Release Notes

#### Alert reset expressions

Kapacitor now supports alert reset expressions.
This way when an alert enters a state, it can only be lowered in severity if its reset expression evaluates to true.

Example:

```go
stream
    |from()
      .measurement('cpu')
      .where(lambda: "host" == 'serverA')
      .groupBy('host')
    |alert()
      .info(lambda: "value" > 60)
      .infoReset(lambda: "value" < 50)
      .warn(lambda: "value" > 70)
      .warnReset(lambda: "value" < 60)
      .crit(lambda: "value" > 80)
      .critReset(lambda: "value" < 70)
```

For example given the following values:

    61 73 64 85 62 56 47

The corresponding alert states are:

    INFO WARNING WARNING CRITICAL INFO INFO OK

### Features

- [#740](https://github.com/influxdata/kapacitor/pull/740): Support reset expressions to prevent an alert from being lowered in severity. Thanks @minhdanh!
- [#670](https://github.com/influxdata/kapacitor/issues/670): Add ability to supress OK recovery alert events.
- [#804](https://github.com/influxdata/kapacitor/pull/804): Add API endpoint for refreshing subscriptions.
    Also fixes issue where subs were not relinked if the sub was deleted.
    UDP listen ports are closed when a database is dropped.

### Bugfixes

- [#783](https://github.com/influxdata/kapacitor/pull/783): Fix panic when revoking tokens not already defined.
- [#784](https://github.com/influxdata/kapacitor/pull/784): Fix several issues with comment formatting in TICKscript.
- [#786](https://github.com/influxdata/kapacitor/issues/786): Deleting tags now updates the group by dimensions if needed.
- [#772](https://github.com/influxdata/kapacitor/issues/772): Delete task snapshot data when a task is deleted.
- [#797](https://github.com/influxdata/kapacitor/issues/797): Fix panic from race condition in task master.
- [#811](https://github.com/influxdata/kapacitor/pull/811): Fix bug where subscriptions + tokens would not work with more than one InfluxDB cluster.
- [#812](https://github.com/influxdata/kapacitor/issues/812): Upgrade to use protobuf version 3.0.0

## v1.0.0-beta4 [2016-07-27]

### Release Notes

#### Group By Fields

Kapacitor now supports grouping by fields.
First convert a field into a tag using the EvalNode.
Then group by the new tag.

Example:

```go
stream
    |from()
        .measurement('alerts')
    // Convert field 'level' to tag.
    |eval(lambda: string("level"))
        .as('level')
        .tags('level')
    // Group by new tag 'level'.
    |groupBy('alert', 'level')
    |...
```

Note the field `level` is now removed from the point since `.keep` was not used.
See the [docs](https://docs.influxdata.com/kapacitor/v1.0/nodes/eval_node/#tags) for more details on how `.tags` works.


#### Delete Fields or Tags

In companion with being able to create new tags, you can now delete tags or fields.


Example:

```go
stream
    |from()
        .measurement('alerts')
    |delete()
        // Remove the field `extra` and tag `uuid` from all points.
        .field('extra')
        .tag('uuid')
    |...
```



### Features

- [#702](https://github.com/influxdata/kapacitor/pull/702): Add plumbing for authentication backends.
- [#624](https://github.com/influxdata/kapacitor/issue/624): BREAKING: Add ability to GroupBy fields. First use EvalNode to create a tag from a field and then group by the new tag.
    Also allows for grouping by measurement.
    The breaking change is that the group ID format has changed to allow for the measurement name.
- [#759](https://github.com/influxdata/kapacitor/pull/759): Add mechanism for token based subscription auth.
- [#745](https://github.com/influxdata/kapacitor/pull/745): Add if function for tick script, for example: `if("value" > 6, 1, 2)`.

### Bugfixes

- [#710](https://github.com/influxdata/kapacitor/pull/710): Fix infinite loop when parsing unterminated regex in TICKscript.
- [#711](https://github.com/influxdata/kapacitor/issues/711): Fix where database name with quotes breaks subscription startup logic.
- [#719](https://github.com/influxdata/kapacitor/pull/719): Fix panic on replay.
- [#723](https://github.com/influxdata/kapacitor/pull/723): BREAKING: Search for valid configuration on startup in ~/.kapacitor and /etc/kapacitor/.
    This is so that the -config CLI flag is not required if the configuration is found in a standard location.
    The configuration file being used is always logged to STDERR.
- [#298](https://github.com/influxdata/kapacitor/issues/298): BREAKING: Change alert level evaluation so each level is independent and not required to be a subset of the previous level.
    The breaking change is that expression evaluation order changed.
    As a result stateful expressions that relied on that order are broken.
- [#749](https://github.com/influxdata/kapacitor/issues/749): Fix issue with tasks with empty DAG.
- [#718](https://github.com/influxdata/kapacitor/issues/718): Fix broken extra expressions for deadman's switch.
- [#752](https://github.com/influxdata/kapacitor/issues/752): Fix various bugs relating to the `fill` operation on a JoinNode.
    Fill with batches and fill when using the `on` property were broken.
    Also changes the DefaultNode set defaults for nil fields.

## v1.0.0-beta3 [2016-07-09]

### Release Notes

### Features

- [#662](https://github.com/influxdata/kapacitor/pull/662): Add `-skipVerify` flag to `kapacitor` CLI tool to skip SSL verification.
- [#680](https://github.com/influxdata/kapacitor/pull/680): Add Telegram Alerting option, thanks @burdandrei!
- [#46](https://github.com/influxdata/kapacitor/issues/46): Can now create combinations of points within the same stream.
  This is kind of like join but instead joining a stream with itself.
- [#669](https://github.com/influxdata/kapacitor/pull/669): Add size function for humanize byte size. thanks @jsvisa!
- [#697](https://github.com/influxdata/kapacitor/pull/697): Can now flatten a set of points into a single points creating dynamcially named fields.
- [#698](https://github.com/influxdata/kapacitor/pull/698): Join delimiter can be specified.
- [#695](https://github.com/influxdata/kapacitor/pull/695): Bash completion filters by enabled disabled status. Thanks @bbczeuz!
- [#706](https://github.com/influxdata/kapacitor/pull/706): Package UDF agents
- [#707](https://github.com/influxdata/kapacitor/pull/707): Add size field to BeginBatch struct of UDF protocol. Provides hint as to size of incoming batch.

### Bugfixes

- [#656](https://github.com/influxdata/kapacitor/pull/656): Fix issues where an expression could not be passed as a function parameter in TICKscript.
- [#627](https://github.com/influxdata/kapacitor/issues/627): Fix where InfluxQL functions that returned a batch could drop tags.
- [#674](https://github.com/influxdata/kapacitor/issues/674): Fix panic with Join On and batches.
- [#665](https://github.com/influxdata/kapacitor/issues/665): BREAKING: Fix file mode not being correct for Alert.Log files.
  Breaking change is that integers numbers prefixed with a 0 in TICKscript are interpreted as octal numbers.
- [#667](https://github.com/influxdata/kapacitor/issues/667): Align deadman timestamps to interval.

## v1.0.0-beta2 [2016-06-17]

### Release Notes

### Features

- [#636](https://github.com/influxdata/kapacitor/pull/636): Change HTTP logs to be in Common Log format.
- [#652](https://github.com/influxdata/kapacitor/pull/652): Add optional replay ID to the task API so that you can get information about a task inside a running replay.

### Bugfixes

- [#621](https://github.com/influxdata/kapacitor/pull/621): Fix obscure error about single vs double quotes.
- [#623](https://github.com/influxdata/kapacitor/pull/623): Fix issues with recording metadata missing data url.
- [#631](https://github.com/influxdata/kapacitor/issues/631): Fix issues with using iterative lambda expressions in an EvalNode.
- [#628](https://github.com/influxdata/kapacitor/issues/628): BREAKING: Change `kapacitord config` to not search default location for configuration files but rather require the `-config` option.
    Since the `kapacitord run` command behaves this way they should be consistent.
    Fix issue with `kapacitord config > kapacitor.conf` when the output file was a default location for the config.
- [#626](https://github.com/influxdata/kapacitor/issues/626): Fix issues when changing the ID of an enabled task.
- [#624](https://github.com/influxdata/kapacitor/pull/624): Fix issues where you could get a read error on a closed UDF socket.
- [#651](https://github.com/influxdata/kapacitor/pull/651): Fix issues where an error during a batch replay would hang because the task wouldn't stop.
- [#650](https://github.com/influxdata/kapacitor/pull/650): BREAKING: The default retention policy name was changed to `autogen` in InfluxDB.
    This changes Kapacitor to use `autogen` for the default retention policy for the stats.
    You may need to update your task DBRPs to use `autogen` instead of `default`.


## v1.0.0-beta1 [2016-06-06]

### Release Notes

#### Template Tasks

The ability to create and use template tasks has been added.
you can define a template for a task and reuse that template across multiple tasks.

A simple example:

```go
// Which measurement to consume
var measurement string
// Optional where filter
var where_filter = lambda: TRUE
// Optional list of group by dimensions
var groups = [*]
// Which field to process
var field string
// Warning criteria, has access to 'mean' field
var warn lambda
// Critical criteria, has access to 'mean' field
var crit lambda
// How much data to window
var window = 5m
// The slack channel for alerts
var slack_channel = '#alerts'

stream
    |from()
        .measurement(measurement)
        .where(where_filter)
        .groupBy(groups)
    |window()
        .period(window)
        .every(window)
    |mean(field)
    |alert()
         .warn(warn)
         .crit(crit)
         .slack()
         .channel(slack_channel)
```

Then you can define the template like so:

```
kapacitor define-template generic_mean_alert -tick path/to/above/script.tick -type stream
```

Next define a task that uses the template:

```
kapacitor define cpu_alert -template generic_mean_alert -vars cpu_vars.json -dbrp telegraf.default
```

Where `cpu_vars.json` would like like this:

```json
{
    "measurement": {"type" : "string", "value" : "cpu" },
    "where_filter": {"type": "lambda", "value": "\"cpu\" == 'cpu-total'"},
    "groups": {"type": "list", "value": [{"type":"string", "value":"host"},{"type":"string", "value":"dc"}]},
    "field": {"type" : "string", "value" : "usage_idle" },
    "warn": {"type" : "lambda", "value" : " \"mean\" < 30.0" },
    "crit": {"type" : "lambda", "value" : " \"mean\" < 10.0" },
    "window": {"type" : "duration", "value" : "1m" },
    "slack_channel": {"type" : "string", "value" : "#alerts_testing" }
}
```


#### Live Replays

With this release you can now replay data directly against a task from InfluxDB without having to first create a recording.
Replay the queries defined in the batch task `cpu_alert` for the past 10 hours.
```sh
kapacitor replay-live batch -task cpu_alert -past 10h
```

Or for a stream task with use a query directly:

```sh
kapacitor replay-live query -task cpu_alert -query 'SELECT usage_idle FROM telegraf."default".cpu WHERE time > now() - 10h'
```

#### HTTP based subscriptions

Now InfluxDB and Kapacitor support HTTP/S based subscriptions.
This means that Kapacitor need only listen on a single port for the HTTP service, greatly simplifying configuration and setup.

In order to start using HTTP subscriptions change the `subscription-protocol` option for your configured InfluxDB clusters.

For example:

```
[[influxdb]]
  enabled = true
  urls = ["http://localhost:8086",]
  subscription-protocol = "http"
  # or to use https
  #subscription-protocol = "https"
```

On startup Kapacitor will detect the change and recreate the subscriptions in InfluxDB to use the HTTP protocol.

>NOTE: While HTTP itself is a TCP transport such that packet loss shouldn't be an issue, if Kapacitor starts to slow down for whatever reason, InfluxDB will drop the subscription writes to Kapacitor.
In order to know if subscription writes are being dropped you should monitor the measurement `_internal.monitor.subscriber` for the field `writeFailures`.

#### Holt-Winters Forecasting

This release contains an new Holt Winters InfluxQL function.

With this forecasting method one can now define an alert based off forecasted future values.

For example, the following TICKscript will take the last 30 days of disk usage stats and using holt-winters forecast the next 7 days.
If the forecasted value crosses a threshold an alert is triggered.

The result is now Kapacitor will alert you 7 days in advance of a disk filling up.
This assumes a slow growth but by changing the vars in the script you could check for shorter growth intervals.

```go
// The interval on which to aggregate the disk usage
var growth_interval = 1d
// The number of `growth_interval`s to forecast into the future
var forecast_count = 7
// The amount of historical data to use for the fit
var history = 30d

// The critical threshold on used_percent
var threshold = 90.0

batch
    |query('''
    SELECT max(used_percent) as used_percent
    FROM "telegraf"."default"."disk"
''')
        .period(history)
        .every(growth_interval)
        .align()
        .groupBy(time(growth_interval), *)
    |holtWinters('used_percent', forecast_count, 0, growth_interval)
        .as('used_percent')
    |max('used_percent')
        .as('used_percent')
    |alert()
         // Trigger alert if the forecasted disk usage is greater than threshold
        .crit(lambda: "used_percent" > threshold)
```


### Features

- [#283](https://github.com/influxdata/kapacitor/issues/283): Add live replays.
- [#500](https://github.com/influxdata/kapacitor/issues/500): Support Float,Integer,String and Boolean types.
- [#82](https://github.com/influxdata/kapacitor/issues/82): Multiple services for PagerDuty alert. thanks @savagegus!
- [#558](https://github.com/influxdata/kapacitor/pull/558): Preserve fields as well as tags on selector InfluxQL functions.
- [#259](https://github.com/influxdata/kapacitor/issues/259): Template Tasks have been added.
- [#562](https://github.com/influxdata/kapacitor/pull/562): HTTP based subscriptions.
- [#595](https://github.com/influxdata/kapacitor/pull/595): Support counting and summing empty batches to 0.
- [#596](https://github.com/influxdata/kapacitor/pull/596): Support new group by time offset i.e. time(30s, 5s)
- [#416](https://github.com/influxdata/kapacitor/issues/416): Track ingress counts by database, retention policy, and measurement. Expose stats via cli.
- [#586](https://github.com/influxdata/kapacitor/pull/586): Add spread stateful function. thanks @upccup!
- [#600](https://github.com/influxdata/kapacitor/pull/600): Add close http response after handler laert post, thanks @jsvisa!
- [#606](https://github.com/influxdata/kapacitor/pull/606): Add Holt-Winters forecasting method.
- [#605](https://github.com/influxdata/kapacitor/pull/605): BREAKING: StatsNode for batch edge now count the number of points in a batch instead of count batches as a whole.
    This is only breaking if you have a deadman switch configured on a batch edge.
- [#611](https://github.com/influxdata/kapacitor/pull/611): Adds bash completion to the kapacitor CLI tool.


### Bugfixes

- [#540](https://github.com/influxdata/kapacitor/issues/540): Fixes bug with log level API endpoint.
- [#521](https://github.com/influxdata/kapacitor/issues/521): EvalNode now honors groups.
- [#561](https://github.com/influxdata/kapacitor/issues/561): Fixes bug when lambda expressions would return error about types with nested binary expressions.
- [#555](https://github.com/influxdata/kapacitor/issues/555): Fixes bug where "time" functions didn't work in lambda expressions.
- [#570](https://github.com/influxdata/kapacitor/issues/570): Removes panic in SMTP service on failed close connection.
- [#587](https://github.com/influxdata/kapacitor/issues/587): Allow number literals without leading zeros.
- [#584](https://github.com/influxdata/kapacitor/issues/584): Do not block during startup to send usage stats.
- [#553](https://github.com/influxdata/kapacitor/issues/553): Periodically check if new InfluxDB DBRPs have been created.
- [#602](https://github.com/influxdata/kapacitor/issues/602): Fix missing To property on email alert handler.
- [#581](https://github.com/influxdata/kapacitor/issues/581): Record/Replay batch tasks get cluster info from task not API.
- [#613](https://github.com/influxdata/kapacitor/issues/613): BREAKING: Allow the ID of templates and tasks to be updated via the PATCH method.
    The breaking change is that now PATCH request return a 200 with the template or task definition, where before they returned 204.

## v0.13.1 [2016-05-13]

### Release Notes

>**Breaking changes may require special upgrade steps from versions <= 0.12, please read the 0.13.0 release notes**

Along with the API changes of 0.13.0, validation logic was added to task IDs, but this was not well documented.
This minor release remedies that.

All IDs (tasks, recordings, replays) must match this regex `^[-\._\p{L}0-9]+$`, which is essentially numbers, unicode letters, '-', '.' and '_'.

If you have existing tasks which do not match this pattern they should continue to function normally.

### Features

### Bugfixes

- [#545](https://github.com/influxdata/kapacitor/issues/545): Fixes inconsistency with API docs for creating a task.
- [#544](https://github.com/influxdata/kapacitor/issues/544): Fixes issues with existings tasks and invalid names.
- [#543](https://github.com/influxdata/kapacitor/issues/543): Fixes default values not being set correctly in API calls.


## v0.13.0 [2016-05-11]

### Release Notes

>**Breaking changes may require special upgrade steps please read below.**

#### Upgrade Steps

Changes to how and where task data is store have been made.
In order to safely upgrade to version 0.13 you need to follow these steps:

1. Upgrade InfluxDB to version 0.13 first.
2. Update all TICKscripts to use the new `|` and `@` operators. Once Kapacitor no longer issues any `DEPRECATION` warnings you are ready to begin the upgrade.
The upgrade will work without this step but tasks using the old syntax cannot be enabled, until modified to use the new syntax.
3. Upgrade the Kapacitor binary/package.
4. Configure new database location. By default the location `/var/lib/kapacitor/kapacitor.db` is chosen for package installs or `./kapacitor.db` for manual installs.
Do **not** remove the configuration for the location of the old task.db database file since it is still needed to do the migration.

    ```
    [storage]
    boltdb = "/var/lib/kapacitor/kapacitor.db"
    ```

5. Restart Kapacitor. At this point Kapacitor will migrate all existing data to the new database file.
If any errors occur Kapacitor will log them and fail to startup. This way if Kapacitor starts up you can be sure the migration was a success and can continue normal operation.
The old database is opened in read only mode so that existing data cannot be corrupted.
Its recommended to start Kapacitor in debug logging mode for the migration so you can follow the details of the migration process.

At this point you may remove the configuration for the old `task` `dir` and restart Kapacitor to ensure everything is working.
Kapacitor will attempt the migration on every startup while the old configuration and db file exist, but will skip any data that was already migrated.


#### API Changes

With this release the API has been updated to what we believe will be the stable version for a 1.0 release.
Small changes may still be made but the significant work to create a RESTful HTTP API is complete.
Many breaking changes introduced, see the [client/API.md](http://github.com/influxdata/kapacitor/blob/master/client/API.md) doc for details on how the API works now.

#### CLI Changes

Along with the API changes, breaking changes where also made to the `kapacitor` CLI command.
Here is a break down of the CLI changes:

* Every thing has an ID now: tasks, recordings, even replays.
    The `name` used before to define a task is now its `ID`.
    As such instead of using `-name` and `-id` to refer to tasks and recordings,
    the flags have been changed to `-task` and `-recording` accordingly.
* Replays can be listed and deleted like tasks and recordings.
* Replays default to `fast` clock mode.
* The record and replay commands now have a `-no-wait` option to start but not wait for the recording/replay to complete.
* Listing recordings and replays displays the status of the respective action.
* Record and Replay command now have an optional flag `-replay-id`/`-recording-id` to specify the ID of the replay or recording.
    If not set then a random ID will be chosen like the previous behavior.

#### Notable features

UDF can now be managed externally to Kapacitor via Unix sockets.
A process or container can be launched independent of Kapacitor exposing a socket.
On startup Kapacitor will connect to the socket and begin communication.

Example UDF config for a socket based UDF.

```
[udf]
[udf.functions]
    [udf.functions.myCustomUDF]
       socket = "/path/to/socket"
       timeout = "10s"
```

Alert data can now be consumed directly from within TICKscripts.
For example, let's say we want to store all data that triggered an alert in InfluxDB with a tag `level` containing the level string value (i.e CRITICAL).

```javascript
...
    |alert()
        .warn(...)
        .crit(...)
        .levelTag('level')
        // and/or use a field
        //.levelField('level')
        // Also tag the data with the alert ID
        .idTag('id')
        // and/or use a field
        //.idField('id')
    |influxDBOut()
        .database('alerts')
        ...
```


### Features

- [#360](https://github.com/influxdata/kapacitor/pull/360): Forking tasks by measurement in order to improve performance
- [#386](https://github.com/influxdata/kapacitor/issues/386): Adds official Go HTTP client package.
- [#399](https://github.com/influxdata/kapacitor/issues/399): Allow disabling of subscriptions.
- [#417](https://github.com/influxdata/kapacitor/issues/417): UDFs can be connected over a Unix socket. This enables UDFs from across Docker containers.
- [#451](https://github.com/influxdata/kapacitor/issues/451): StreamNode supports `|groupBy` and `|where` methods.
- [#93](https://github.com/influxdata/kapacitor/issues/93): AlertNode now outputs data to child nodes. The output data can have either a tag or field indicating the alert level.
- [#281](https://github.com/influxdata/kapacitor/issues/281): AlertNode now has an `.all()` property that specifies that all points in a batch must match the criteria in order to trigger an alert.
- [#384](https://github.com/influxdata/kapacitor/issues/384): Add `elapsed` function to compute the time difference between subsequent points.
- [#230](https://github.com/influxdata/kapacitor/issues/230): Alert.StateChangesOnly now accepts optional duration arg. An alert will be triggered for every interval even if the state has not changed.
- [#426](https://github.com/influxdata/kapacitor/issues/426): Add `skip-format` query parameter to the `GET /task` endpoint so that returned TICKscript content is left unmodified from the user input.
- [#388](https://github.com/influxdata/kapacitor/issues/388): The duration of an alert is now tracked and exposed as part of the alert data as well as can be set as a field via `.durationField('duration')`.
- [#486](https://github.com/influxdata/kapacitor/pull/486): Default config file location.
- [#461](https://github.com/influxdata/kapacitor/pull/461): Make Alerta `event` property configurable.
- [#491](https://github.com/influxdata/kapacitor/pull/491): BREAKING: Rewriting stateful expression in order to improve performance, the only breaking change is: short circuit evaluation for booleans - for example: ``lambda: "bool_value" && (count() > 100)`` if "bool_value" is false, we won't evaluate "count".
- [#504](https://github.com/influxdata/kapacitor/pull/504): BREAKING: Many changes to the API and underlying storage system. This release requires a special upgrade process.
- [#511](https://github.com/influxdata/kapacitor/pull/511): Adds DefaultNode for providing default values for missing fields or tags.
- [#285](https://github.com/influxdata/kapacitor/pull/285): Track created,modified and last enabled dates on tasks.
- [#533](https://github.com/influxdata/kapacitor/pull/533): Add useful statistics for nodes.

### Bugfixes

- [#499](https://github.com/influxdata/kapacitor/issues/499): Fix panic in InfluxQL nodes if field is missing or incorrect type.
- [#441](https://github.com/influxdata/kapacitor/issues/441): Fix panic in UDF code.
- [#429](https://github.com/influxdata/kapacitor/issues/429): BREAKING: Change TICKscript parser to be left-associative on equal precedence operators. For example previously this statement `(1+2-3*4/5)` was evaluated as `(1+(2-(3*(4/5))))`
    which is not the typical/expected behavior. Now using left-associative parsing the statement is evaluated as `((1+2)-((3*4)/5))`.
- [#456](https://github.com/influxdata/kapacitor/pull/456): Fixes Alerta integration to let server set status, fix `rawData` attribute and set default severity to `indeterminate`.
- [#425](https://github.com/influxdata/kapacitor/pull/425): BREAKING: Preserving tags on influxql simple selectors - first, last, max, min, percentile
- [#423](https://github.com/influxdata/kapacitor/issues/423): Recording stream queries with group by now correctly saves data in time order not group by order.
- [#331](https://github.com/influxdata/kapacitor/issues/331): Fix panic when missing `.as()` for JoinNode.
- [#523](https://github.com/influxdata/kapacitor/pull/523): JoinNode will now emit join sets as soon as they are ready. If multiple joinable sets arrive in the same tolerance window than each will be emitted (previously the first points were dropped).
- [#537](https://github.com/influxdata/kapacitor/issues/537): Fix panic in alert node when batch is empty.

## v0.12.0 [2016-04-04]

### Release Notes

New TICKscript syntax that uses a different operators for chaining methods vs property methods vs UDF methods.

* A chaining method is a method that creates a new node in the pipeline. Uses the `|` operator.
* A property method is a method that changes a property on a node. Uses the `.` operator.
* A UDF method is a method that calls out to a UDF. Uses the `@` operator.

For example below the `from`, `mean`, and `alert` methods create new nodes,
the `detectAnomalies` method calls a UDF,
and the other methods modify the nodes as property methods.

```javascript
stream
    |from()
        .measurement('cpu')
        .where(lambda: "cpu" == 'cpu-total')
    |mean('usage_idle')
        .as('value')
    @detectAnomalies()
        .field('mean')
    |alert()
        .crit(lambda: "anomaly_score" > 10)
        .log('/tmp/cpu.log')
```

With this change a new binary is provided with Kapacitor `tickfmt` which will
format a TICKscript file according to a common standard.


### Features

- [#299](https://github.com/influxdata/kapacitor/issues/299): Changes TICKscript chaining method operators and adds `tickfmt` binary.
- [#389](https://github.com/influxdata/kapacitor/pull/389): Adds benchmarks to Kapacitor for basic use cases.
- [#390](https://github.com/influxdata/kapacitor/issues/390): BREAKING: Remove old `.mapReduce` functions.
- [#381](https://github.com/influxdata/kapacitor/pull/381): Adding enable/disable/delete/reload tasks by glob.
- [#401](https://github.com/influxdata/kapacitor/issues/401): Add `.align()` property to BatchNode so you can align query start and stop times.

### Bugfixes

- [#378](https://github.com/influxdata/kapacitor/issues/378): Fix issue where derivative would divide by zero.
- [#387](https://github.com/influxdata/kapacitor/issues/387): Add `.quiet()` option to EvalNode so errors can be suppressed if expected.
- [#400](https://github.com/influxdata/kapacitor/issues/400): All query/connection errors are counted and reported in BatchNode stats.
- [#412](https://github.com/influxdata/kapacitor/pull/412): Fix issues with batch queries dropping points because of nil fields.
- [#413](https://github.com/influxdata/kapacitor/pull/413): Allow disambiguation between ".groupBy" and "|groupBy".


## v0.11.0 [2016-03-22]

### Release Notes

Kapacitor is now using the functions from the new query engine in InfluxDB core.
Along with this change is a change in the TICKscript API so that using the InfluxQL functions is easier.
Simply call the desired method directly no need to call `.mapReduce` explicitly.
This change now hides the mapReduce aspect and handles it internally.
Using `.mapReduce` is officially deprecated in this release and will be remove in the next major release.
We feel that this change improves the readability of TICKscripts and exposes less implementation details
to the end user.
Updating your exising TICKscripts is simple.
If previously you had code like this:

```javascript
stream.from()...
    .window()...
    .mapReduce(influxql.count('value'))
```
then update it to look like this:

```javascript
stream.from()...
    .window()...
    .count('value')
```

a simple regex could fix all your existing scripts.

Kapacitor now exposes more internal metrics for determining the performance of a given task.
The internal statistics includes a new measurement named `node` that contains any stats a node provides, tagged by the task, node, task type and kind of node (i.e. window vs union).
All nodes provide an averaged execution time for the node.
These stats are also available in the DOT output of the Kapacitor show command.

Significant performance improvements have also been added.
In some cases Kapacitor throughput has improved by 4X.

Kapacitor can now connect to different InfluxDB clusters.
Multiple InfluxDB config sections can be defined and one will be marked as default.
To upgrade convert an `influxdb` config.

From this:

```
[influxdb]
  enabled = true
  ...
```

to this:

```
[[influxdb]]
  enabled = true
  default = true
  name = "localhost"
  ...
```

Various improvements to joining features have been implemented.
With #144 you can now join streams with differing group by dimensions.

If you previously configured Email, Slack or HipChat globally now you must also set the `state-changes-only` option to true as well if you want to preserve the original behavior.
For example:

```
[slack]
   enable = true
   global = true
   state-changes-only = true
```

### Features
- [#236](https://github.com/influxdata/kapacitor/issues/236): Implement batched group by
- [#231](https://github.com/influxdata/kapacitor/pull/231): Add ShiftNode so values can be shifted in time for joining/comparisons.
- [#190](https://github.com/influxdata/kapacitor/issues/190): BREAKING: Deadman's switch now triggers off emitted counts and is grouped by to original grouping of the data.
    The breaking change is that the 'collected' stat is no longer output for `.stats` and has been replaced by `emitted`.
- [#145](https://github.com/influxdata/kapacitor/issues/145): The InfluxDB Out Node now writes data to InfluxDB in buffers.
- [#215](https://github.com/influxdata/kapacitor/issues/215): Add performance metrics to nodes for average execution times and node throughput values.
- [#144](https://github.com/influxdata/kapacitor/issues/144): Can now join streams with differing dimensions using the join.On property.
- [#249](https://github.com/influxdata/kapacitor/issues/249): Can now use InfluxQL functions directly instead of via the MapReduce method. Example `stream.from().count()`.
- [#233](https://github.com/influxdata/kapacitor/issues/233): BREAKING: Now you can use multiple InfluxDB clusters. The config changes to make this possible are breaking. See notes above for changes.
- [#302](https://github.com/influxdata/kapacitor/issues/302): Can now use .Time in alert message.
- [#239](https://github.com/influxdata/kapacitor/issues/239): Support more detailed TLS config when connecting to an InfluxDB host.
- [#323](https://github.com/influxdata/kapacitor/pull/323): Stats for task execution are provided via JSON HTTP request instead of just DOT string. thanks @yosiat
- [#358](https://github.com/influxdata/kapacitor/issues/358): Improved logging. Adds LogNode so any data in a pipeline can be logged.
- [#366](https://github.com/influxdata/kapacitor/issues/366): HttpOutNode now allows chaining methods.


### Bugfixes
- [#199](https://github.com/influxdata/kapacitor/issues/199): BREAKING: Various fixes for the Alerta integration.
    The `event` property has been removed from the Alerta node and is now set as the value of the alert ID.
- [#232](https://github.com/influxdata/kapacitor/issues/232): Better error message for alert integrations. Better error message for VictorOps 404 response.
- [#231](https://github.com/influxdata/kapacitor/issues/231): Fix window logic when there were gaps in the data stream longer than window every value.
- [#213](https://github.com/influxdata/kapacitor/issues/231): Add SourceStreamNode so that yuou must always first call `.from` on the `stream` object before filtering it, so as to not create confusing to understand TICKscripts.
- [#255](https://github.com/influxdata/kapacitor/issues/255): Add OPTIONS handler for task delete method so it can be preflighted.
- [#258](https://github.com/influxdata/kapacitor/issues/258): Fix UDP internal metrics, change subscriptions to use clusterID.
- [#240](https://github.com/influxdata/kapacitor/issues/240): BREAKING: Fix issues with Sensu integration. The breaking change is that the config no longer takes a `url` but rather a `host` option since the communication is raw TCP rather HTTP.
- [#270](https://github.com/influxdata/kapacitor/issues/270): The HTTP server will now gracefully stop.
- [#300](https://github.com/influxdata/kapacitor/issues/300): Add OPTIONS method to /recording endpoint for deletes.
- [#304](https://github.com/influxdata/kapacitor/issues/304): Fix panic if recording query but do not have an InfluxDB instance configured
- [#289](https://github.com/influxdata/kapacitor/issues/289): Add better error handling to batch node.
- [#142](https://github.com/influxdata/kapacitor/issues/142): Fixes bug when defining multiple influxdb hosts.
- [#266](https://github.com/influxdata/kapacitor/issues/266): Fixes error log for HipChat that is not an error.
- [#333](https://github.com/influxdata/kapacitor/issues/333): Fixes hang when replaying with .stats node. Fixes issues with batch and stats.
- [#340](https://github.com/influxdata/kapacitor/issues/340): BREAKING: Decouples global setting for alert handlers from the state changes only setting.
- [#348](https://github.com/influxdata/kapacitor/issues/348): config.go: refactor to simplify structure and fix support for array elements
- [#362](https://github.com/influxdata/kapacitor/issues/362): Fix bug with join tolerance and batches.

## v0.10.1 [2016-02-08]

### Release Notes

This is a bug fix release that fixes many issues releated to the recent 0.10.0 release.
The few additional features are focused on usability improvements from recent feedback.

Improved UDFs, lots of bug fixes and improvements on the API. There was a breaking change for UDFs protobuf messages, see #176.

There was a breaking change to the `define` command, see [#173](https://github.com/influxdata/kapacitor/issues/173) below.

### Features

- [#176](https://github.com/influxdata/kapacitor/issues/176): BREAKING: Improved UDFs and groups. Now it is easy to deal with groups from the UDF process.
    There is a breaking change in the BeginBatch protobuf message for this change.
- [#196](https://github.com/influxdata/kapacitor/issues/196): Adds a 'details' property to the alert node so that the email body can be defined. See also [#75](https://github.com/influxdata/kapacitor/issues/75).
- [#132](https://github.com/influxdata/kapacitor/issues/132): Make is so multiple calls to `where` simply `AND` expressions together instead of replacing or creating extra nodes in the pipeline.
- [#173](https://github.com/influxdata/kapacitor/issues/173): BREAKING: Added a `-no-reload` flag to the define command in the CLI. Now if the task is enabled define will automatically reload it unless `-no-reload` is passed.
- [#194](https://github.com/influxdata/kapacitor/pull/194): Adds Talk integration for alerts. Thanks @wutaizeng!
- [#320](https://github.com/influxdata/kapacitor/pull/320): Upgrade to go 1.6

### Bugfixes

- [#177](https://github.com/influxdata/kapacitor/issues/177): Fix panic for show command on batch tasks.
- [#185](https://github.com/influxdata/kapacitor/issues/185): Fix panic in define command with invalid dbrp value.
- [#195](https://github.com/influxdata/kapacitor/issues/195): Fix panic in where node.
- [#208](https://github.com/influxdata/kapacitor/issues/208): Add default stats dbrp to default subscription excludes.
- [#203](https://github.com/influxdata/kapacitor/issues/203): Fix hang when deleteing invalid batch task.
- [#182](https://github.com/influxdata/kapacitor/issues/182): Fix missing/incorrect Content-Type headers for various HTTP endpoints.
- [#187](https://github.com/influxdata/kapacitor/issues/187): Retry connecting to InfluxDB on startup for up to 5 minutes by default.

## v0.10.0 [2016-01-26]

### Release Notes

This release marks the next major release of Kapacitor.
With this release you can now run your own custom code for processing data within Kapacitor.
See [udf/agent/README.md](https://github.com/influxdata/kapacitor/blob/master/udf/agent/README.md) for more details.

With the addition of UDFs it is now possible to run custom anomaly detection alogrithms suited to your needs.
There are simple examples of how to use UDFs in [udf/agent/examples](https://github.com/influxdata/kapacitor/tree/master/udf/agent/examples/).

The version has jumped significantly so that it is inline with other projects in the TICK stack.
This way you can easily tell which versions of Telegraf, InfluxDB, Chronograf and Kapacitor work together.

See note on a breaking change in the HTTP API below. #163


### Features
- [#137](https://github.com/influxdata/kapacitor/issues/137): Add deadman's switch. Can be setup via TICKscript and globally via configuration.
- [#72](https://github.com/influxdata/kapacitor/issues/72): Add support for User Defined Functions (UDFs).
- [#139](https://github.com/influxdata/kapacitor/issues/139): Alerta.io support thanks! @md14454
- [#85](https://github.com/influxdata/kapacitor/issues/85): Sensu support using JIT clients. Thanks @sstarcher!
- [#141](https://github.com/influxdata/kapacitor/issues/141): Time of day expressions for silencing alerts.

### Bugfixes
- [#153](https://github.com/influxdata/kapacitor/issues/153): Fix panic if referencing non existant field in MapReduce function.
- [#138](https://github.com/influxdata/kapacitor/issues/138): Change over to influxdata github org.
- [#164](https://github.com/influxdata/kapacitor/issues/164): Update imports etc from InfluxDB as per the new meta store/client changes.
- [#163](https://github.com/influxdata/kapacitor/issues/163): BREAKING CHANGE: Removed the 'api/v1' pathing from the HTTP API so that Kapacitor is
    path compatible with InfluxDB. While this is a breaking change the kapacitor cli has been updated accordingly and you will not experience any distruptions unless you
    were calling the HTTP API directly.
- [#147](https://github.com/influxdata/kapacitor/issues/147): Compress .tar archives from builds.

## v0.2.4 [2016-01-07]

### Release Notes

### Features
- [#118](https://github.com/influxdata/kapacitor/issues/118): Can now define multiple handlers of the same type on an AlertNode.
- [#119](https://github.com/influxdata/kapacitor/issues/119): HipChat support thanks! @ericiles *2
- [#113](https://github.com/influxdata/kapacitor/issues/113): OpsGenie support thanks! @ericiles
- [#107](https://github.com/influxdata/kapacitor/issues/107): Enable TICKscript variables to be defined and then referenced from lambda expressions.
        Also fixes various bugs around using regexes.

### Bugfixes
- [#124](https://github.com/influxdata/kapacitor/issues/124): Fix panic where there is an error starting a task.
- [#122](https://github.com/influxdata/kapacitor/issues/122): Fixes panic when using WhereNode.
- [#128](https://github.com/influxdata/kapacitor/issues/128): Fix not sending emails when using recipient list from config.

## v0.2.3 [2015-12-22]

### Release Notes

Bugfix #106 made a breaking change to the internal HTTP API. This was to facilitate integration testing and overall better design.
Now POSTing a recording request will start the recording and immediately return. If you want to wait till it is complete do
a GET for the recording info and it will block until its complete. The kapacitor cli has been updated accordingly.

### Features
- [#96](https://github.com/influxdata/kapacitor/issues/96): Use KAPACITOR_URL env var for setting the kapacitord url in the client.
- [#109](https://github.com/influxdata/kapacitor/pull/109): Add throughput counts to DOT format in `kapacitor show` command, if task is executing.

### Bugfixes
- [#102](https://github.com/influxdata/kapacitor/issues/102): Fix race when start/stoping timeTicker in batch.go
- [#106](https://github.com/influxdata/kapacitor/pull/106): Fix hang when replaying stream recording.


## v0.2.2 [2015-12-16]

### Release Notes

Some bug fixes including one that cause Kapacitor to deadlock.

### Features
- [#83](https://github.com/influxdata/kapacitor/pull/83): Use enterprise usage client, remove deprecated enterprise register and reporting features.

### Bugfixes

- [#86](https://github.com/influxdata/kapacitor/issues/86): Fix dealock form errors in tasks. Also fixes issue where task failures did not get logged.
- [#95](https://github.com/influxdata/kapacitor/pull/95): Fix race in bolt usage when starting enabled tasks at startup.

## v0.2.0 [2015-12-8]

### Release Notes

Major public release.


