# Changelog

## v0.11.0 [unreleased]

### Release Notes

### Features
- [#236](https://github.com/influxdata/kapacitor/issues/236): Implement batched group by
- [#231](https://github.com/influxdata/kapacitor/pull/231): Add ShiftNode so values can be shifted in time for joining/comparisons.
- [#190](https://github.com/influxdata/kapacitor/issues/190): BREAKING: Deadman's switch now triggers off emitted counts and is grouped by to original grouping of the data.
    The breaking change is that the 'collected' stat is no longer output for `.stats` and has been replaced by `emitted`.
- [#145](https://github.com/influxdata/kapacitor/issues/145): The InfluxDB Out Node now writes data to InfluxDB in buffers.


### Bugfixes
- [#199](https://github.com/influxdata/kapacitor/issues/199): BREAKING: Various fixes for the Alerta integration.
    The `event` property has been removed from the Alerta node and is now set as the value of the alert ID.
- [#232](https://github.com/influxdata/kapacitor/issues/232): Better error message for alert integrations. Better error message for VictorOps 404 response.
- [#231](https://github.com/influxdata/kapacitor/issues/231): Fix window logic when there were gaps in the data stream longer than window every value.

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


