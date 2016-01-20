# Changelog

## v0.10.0 [unreleased]

### Release Notes

This release marks the next major release of Kapacitor.
With this release you can now run your own custom code for processing data within Kapacitor.
See [udf/agent/README.md](https://github.com/influxdata/kapacitor/blob/master/udf/agent/README.md) for more details.

With the addition of UDFs it is now possible to run custom anomaly detection alogrithms suited to your needs.
There are simple examples of how to use UDFs in [udf/agent/examples](https://github.com/influxdata/kapacitor/tree/master/udf/agent/examples/).

The version has jumped significantly so that it is inline with other projects in the TICK stack.
This way you can easily tell which versions of Telegraf, InfluxDB, Chronograf and Kapacitor work together.


### Features
- [#137](https://github.com/influxdata/kapacitor/issues/137): Add deadman's switch. Can be setup via TICKscript and globally via configuration.
- [#72](https://github.com/influxdata/kapacitor/issues/72): Add support for User Defined Functions (UDFs).
- [#139](https://github.com/influxdata/kapacitor/issues/139): Alerta.io support thanks! @md14454

### Bugfixes
- [#153](https://github.com/influxdata/kapacitor/issues/153): Fix panic if referencing non existant field in MapReduce function.
- [#138](https://github.com/influxdata/kapacitor/issues/138): Change over to influxdata github org.

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


