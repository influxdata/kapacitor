# Changelog

## v0.2.3 [unreleased]

### Release Notes

### Features
- [#96](https://github.com/influxdb/kapacitor/issues/96): Use KAPACITOR_URL env var for setting the kapacitord url in the client.

### Bugfixes

## v0.2.2 [2015-12-16]

### Release Notes

Some bug fixes including one that cause Kapacitor to deadlock.

### Features
- [#83](https://github.com/influxdb/kapacitor/pull/83): Use enterprise usage client, remove deprecated enterprise register and reporting features.

### Bugfixes

- [#86](https://github.com/influxdb/kapacitor/issues/86): Fix dealock form errors in tasks. Also fixes issue where task failures did not get logged.
- [#95](https://github.com/influxdb/kapacitor/pull/95): Fix race in bolt usage when starting enabled tasks at startup.

## v0.2.0 [2015-12-8]

### Release Notes

Major public release.


