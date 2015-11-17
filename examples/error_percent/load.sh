#!/bin/bash

set -e

# This script uses influx_stress to create ~11 days of page view/error data with a data point every 10s.
# It creates data for two measurements 'errors' and 'views' in a database called 'pages'.
#  The 'errors' measurement represent to number of page views that resulted in an error.
#  The 'views' measurement represent to number of page views that did not result in an error.
DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
cd $DIR

cat > pages.toml << EOF
channel_buffer_size = 100000

[write]
  concurrency = 1
  batch_size = 5000
  batch_interval = "0s"
  database = "pages"
  precision = "n"
  address = "localhost:8086"
  reset_database = true
  # The date for the first point that is written into influx
  start_date = "2015-Jan-01"

# Describes the schema for series that will be
# written
[[series]]
  tick = "10s"
  jitter = false
  point_count = 100000
  measurement = "errors"
  series_count = 100

  # Defines a tag for a series
  [[series.tag]]
    key = "page"
    value = "page"

  [[series.field]]
    key = "value"
    type = "int-range"
    min = 0
    max = 200

[[series]]
  tick = "10s"
  jitter = false
  point_count = 100000
  measurement = "views"
  series_count = 100

  # Defines a tag for a series
  [[series.tag]]
    key = "page"
    value = "page"

  [[series.field]]
    key = "value"
    type = "int-range"
    min = 0
    max = 1000
EOF

influx_stress -test pages.toml
rm pages.toml
