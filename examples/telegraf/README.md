# `telegraf`

`telegraf` contains tickscripts for individual [telegraf](github.com/influxdata/telegraf) plugins. The directories at the root of the repo are named for the telegraf plugin they reference. There is a common structure for each of the tickscripts so as to be consistent and easily understandable for beginners. Each of the scripts are broken up into a few different sections:

* Comments
  - Name of the alert
  - Name of field to alert on and other fields available from the telegraf plugin
  - Full telegraf configuration for the plugin the tickscript references including comments
  - Commands for `define`-ing and `enable`-ing the script from the root of the repo
* Parameters
  - A list of variables to make customization easy
* Dataframe
  - A definition of the data to alert on 
* Thresholds
  - Define the thresholds or expressions to alert on 
* Alert
  - Where to send the alert. All scripts `.log()` by default.
  - The tickscripts are written to make it easy to swap in whatever alert output you need. Just change the `alert.log('/tmp/{alert_name}.txt')` line in each tickscipt to your desired alert output. A full listing of outputs with code samples is available in the [kapacitor documentation](https://docs.influxdata.com/kapacitor/v0.13/nodes/alert_node/).
  
> **On Alert Volume:** These alerts may be very noisy or quiet depending on your environment. They are meant to be starting points for alerts with all the knobs easily adjustable from the Parameters. Many users will also want to eliminate the `.info()` level of logging. It is included here for completeness.

> **On Verbosity:** These scripts are meant as templates for users who are new to writing tickscripts. All of the examples here can be written as one large stream. See the [documentation](https://docs.influxdata.com/kapacitor/v0.13/) for examples and full tick syntax.
  
### Batch script example

```javascript
// {alert_name}

// metric: {alert_metric}
// available_fields: [[other_telegraf_fields]]

// TELEGRAF CONFIGURATION
// [inputs.{plugin}]
//   # Full configuration

// DEFINE: kapacitor define {alert_name} -type batch -tick {plugin}/{alert_name}.tick -dbrp telegraf.autogen
// ENABLE: kapacitor enable {alert_name}

// Parameters
var info = {info_level} 
var warn = {warn_level}
var crit = {crit_level}
var infoSig = 2.5
var warnSig = 3
var critSig = 3.5
var period = 10s
var every = 10s

// Dataframe
var data = batch
  |query('''{InfluxQL_Query}''')
    .period(period)
    .every(every)
    .groupBy('host')
        
// Thresholds
var alert = data
  |eval(lambda: sigma("stat"))
    .as('sigma')
    .keep()
  |alert()
    .id('{{ index .Tags "host"}}/{alert_metric}')
    .message('{{ .ID }}:{{ index .Fields "stat" }}')
    .info(lambda: "stat" > info OR "sigma" > infoSig)
    .warn(lambda: "stat" > warn OR "sigma" > warnSig)
    .crit(lambda: "stat" > crit OR "sigma" > critSig)

// Alert
alert
  .log('/tmp/{alert_name}_log.txt')

```

### Stream script example

```javascript
// {alert_name}

// metric: {alert_metric}
// available_fields: [[other_telegraf_fields]]

// TELEGRAF CONFIGURATION
// [inputs.{plugin}]
//   # full configuration

// DEFINE: kapacitor define {alert_name} -type batch -tick {plugin}/{alert_name}.tick -dbrp telegraf.autogen
// ENABLE: kapacitor enable {alert_name}

// Parameters
var info = {info_level} 
var warn = {warn_level}
var crit = {crit_level}
var infoSig = 2.5
var warnSig = 3
var critSig = 3.5
var period = 10s
var every = 10s

// Dataframe
var data = stream
  |from()
    .database('telegraf')
    .retentionPolicy('autogen')
    .measurement({plugin})
    .groupBy('host')
  |window()
    .period(period)
    .every(every)
  |mean({alert_metric})
    .as("stat")
    
// Thresholds
var alert = data
  |eval(lambda: sigma("stat"))
    .as('sigma')
    .keep()
  |alert()
    .id('{{ index .Tags "host"}}/{alert_metric}')
    .message('{{ .ID }}:{{ index .Fields "stat" }}')
    .info(lambda: "stat" > info OR "sigma" > infoSig)
    .warn(lambda: "stat" > warn OR "sigma" > warnSig)
    .crit(lambda: "stat" > crit OR "sigma" > critSig)

// Alert
alert
  .log('/tmp/{alert_name}_log.txt')
```