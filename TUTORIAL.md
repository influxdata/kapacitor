# Getting Started with Kapacitor

This document will walk you through getting started with two simple use cases of Kapacitor.


## Alert on high cpu usage

Classic example, how to get an alert when a server is over loaded.
The following will walk you through how to get data into Kapacitor and how to setup and alert based on that stream of data.
All along showcasing some of the neat features.


First we need to get data into Kapacitor.
This can be done simply via [Telegraf](https://github.com/influxdb/telegraf).
Since we are concerned only about cpu right now lets just use this simple configuration:

```
[agent]
    interval = "1s"

[outputs]

# Configuration to send data to Kapacitor.
# Since Kapacitor acts like an InfluxDB server the configuration is the same.
[outputs.influxdb]
    # Note the port 9092, this is the default port that Kapacitor uses.
    urls = ["http://localhost:9092"]
    database = "telegraf"
    user_agent = "telegraf"

# Read metrics about cpu usage
[cpu]
    percpu = false
    totalcpu = true
    drop = ["cpu_time"]

```

Go ahead an start Telegraf with the above configuration.

```sh
$ telegraf -config telegraf.conf
```

It will complain about not being able to connect to Kapacitor but thats fine, it will keep trying.


Now lets start Kapacitor

```sh
$ kapacitord
```

That's it. In a sec Telegraf will connect to Kapacitor and start sending it cpu metrics.


Now we need to tell Kapacitor what to do.
Kapacitor's behavior is very dynamic and so it not controlled via configuration but through an HTTP API.
We provide a simple cli utility to call the API to tell Kapacitor what to do.

We want to first create a snapshot of data for testing.

```sh
$ rid=$(kapacitor record stream -duration 60s) # save the id for later use
$ echo $rid
RECORDING_ID_HERE
```

OK, so we want to get an alert if the CPU usage gets too high.
We can define that like so:

```
stream
    // Select just the cpu_usage_idle measurement
    .from("cpu_usage_idle")
    .alert()
        // We are using idle so we want to check
        // if idle drops below 70% (aka cpu used > 30%)
        .crit("value <  70")
        // Post the data for the point to a URL
        .post("http://localhost:8000");
```


The above script is called a `TICK` script.
It is written in a custom language that makes it easy to define actions on a series of data.
Go ahead and save the script to a file called `cpu_idle_alert.tick`.

Now that we have our `TICK` script we need to hand it to Kapacitor so it can run it.

```sh
$ kapacitor define -name cpu_alert -type streamer -tick cpu_idle_alert.tick
```

Here we have defined a `task` for Kapacitor to run. The `task` has a `name`, `type`, and a `tick` script.
The name needs to be unique and the type is `streamer` in this case since we are streaming data from Telegraf to Kapacitor.


Since the `alert` is a POST to a url we need to give Kapacitor something to hit.

In a seperate terminal run this:

```sh
$ # Print to STDOUT anything POSTed to http://localhost:8000
$ mkfifo fifo
$ cat fifo | nc -k -l 8000 | tee fifo
```

You can `rm fifo` once you are done.


Now we want to see it in action. Replay the recording from a bit ago to the task called `cpu_alert`.

```sh
$ kapacitor replay -id $rid -name cpu_alert -fast
```

Did you catch any alerts? Maybe not if your system wasn't too busy during the recording.
If not then lets lower the threshold so we will see some alerts.

Note the `-fast` flag tells Kapacitor to replay the data as fast as possible but it still emulates the time in the recording.
Without the `-fast` Kapacitor would replay the data in real time.

Edit the `.crit("value < 70")` line to be `.crit("value < 99")`.
Now if your system is at least 1% busy you will get an alert.

Redefine the `task` so that Kapacitor knows about your update.

```sh
$ kapacitor define -name cpu_alert -type streamer -tick cpu_idle_alert.tick
$ # Now run replay the data agian and see if we go any alerts.
$ kapacitor replay -id $rid -name cpu_alert -fast
```


Since we recorded a snapshot of the data we can test again and again with the exact same dataset. 
This is powerful for both reproducing bugs with your `TICK` scripts or just knowing that the data isn't changing with each test to keep your sanity.
Run the replay again if you like to see that you get the exact same alerts.


But now we want to see it in action with the live data.
`Enable` your task so it starts working on the live data stream.

```sh
$ kapacitor enable cpu_alert
```

Now just about every second you are probably getting an alert that your system is busy.
That's way to noisy: we could just move the threshold back but that isn't good enough.
We want to only get alerts when things are really bad. Try this:

```
stream
    .from("cpu_usage_idle")
    .alert()
        .crit("sigma(value) >  3")
        .post("http://localhost:8000");
```

Just like that we have told Kapacitor to only alert us if the current value is more than `3 sigma` away from the running mean.
Now if the system cpu climbs throughout the day and drops throughout the night you will still get and alert if it spikes at night or drops during the day!


Stop the noise!

```sh
$ kapacitor define -name cpu_alert -type streamer -tick cpu_idle_alert.tick
$ # The old task definition continues to run until you disable/enable the task.
$ kapacitor disable cpu_alert
$ kapacitor enable cpu_alert
```

What about aggregating our alerts?
If the cpu data coming from Telegraf were tagged with a `service` name then we could do something like this.

```
stream
    .from("cpu_usage_idle")
    .groupBy("service")
    .window()
        .period(10s)
        .every(5s)
    .mapReduce(influxql.mean("value"))
    .apply(expr("sigma", "sigma(value)"))
    .alert()
        .info("sigma > 2")
        .warn("sigma > 2.5")
        .crit("sigma > 3")
        .post("http://localhost:8000");
```

This `TICK` script alerts if the `mean` idle cpu, over the last `10s` `window` for each `service` group is `3` sigma away from the running mean, every `5s`.
Wow, just like that we are aggregating across potentially thousands of servers and getting alerts that are actionable, not just a bunch of noise.
Go ahead and try it out.




