Using as API
------------

grepros can also be used as a Python library for reading, matching and writing
ROS messages, with functionality for converting and exporting messages in various formats.

Full API documentation available at https://suurjaak.github.io/grepros.

### Convenience entrypoint functions

```python
import grepros
grepros.init()

# Print first message from each bag under path:
for topic, msg, stamp, *_ in grepros.grep(path="my/path", max_count=1):
    print(topic, stamp, msg)

# Write one message from each live topic to an HTML file:
with grepros.source(live=True, max_per_topic=1) as source, \
     grepros.sink("my.html") as sink:
    for topic, msg, stamp in source: sink.emit(topic, msg, stamp)
```

### Working with bags

```python
import grepros
grepros.init()

# Read and write bags:
with grepros.Bag("my.bag") as inbag, grepros.Bag("my.mcap", mode="w") as outbag:
    for topic, msg, stamp in inbag:
        outbag.write(topic, msg, stamp)  # Convert ROS1 bag to MCAP

# Find messages in bag:
bag  = grepros.Bag("my.bag")
scan = grepros.Scanner(topic="/diagnostics", pattern="temperature")
for topic, msg, stamp, match in scan.find(bag, highlight=True):
    print("MATCH: ", topic, stamp, match)

# Write live topics to bag, no more than once a minute per topic:
with grepros.Bag("my.bag", "w") as bag:
    for topic, msg, stamp, *_ in grepros.grep(live=True, nth_interval=60):
        bag.write(topic, msg, stamp)

# Find messages +- 2 minutes around first pointcloud message in bag:
with grepros.Bag("my.bag") as bag:
    _, _, stamp, *_ = next(grepros.grep(bag, type="*/pointcloud*"))
    delta = grepros.api.make_duration(secs=120)
    args = dict(start_time=stamp - delta, end_time=stamp + delta)
    for topic, msg, stamp, *_ in grepros.grep(bag, **args):
        print("%s [%s] %s" % (topic, stamp, msg))

# Bag API conveniences:
with grepros.Bag("my.bag") as bag:
    print("Messages in bag: ", len(bag))
    if "/my/topic" in bag:
        print("/my/topic messages in bag: ", len(bag["/my/topic"]))
    for topic in bag.topics:
        print("Topic: ", topic)
        for topic, msg, stamp in bag[topic]:
            print(msg)
```

### Sources and sinks

```python
import grepros
grepros.init()

# Write all bags in directory to Postgres database:
with grepros.PostgresSink("username=postgres dbname=postgres") as sink:
    for data in grepros.BagSource(path="/tmp/bags"):
        sink.emit(*data)

# Grep live topics:
for topic, msg, stamp, match, index in grepros.LiveSource(topic="/diagnostics", pattern="cpu"):
    print("MESSAGE #%s MATCH: " % index, match)

# Subscribe to live topics and write to bag:
with grepros.LiveSource(topic="/rosout") as source, \
     grepros.Bag("my.bag", "w") as bag:
    for topic, msg, stamp, *_ in grepros.Scanner(pattern="error").find(source)
        bag.write(topic, msg, stamp)

# Write all pointclouds from bags in directory to SQLite database:
with grepros.BagSource(path="/tmp/bags") as source, \
     grepros.SqliteSink("my.sqlite") as sink:
    total = grepros.Scanner(type="*/pointcloud*").work(source, sink)
    print("Messages written: %s" % total)
```

Output sink `write_options` arguments can be given with underscores
instead of dashes, e.g. `"rollover_size"` instead of `"rollover-size"`.

### Main classes

Source classes:

- [`grepros.AppSource`](https://suurjaak.github.io/grepros/api/classgrepros_1_1inputs_1_1_app_source.html): produces messages from iterable or pushed data
- [`grepros.BagSource`](https://suurjaak.github.io/grepros/api/classgrepros_1_1inputs_1_1_bag_source.html): produces messages from ROS bagfiles
- [`grepros.LiveSource`](https://suurjaak.github.io/grepros/api/classgrepros_1_1inputs_1_1_live_source.html): produces messages from live ROS topics

Sink classes:

- [`grepros.AppSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1outputs_1_1_app_sink.html): provides messages to callback function
- [`grepros.BagSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1outputs_1_1_bag_sink.html): writes messages to bagfile
- [`grepros.ConsoleSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1outputs_1_1_console_sink.html): prints messages to console
- [`grepros.CsvSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1auto_1_1csv_1_1_csv_sink.html): writes messages to CSV files, each topic to a separate file
- [`grepros.HtmlSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1auto_1_1html_1_1_html_sink.html): writes messages to an HTML file
- [`grepros.LiveSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1outputs_1_1_live_sink.html): publishes messages to live ROS topics
- [`grepros.McapSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1mcap_1_1_mcap_sink.html): writes messages to an MCAP file
- [`grepros.MultiSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1outputs_1_1_multi_sink.html): combines any number of sinks
- [`grepros.ParquetSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1parquet_1_1_parquet_sink.html): writes messages to Apache Parquet files
- [`grepros.PostgresSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1auto_1_1postgres_1_1_postgres_sink.html): writes messages to a Postgres database
- [`grepros.SqliteSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1auto_1_1sqlite_1_1_sqlite_sink.html): writes messages to an SQLite database
- [`grepros.SqlSink`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1sql_1_1_sql_sink.html): writes an SQL schema file for message type tables and topic views

[`grepros.Bag`](https://suurjaak.github.io/grepros/api/classgrepros_1_1api_1_1_base_bag.html): generic ROS bag interface.<br />
[`grepros.Scanner`](https://suurjaak.github.io/grepros/api/classgrepros_1_1search_1_1_scanner.html): ROS message grepper.

Format-specific bag classes:

- [`grepros.ros1.Bag`](https://suurjaak.github.io/grepros/api/classgrepros_1_1ros1_1_1_bag.html): ROS1 bag reader and writer in .bag format
- [`grepros.ros2.Bag`](https://suurjaak.github.io/grepros/api/classgrepros_1_1ros2_1_1_bag.html): ROS2 bag reader and writer in .db3 SQLite format
- [`grepros.plugins.embag.EmbagReader`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1embag_1_1_embag_reader.html): ROS1 bag reader using the [embag](https://github.com/embarktrucks/embag) library
- [`grepros.plugins.mcap.McapBag`](https://suurjaak.github.io/grepros/api/classgrepros_1_1plugins_1_1_mcapbag.html): ROS1/ROS2 bag reader and writer in MCAP format
