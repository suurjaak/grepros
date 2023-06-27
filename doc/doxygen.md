\internal  Entrypoint README for doxygen  \endinternal

# Overview

grep for ROS bag files and live topics: read, filter, export.

Written as a command-line tool, also provides a comprehensive API to use as a library.

Supports both ROS1 and ROS2. ROS environment variables need to be set, at least `ROS_VERSION`.

Supports loading custom plugins, mainly for additional output formats.

## Main class index

| ||
| ----------------------------------------------------------------------- | ---------------------------------------------------
| {@link grepros.api.BaseBag                        grepros.Bag}          | generic ROS bag interface
| {@link grepros.search.Scanner                     grepros.Scanner}      | ROS message grepper
|                                                                         | **Sources**
| {@link grepros.inputs.AppSource                   grepros.AppSource}    | produces messages from iterable or pushed data
| {@link grepros.inputs.BagSource                   grepros.BagSource}    | produces messages from ROS bagfiles
| {@link grepros.inputs.TopicSource                 grepros.TopicSource}  | produces messages from live ROS topics
|                                                                         | **Sinks**
| {@link grepros.outputs.AppSink                    grepros.AppSink}      | provides messages to callback function
| {@link grepros.outputs.BagSink                    grepros.BagSink}      | writes messages to bagfile
| {@link grepros.outputs.ConsoleSink                grepros.ConsoleSink}  | prints messages to console
| {@link grepros.plugins.auto.csv.CsvSink           grepros.CsvSink}      | writes messages to CSV files, each topic separately
| {@link grepros.plugins.auto.html.HtmlSink         grepros.HtmlSink}     | writes messages to an HTML file
| {@link grepros.plugins.mcap.McapSink              grepros.McapSink}     | writes messages to an MCAP bag file
| {@link grepros.outputs.MultiSink                  grepros.MultiSink}    | combines any number of sinks
| {@link grepros.plugins.parquet.ParquetSink        grepros.ParquetSink}  | writes messages to Apache Parquet files
| {@link grepros.plugins.auto.postgres.PostgresSink grepros.PostgresSink} | writes messages to a Postgres database
| {@link grepros.plugins.auto.sqlite.SqliteSink     grepros.SqliteSink}   | writes messages to an SQLite database
| {@link grepros.outputs.TopicSink                  grepros.TopicSink}    | publishes messages to live ROS topics


## Convenience entrypoints

| ||
| ----------------------------------------------------- | -------------------------------------------------------
| {@link grepros.library.grep   grepros.grep}(..)       | yields matching messages from specified source
| {@link grepros.library.source grepros.source}(..)     | returns a {@link grepros.inputs.Source Source} instance
| {@link grepros.library.sink   grepros.sink}(..)       | returns a {@link grepros.outputs.Sink Sink} instance


## Command-line scripts

|                              ||
| ----------------------------- | ----------------------------------------------------------
| generate_msgs                 | Test script, generating and publishing random ROS messages
| grepros                       | Main command-line tool


## Example usage

### Convenience entrypoints

\code{.py}
import grepros
grepros.init()

# Print first message from each bag under path:
for topic, msg, stamp, *_ in grepros.grep(path="my/path", max_count=1):
    print(topic, stamp, msg)

# Write one message from each live topic to an HTML file:
with grepros.source(live=True, max_per_topic=1) as source, \
     grepros.sink("my.html") as sink:
    for topic, msg, stamp in source: sink.emit(topic, msg, stamp)
\endcode


### Working with bags

\code{.py}
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
    for topic, msg, stamp, match, index in grepros.grep(live=True, nth_interval=60):
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
\endcode


### Sources and sinks

\code{.py}
import grepros
grepros.init()

# Write all bags in directory to Postgres database:
with grepros.PostgresSink("username=postgres dbname=postgres") as sink:
    for data in grepros.BagSource(path="/tmp/bags"):
        sink.emit(*data)

# Grep live topics:
for topic, msg, stamp, match, index in grepros.TopicSource(topic="/diagnostics", pattern="cpu"):
    print("MESSAGE #%s MATCH: " % index, match)

# Subscribe to live topics and write to bag:
with grepros.TopicSource(topic="/rosout") as source, \
     grepros.Bag("my.bag", "w") as bag:
    for topic, msg, stamp, *_ in grepros.Scanner(pattern="error").find(source)
        bag.write(topic, msg, stamp)

# Write all pointclouds from bags in directory to SQLite database:
with grepros.BagSource(path="/tmp/bags") as source, \
     grepros.SqliteSink("my.sqlite") as sink:
    total = grepros.Scanner(type="*/pointcloud*").work(source, sink)
    print("Messages written: %s" % total)
\endcode
