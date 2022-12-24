\internal  Entrypoint README for doxygen  \endinternal

grep for ROS bag files and live topics.

Searches through ROS messages and matches any message field value by regular
expression patterns or plain text, regardless of field type.
Can also look for specific values in specific message fields only.

By default, matches are printed to console. Additionally, matches can be written
to a bagfile or HTML/CSV/MCAP/Parquet/Postgres/SQL/SQLite, or published to live topics.

Supports both ROS1 and ROS2. ROS environment variables need to be set, at least `ROS_VERSION`.

Supports loading custom plugins, mainly for additional output formats.


| Module Index                                                                            ||
| ----------------------------- | ----------------------------------------------------------
| grepros.api                   | ROS interface, shared facade for ROS1 and ROS2
| grepros.common                | Common utilities
| grepros.inputs                | Input sources for search content
| grepros.main                  | Program main interface
| grepros.outputs               | Main outputs for search results
| grepros.ros1                  | ROS1 interface
| grepros.ros2                  | ROS2 interface
| grepros.search                | Search core


#### Command-line Scripts

|                              ||
| ----------------------------- | ----------------------------------------------------------
| generate_msgs                 | Test script, generating and publishing random ROS messages
| grepros                       | Main command-line tool

#### Plugins Interface

##### Auto-loaded plugins

|                              ||
| ----------------------------- | ----------------------------------------------------------
| grepros.plugins.auto.csv      | CSV output for search results
| grepros.plugins.auto.dbbase   | Shared functionality for database sinks
| grepros.plugins.auto.html     | HTML output for search results
| grepros.plugins.auto.postgres | Sink plugin for dumping messages to a Postgres database
| grepros.plugins.auto.sqlbase  | Base class for producing SQL for topics and messages
| grepros.plugins.auto.sqlite   | SQLite output for search results

##### Explicitly loaded plugins

|                              ||
| ----------------------------- | ----------------------------------------------------------
| grepros.plugins.embag         | ROS1 bag reader plugin using the `embag` library
| grepros.plugins.mcap          | MCAP input and output
| grepros.plugins.parquet       | Parquet output for search results
| grepros.plugins.sql           | SQL schema output for search results
