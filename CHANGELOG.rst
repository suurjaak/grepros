^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package grepros
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

0.4.4 (2022-03-12)
-------------------
* add support for reading zstd-compressed bagfiles
* use message type definition from ROS1 live topics instead of locally installed
* strip leading "./" from printed filename prefix if grepping working directory
* fix not skipping ROS2 bag if all topics filtered out
* fix making compatible QoS for topic subscriptions

0.4.3 (2022-03-01)
-------------------
* continue subscribing to other live topics even if one causes error
* continue reading from ROS2 bag even if one message type causes error
* subscribe to live topics in ROS2 with QoS matching publisher
* fix not saving publisher QoS profiles in written ROS2 bags
* fix package build not including submodules (#1)

0.4.2 (2022-02-09)
-------------------
* add overwrite=true|false to --write options
* add column-name=rostype:value to --write Parquet options
* add scripts/generate_msgs.py
* create message type definition from .idl if .msg file not available in ROS2
* improve DDS type parsing in ROS2
* handle ROS2 char and byte int8/uint8 reversal vs ROS1
* fix assembling message type full definition in ROS2
* fix processing byte values in ROS2
* fix processing bounded string types in ROS2
* fix inserting chars in Postgres
* fix inserting very large integers in SQLite

0.4.1 (2022-01-08)
-------------------
* refactor database and SQL sinks onto a common base
* allow specifying dialect options in Postgres/SQLite output
* allow overriding table and view names in SQL dialects
* allow specifying field value adapters in SQL dialects
* allow specifying structured type mappings in Parquet output like type-uint8[]="list(uint8())"
* drop meta-table from Postgres output
* drop default value from --publish-prefix option
* fix error in parsing subtypes from message definitions
* fix converting ROS2 temporal messages to seconds/nanoseconds

0.4.0 (2021-12-26)
-------------------
* add --plugin grepros.plugins.parquet (Parquet output)
* add --plugin grepros.plugins.sql (SQL schema output)
* add --plugin grepros.plugins.embag (faster ROS1 bag reader)
* add --reindex-if-unindexed option
* add --every-nth-match option
* add --every-nth-message option
* add --every-nth-interval option
* allow multiple write sinks, combine --write-format and --write-option to --write
* refactor plugins interface
* populate topics.offered_qos_profiles in ROS2 bag output where possible
* fix progress bar afterword not updating when grepping multiple bags
* fix error on empty bag with no messages
* fix error in Postgres output for NaNs in nested JSON values
* fix skipping some messages in ROS1 bag for types with identical hashes
* fix not being able to specify list arguments several times
* ensure no conflicts from changed message types or identical type hashes
* add tests

0.3.5 (2021-12-14)
-------------------
* fix Postgres output not having content

0.3.4 (2021-12-14)
-------------------
* add --write-option message-yaml=true|false, for SQLite output
* speed up SQLite output (~4-8x)
* speed up YAML formatting (~2x)
* fix no engine name in console texts for Postgres output

0.3.3 (2021-12-13)
-------------------
* fix errors in Postgres/SQLite output

0.3.2 (2021-12-12)
-------------------
* rename --write-option commit_interval to commit-interval
* raise Postgres default commit-interval from 100 to 1000
* add --write-option commit-interval support to SQLite output
* add --write-option subtypes=array|all, for Postgres/SQLite output
* speed up SQLite output (~2x)
* refactor Postgres/SQLite sinks onto common base class

0.3.1 (2021-12-06)
-------------------
* add --write-option template=/my/html.template, for HTML output
* add --write-option commit_interval=NUM, for Postgres output
* drop --write-format-template
* fix highlighting subtype arrays

0.3.0 (2021-12-05)
-------------------
* add --write-format postgres
* add --no-verbose option
* add --condition option
* add --plugin option
* add wildcard support to fields in "field=PATTERN"
* use up to nanosecond precision in HTML output timeline
* highlight empty arrays on any-match regardless of type
* select meta-fields as last in SQLite topic views
* fix potential error on using --max-per-topic with live topics
* fix detecting ROS2 bags
* fix using --progress with --live

0.2.5 (2021-11-28)
-------------------
* add --progress option
* match anything by default if no patterns given
* add timeline to HTML output
* auto-detect output format from given filename
* fix breaking too early on --max-per-topic
* fix adding topic views to existing SQLite output database
* fix error on adding message type tables for empty list values in SQLite output
* fix sorting table of contents in HTML output
* do not auto-expand table of contents in HTML output

0.2.4 (2021-11-18)
-------------------
* skip retrieving full message counts from ROS2 bag before any match
* ensure message YAMLs in html output always in color and wrapped at 120 characters
* fix inserting duplicate types-rows when adding to an existing SQLite output file
* improve wrapping lists and nunbers

0.2.3 (2021-11-15)
-------------------
* add --write-format csv
* add --write-format sqlite
* local Python packages no longer required for custom message types in ROS1
* add topic toggle checkboxes to HTML output
* add topic count to live source metainfo
* break early when max matches per topic reached
* improve HTML output

0.2.2 (2021-11-10)
-------------------
* shut down ROS2 live node properly
* better support for ROS2 primitive types
* make HTML output table of contents sortable
* stop requiring unneeded environment variables

0.2.1 (2021-11-09)
-------------------
* add --write-format option, with HTML support
* add --wrap-width option
* add --order-bag-by option
* handle topics with multiple message types
* improve console output wrapping
* fix detecting ROS2 primitive array types
* fix using ROS2 bag start-end timestamps

0.2.0 (2021-11-04)
-------------------
* add ROS2 support
* flush stdout on every print, to avoid buffering in redirected output
* add --ros-time-in option
* add --unique-only option
* rename options --noselect-field and --noprint-field to --no-select-field and --no-print-field

0.1.0 (2021-10-31)
-------------------
* grep for ROS1 bag files and live topics, able to print and publish and write bagfiles
