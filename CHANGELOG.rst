^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Changelog for package grepros
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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
