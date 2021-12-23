cmake_minimum_required(VERSION 3.8)
project(grepros)

if($ENV{ROS_VERSION} EQUAL "1")
  find_package(catkin REQUIRED COMPONENTS
    genpy rosbag roslib rospy
  )

  catkin_python_setup()

  if(CATKIN_ENABLE_TESTING)
    find_package(rostest REQUIRED)
    find_package(std_msgs REQUIRED)

    catkin_add_nosetests(test/ros1/test_bag_to_console.py)
    catkin_add_nosetests(test/ros1/test_bag_to_bag.py)
    catkin_add_nosetests(test/ros1/test_bag_to_csv.py)
    catkin_add_nosetests(test/ros1/test_bag_to_html.py)
    catkin_add_nosetests(test/ros1/test_bag_to_sqlite.py)
    catkin_add_nosetests(test/ros1/test_bag_to_sql.py)
    add_rostest(test/ros1/test_bag_to_live.launch)
    add_rostest(test/ros1/test_live_to_console.launch)
  endif()

  catkin_package(CATKIN_DEPENDS genpy rosbag roslib rospy)

  catkin_install_python(PROGRAMS
                        scripts/grepros
                        DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
else()
  find_package(ament_cmake REQUIRED)
  find_package(ament_cmake_python REQUIRED)
  find_package(rclpy REQUIRED)
  find_package(rosidl_runtime_py REQUIRED)

  find_package(ament_lint_auto REQUIRED)

  if(BUILD_TESTING)
    find_package(ament_lint_auto REQUIRED)
    ament_lint_auto_find_test_dependencies()
  endif()

  ament_python_install_package(grepros PACKAGE_DIR src/grepros)
  ament_package()
endif()
