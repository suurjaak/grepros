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

    catkin_add_nosetests(test/test_bag_to_console.py)
    catkin_add_nosetests(test/test_bag_to_bag.py)
    catkin_add_nosetests(test/test_bag_to_csv.py)
    catkin_add_nosetests(test/test_bag_to_html.py)
    catkin_add_nosetests(test/test_bag_to_sqlite.py)
    catkin_add_nosetests(test/test_bag_to_sql.py)
    add_rostest(test/test_bag_to_live.launch)
    add_rostest(test/test_live_to_console.launch)
    add_rostest(test/test_source_filters.launch)
    catkin_add_nosetests(test/test_api.py)
    catkin_add_nosetests(test/test_common.py)
    catkin_add_nosetests(test/test_expression.py)
    catkin_add_nosetests(test/test_library.py)
  endif()

  catkin_package(CATKIN_DEPENDS genpy rosbag roslib rospy)

  catkin_install_python(PROGRAMS
                        scripts/${PROJECT_NAME}
                        DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
else()
  find_package(ament_cmake REQUIRED)
  find_package(ament_cmake_python REQUIRED)
  find_package(rclpy REQUIRED)
  find_package(rosidl_parser REQUIRED)
  find_package(rosidl_runtime_py REQUIRED)

  find_package(ament_lint_auto REQUIRED)

  if(BUILD_TESTING)
    find_package(ament_cmake_pytest REQUIRED)
    find_package(std_msgs REQUIRED)

    ament_add_pytest_test(test_bag_to_console  test/test_bag_to_console.py)
    ament_add_pytest_test(test_bag_to_bag      test/test_bag_to_bag.py)
    ament_add_pytest_test(test_bag_to_csv      test/test_bag_to_csv.py)
    ament_add_pytest_test(test_bag_to_html     test/test_bag_to_html.py)
    ament_add_pytest_test(test_bag_to_sqlite   test/test_bag_to_sqlite.py)
    ament_add_pytest_test(test_bag_to_sql      test/test_bag_to_sql.py)
    ament_add_pytest_test(test_bag_to_live     test/test_bag_to_live.py)
    ament_add_pytest_test(test_live_to_console test/test_live_to_console.py)
    ament_add_pytest_test(test_source_filters  test/test_source_filters.py)
    ament_add_pytest_test(test_api             test/test_api.py)
    ament_add_pytest_test(test_common          test/test_common.py)
    ament_add_pytest_test(test_expression      test/test_expression.py)
    ament_add_pytest_test(test_library         test/test_library.py)
  endif()

  ament_python_install_package(${PROJECT_NAME} PACKAGE_DIR src/${PROJECT_NAME})
  ament_package()
endif()
