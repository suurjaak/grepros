cmake_minimum_required(VERSION 3.8)
project(grepros)

find_package(catkin REQUIRED COMPONENTS
  genpy rosbag roslib rosmsg rospy
)

catkin_python_setup()

catkin_package(CATKIN_DEPENDS genpy rosbag roslib rosmsg rospy)

catkin_install_python(PROGRAMS
                      scripts/grepros
                      DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
