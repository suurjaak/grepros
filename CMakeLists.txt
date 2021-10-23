cmake_minimum_required(VERSION 3.8)
project(grep4ros)

find_package(catkin REQUIRED COMPONENTS
  genpy rosbag rospy
)

catkin_python_setup()

catkin_package(CATKIN_DEPENDS genpy rosbag rospy)

catkin_install_python(PROGRAMS
                      scripts/grep4ros
                      DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
