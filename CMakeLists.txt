cmake_minimum_required(VERSION 3.22)
project(OS_ex3)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(Barrier)

add_library(libMapReduceFramework.a STATIC
        MapReduceFramework.cpp)

add_executable(OS_ex3
        SampleClient.cpp
        Barrier/Barrier.h
        Barrier/Barrier.cpp
        MapReduceClient.h
        MapReduceFramework.h)

target_link_libraries(OS_ex3 "libMapReduceFramework.a")
