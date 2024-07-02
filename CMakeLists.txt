cmake_minimum_required(VERSION 3.22)
project(OS_ex3)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)
include_directories(Barrier)

add_executable(OS_ex3

        MapReduceFramework.cpp
        Barrier/Barrier.h
        MapReduceClient.h
        MapReduceFramework.h)
