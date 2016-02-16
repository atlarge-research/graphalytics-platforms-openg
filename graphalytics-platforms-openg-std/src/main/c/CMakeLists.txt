project (OpenGApps CXX)
cmake_minimum_required (VERSION 2.6)

if (NOT OPENG_HOME)
    set (OPENG_HOME $ENV{OPENG_HOME})
endif ()

if (NOT OPENG_HOME)
    message(FATAL_ERROR "Before running cmake, please specify OPENG_HOME as environment variable or use -DOPENG_HOME")
endif ()

if (GRANULA)
    add_definitions(-DGRANULA=1)
endif ()

include_directories(SYSTEM "${OPENG_HOME}/common")
include_directories(SYSTEM "${OPENG_HOME}/openG")

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x -Wall -Wno-deprecated -O3 -DNO_PFM -DENABLE_OUTPUT -fopenmp")

add_executable (bfs bfs.cpp)
add_executable (pr pr.cpp)
add_executable (cdlp cdlp.cpp)
add_executable (wcc wcc.cpp)
add_executable (lcc lcc.cpp)
add_executable (sssp sssp.cpp)

