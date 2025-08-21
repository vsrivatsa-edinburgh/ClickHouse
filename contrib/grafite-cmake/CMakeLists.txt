cmake_minimum_required(VERSION 3.16.3)
project(Grafite
        VERSION 1.0
        HOMEPAGE_URL https://github.com/marcocosta97/grafite)

option(ENABLE_GRAFITE "Enable Grafite" ${ENABLE_LIBRARIES})

if (NOT ENABLE_GRAFITE)
    message(STATUS "Not using Grafite")
    return()
endif()

message(STATUS "Using Grafite")

set(GRAFITE_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/grafite")

# Check if Grafite directory exists
if(NOT EXISTS "${GRAFITE_LIBRARY_DIR}/include")
    message(FATAL_ERROR "Grafite include directory not found at ${GRAFITE_LIBRARY_DIR}/include")
endif()

# Header-only interface library for Grafite (keep as requested)
add_library(_grafite INTERFACE)
if(NOT EXISTS "${GRAFITE_LIBRARY_DIR}/include/grafite/grafite.hpp")
    message(FATAL_ERROR "Main Grafite header not found: ${GRAFITE_LIBRARY_DIR}/include/grafite/grafite.hpp")
endif()
target_include_directories(_grafite SYSTEM INTERFACE "${GRAFITE_LIBRARY_DIR}/include/grafite")

# Main interface library for full feature set
list(APPEND SUCCINCT_LIBS "sux" "sdsl-lite")
set(CMAKE_CXX_STANDARD 17)
if (CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
else()
    set(USE_MULTI_THREADED OFF)
endif ()

if ("sux" IN_LIST SUCCINCT_LIBS)
    message(STATUS "Using sux")
    target_compile_definitions(_grafite INTERFACE -DSUCCINCT_LIB_SUX)
    target_include_directories(_grafite INTERFACE ${GRAFITE_LIBRARY_DIR}/include/grafite ${GRAFITE_LIBRARY_DIR}/lib/sux)
endif()
if ("sdsl-lite" IN_LIST SUCCINCT_LIBS)
    message(STATUS "Using sdsl-lite")
    target_compile_definitions(_grafite INTERFACE -DSUCCINCT_LIB_SDSL)
    target_include_directories(_grafite INTERFACE ${GRAFITE_LIBRARY_DIR}/include/grafite ${GRAFITE_LIBRARY_DIR}/lib/sdsl-lite/include)
endif()

# Documentation note: Only include grafite.hpp in your code
target_compile_definitions(_grafite INTERFACE GRAFITE_MAIN_HEADER_ONLY)

# Add alias for consistent naming
add_library(ch_contrib::grafite ALIAS _grafite)
