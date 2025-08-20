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

# Create header-only interface library for Grafite
add_library(_grafite INTERFACE)

# Check if main grafite.hpp exists
if(NOT EXISTS "${GRAFITE_LIBRARY_DIR}/include/grafite/grafite.hpp")
    message(FATAL_ERROR "Main Grafite header not found: ${GRAFITE_LIBRARY_DIR}/include/grafite.hpp")
endif()

# Only expose the include directory - users should only include grafite.hpp
target_include_directories(_grafite SYSTEM INTERFACE 
    "${GRAFITE_LIBRARY_DIR}/include/grafite"
)

# Documentation note: Only include grafite.hpp in your code
target_compile_definitions(_grafite INTERFACE GRAFITE_MAIN_HEADER_ONLY)

# Add alias for consistent naming
add_library(ch_contrib::grafite ALIAS _grafite)
