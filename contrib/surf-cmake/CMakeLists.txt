option(ENABLE_SURF "Enable SuRF" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SURF)
    message(STATUS "Not using SuRF")
    return()
endif()

message(STATUS "Using SuRF")

set(SURF_LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/SuRF")

# Check if SuRF directory exists
if(NOT EXISTS "${SURF_LIBRARY_DIR}/include")
    message(FATAL_ERROR "SuRF include directory not found at ${SURF_LIBRARY_DIR}/include")
endif()

# Create header-only interface library for SuRF
add_library(_surf INTERFACE)

# Check if main surf.hpp exists
if(NOT EXISTS "${SURF_LIBRARY_DIR}/include/surf.hpp")
    message(FATAL_ERROR "Main SuRF header not found: ${SURF_LIBRARY_DIR}/include/surf.hpp")
endif()

# Only expose the include directory - users should only include surf.hpp
target_include_directories(_surf SYSTEM INTERFACE 
    "${SURF_LIBRARY_DIR}/include"
)

# Documentation note: Only include surf.hpp in your code
target_compile_definitions(_surf INTERFACE SURF_MAIN_HEADER_ONLY)

# Add alias for consistent naming
add_library(ch_contrib::surf ALIAS _surf)
