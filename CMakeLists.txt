cmake_minimum_required(VERSION 3.26 FATAL_ERROR) # 3.26 For now until XGBOOST Removes FindCUDA

option(BUILD_SHARED_LIBS "Build libraries as shared instead of static" ON)
message(STATUS "Building libraries as ${BUILD_SHARED_LIBS}")
if(BUILD_SHARED_LIBS)
  message(STATUS "Building SHARED libraries")
else()
  message(STATUS "Building STATIC libraries")
endif()

# Set binary and library output directories to keep build files separate from source
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Set PIC only when building shared libraries
if(BUILD_SHARED_LIBS)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
else()
    set(CMAKE_POSITION_INDEPENDENT_CODE OFF)
endif()

set(PROJECT_VERSION 0.1.0)
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

# Ensure try_compile checks inherit the configured toolchain and compilers
set(CMAKE_TRY_COMPILE_PLATFORM_VARIABLES
    CMAKE_C_COMPILER
    CMAKE_CXX_COMPILER
    CMAKE_C_FLAGS
    CMAKE_CXX_FLAGS
    CMAKE_TOOLCHAIN_FILE
    VCPKG_CHAINLOAD_TOOLCHAIN_FILE
    VCPKG_TARGET_TRIPLET
    VCPKG_HOST_TRIPLET
)

# Prefer NEW behavior where try_compile preserves toolchain settings (CMake >= 3.27)
if(POLICY CMP0137)
  cmake_policy(SET CMP0137 NEW)
endif()

option(BUILD_TEST OFF)
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)
option(BUILD_EXAMPLES "Build the examples" OFF)

project(EpochFolio VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23) # REQUIRED DUE TO CUDA
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (BUILD_TEST)
  # Find Catch2 for both tests and benchmarks
  find_package(Catch2 3 REQUIRED)
  find_package(trompeloeil 47 REQUIRED)

  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage")
  enable_testing()
endif()
# EpochProtos is now included via add_subdirectory(thirdparty/epoch_protos) below

if (GIT_TOKEN)
    SET(REPO_URL "https://${GIT_TOKEN}:@github.com")
else ()
    SET(REPO_URL "https://github.com")
endif ()

# Include the EpochCore dependency
include(${PROJECT_SOURCE_DIR}/cmake/EpochFlow.cmake)

set(EPOCH_FOLIO_LIBRARY_NAME "epoch_folio")
add_library(epoch_folio "")
add_library(epoch::folio ALIAS epoch_folio)

# Configure version for shared libraries
set_target_properties(${EPOCH_FOLIO_LIBRARY_NAME} PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
)

# For shared libraries, export all symbols by default (Linux-friendly approach)
if(BUILD_SHARED_LIBS)
    set_target_properties(epoch_folio PROPERTIES
        CXX_VISIBILITY_PRESET default
        VISIBILITY_INLINES_HIDDEN OFF
    )
endif()
# TODO: Remove src from public include directory

target_include_directories(epoch_folio
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src
    PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_compile_options(epoch_folio PRIVATE -Wall -Wextra -Werror)

# Add subdirectories before linking
add_subdirectory(src)

find_package(Protobuf REQUIRED)
target_link_libraries(epoch_folio PUBLIC epoch::flow)

if (BUILD_TEST)
  add_subdirectory(test)
endif()