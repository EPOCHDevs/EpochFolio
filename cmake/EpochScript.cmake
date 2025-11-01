# EpochCore.cmake
#
# This is a helper file to include EpochCore
include(FetchContent)

# Download CPM if not already included
if(NOT COMMAND CPMAddPackage)
    include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)
endif()

set(EPOCH_SCRIPT_REPOSITORY "https://github.com/EPOCHDevs/EpochScript.git" CACHE STRING "EpochScript repository URL")
set(EPOCH_SCRIPT_TAG "main" CACHE STRING "EpochScript Git tag to use")

CPMAddPackage(
    EpochScript
    GIT_REPOSITORY ${EPOCH_SCRIPT_REPOSITORY}
    GIT_TAG ${EPOCH_SCRIPT_TAG}
)

message(STATUS "EpochScript fetched and built from source")
