# EpochCore.cmake
#
# This is a helper file to include EpochCore
include(FetchContent)

# Download CPM if not already included
if(NOT COMMAND CPMAddPackage)
    include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)
endif()

set(EPOCH_FLOW_REPOSITORY "https://github.com/EPOCHDevs/EpochFlow.git" CACHE STRING "EpochFlow repository URL")
set(EPOCH_FLOW_TAG "main" CACHE STRING "EpochFlow Git tag to use")

CPMAddPackage(
    EpochFlow
    GIT_REPOSITORY ${EPOCH_FLOW_REPOSITORY}
    GIT_TAG ${EPOCH_FLOW_TAG}
)

message(STATUS "EpochFlow fetched and built from source")
