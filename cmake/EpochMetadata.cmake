# EpochCore.cmake
#
# This is a helper file to include EpochCore
include(FetchContent)
    
set(EPOCH_METADATA_REPOSITORY "https://github.com/EPOCHDevs/EpochMetadata.git" CACHE STRING "EpochFrame repository URL")
set(EPOCH_METADATA_TAG "master" CACHE STRING "EpochFrame Git tag to use")

FetchContent_Declare(
    EpochMetadata
    GIT_REPOSITORY ${EPOCH_METADATA_REPOSITORY}
    GIT_TAG ${EPOCH_METADATA_TAG}
)

FetchContent_MakeAvailable(EpochMetadata)

message(STATUS "EpochMetadata fetched and built from source")