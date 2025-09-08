# Local EpochProtos installation
cmake_minimum_required(VERSION 3.20)

# Create imported target for the static library
add_library(epoch_protos_cpp STATIC IMPORTED)
set_target_properties(epoch_protos_cpp PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_LIST_DIR}/lib/libepoch_protos_cpp.a"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_LIST_DIR}/include"
)

# Add aliases for compatibility
add_library(EpochProtos::epoch_protos_cpp ALIAS epoch_protos_cpp)
add_library(epoch::proto ALIAS epoch_protos_cpp)

# Find protobuf dependency
find_package(Protobuf REQUIRED)
target_link_libraries(epoch_protos_cpp INTERFACE protobuf::libprotobuf)
