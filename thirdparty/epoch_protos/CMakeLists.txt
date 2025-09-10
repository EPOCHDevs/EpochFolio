# EpochProtos CMake integration for EpochFolio
cmake_minimum_required(VERSION 3.20)

# Add include directory
target_include_directories(${EPOCH_FOLIO_LIBRARY_NAME} PRIVATE 
    ${CMAKE_CURRENT_LIST_DIR}/include/epoch_protos
)

# Link the static library
target_link_libraries(${EPOCH_FOLIO_LIBRARY_NAME} PRIVATE 
    ${CMAKE_CURRENT_LIST_DIR}/lib/libepoch_protos_cpp.a
)

# Ensure protobuf is available
find_package(Protobuf REQUIRED)
target_link_libraries(${EPOCH_FOLIO_LIBRARY_NAME} PRIVATE protobuf::libprotobuf)
