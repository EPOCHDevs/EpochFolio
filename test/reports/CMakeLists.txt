target_sources(epoch_folio_test PRIVATE report_test_all.cpp)

# Copy YAML test cases to build directory for report tests
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/report_test_cases
     DESTINATION ${CMAKE_BINARY_DIR}/bin/)
