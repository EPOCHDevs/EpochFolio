add_executable(epoch_folio_test catch_main.cpp tearsheet_test.cpp)

target_link_libraries(epoch_folio_test PRIVATE epoch_folio Catch2::Catch2 epoch_testing_framework Catch2::Catch2)
target_include_directories(epoch_folio_test PRIVATE ${PROJECT_SOURCE_DIR}/src )
target_compile_definitions(epoch_folio_test PRIVATE -DEPOCH_FOLIO_S3_TEST_BUCKET="epoch-test-files/epoch_folio")

# Copy report test case YAML files to runtime output directory
file(GLOB_RECURSE REPORT_TEST_CASE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/reports/report_test_cases/*.yaml)
foreach(TEST_FILE ${REPORT_TEST_CASE_FILES})
    file(RELATIVE_PATH REL_PATH ${CMAKE_CURRENT_SOURCE_DIR}/reports ${TEST_FILE})
    configure_file(${TEST_FILE} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${REL_PATH} COPYONLY)
endforeach()

add_subdirectory(empyrical)
add_subdirectory(portfolio)
add_subdirectory(testing)
add_subdirectory(reports)  # Re-enabled with updated tests
