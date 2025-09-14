add_executable(epoch_folio_test catch_main.cpp tearsheet_test.cpp)

target_link_libraries(epoch_folio_test PRIVATE epoch_folio Catch2::Catch2 Catch2::Catch2WithMain )
target_include_directories(epoch_folio_test PRIVATE ${PROJECT_SOURCE_DIR}/src )
target_compile_definitions(epoch_folio_test PRIVATE -DEPOCH_FOLIO_S3_TEST_BUCKET="epoch-test-files/epoch_folio")

add_subdirectory(empyrical)
add_subdirectory(portfolio)
add_subdirectory(reports)  # Re-enabled with updated tests
