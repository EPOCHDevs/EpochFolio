target_sources(epoch_folio PRIVATE metadata.cpp tearsheet.cpp)

add_subdirectory(empyrical)
add_subdirectory(models)
add_subdirectory(portfolio)
add_subdirectory(tear_sheets)
add_subdirectory(reports)

