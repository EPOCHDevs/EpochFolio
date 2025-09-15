//
// Generic report tester that loads all YAML test cases for reports
// Uses the tearsheet testing framework for report-specific testing
//
#include "../testing/tearsheet_tester.hpp"
#include <epoch_core/catch_defs.h>

using namespace epoch_folio::test;

TEST_CASE("All Report Tests - YAML Based", "[Report][YAML]") {
    // Test only the predefined report_test_cases directory for this library
    YamlTearSheetTester::Config config("report_test_cases");
    config.recursive = true;
    config.requireTestCasesDir = false;

    // Run all tests using the report registry approach
    YamlTearSheetTester::runReportRegistryTests(config);
}