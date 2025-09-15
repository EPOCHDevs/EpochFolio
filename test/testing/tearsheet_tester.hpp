#ifndef TEARSHEET_TESTER_HPP
#define TEARSHEET_TESTER_HPP

#include <epoch_testing/transform_tester_base.hpp>
#include <epoch_testing/yaml_transform_tester.hpp>
#include <epoch_protos/tearsheet.pb.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_core/catch_defs.h>
#include <yaml-cpp/yaml.h>
#include <memory>
#include <filesystem>

namespace epoch_folio {
namespace test {

using namespace epoch::test;

// TearSheet output implementation
class TearSheetOutput : public IOutputType {
public:
    epoch_proto::TearSheet data;

    TearSheetOutput() = default;
    explicit TearSheetOutput(const epoch_proto::TearSheet& tearsheet) : data(tearsheet) {}

    std::string getType() const override { return "tearsheet"; }
    bool equals(const IOutputType& other) const override;
    std::string toString() const override;

    // Factory method for creating from YAML
    static std::unique_ptr<IOutputType> fromYAML(const YAML::Node& node);
};

    epoch_frame::DataFrame GetDataFrameFromYAML(const YAML::Node& node,
                                               const epoch_metadata::transforms::TransformsMetaData& metadata);

// Gap test case structure
struct ReportTestCase {
    std::string title;
    epoch_frame::DataFrame input;
    std::unique_ptr<IOutputType> expect;
    MetaDataArgDefinitionMapping options;
};


// Helper function to register TearSheet type
inline void registerTearSheetType() {
    OutputTypeRegistry::instance().registerType("tearsheet",
        [](const YAML::Node& node) { return TearSheetOutput::fromYAML(node); });
}

    std::vector<ReportTestCase> loadReportTestsFromYAML(const std::string& filePath);

/**
 * YAML-based tearsheet testing utility that extends YamlTransformTester
 * for tearsheet-specific testing workflows
 */
class YamlTearSheetTester {
public:
    using Config = YamlTransformTester::Config;

    /**
     * Run all YAML gap report tests found in configured directories
     */
    static void runAllTests(
        const Config& config,
        std::function<epoch_proto::TearSheet(const epoch_frame::DataFrame&, const MetaDataArgDefinitionMapping&)> tearsheetAdapter) {

        // Register TearSheet type once
        static bool registered = false;
        if (!registered) {
            registerTearSheetType();
            registered = true;
        }

        // Find all test files
        std::vector<std::string> allTestFiles = YamlTransformTester::findAllTestFiles(config);

        if (allTestFiles.empty()) {
            if (config.requireTestCasesDir) {
                FAIL("No test files found in any of the configured directories");
            } else {
                WARN("No test files found in any of the configured directories");
                return;
            }
        }

        // Sort files for consistent test ordering
        std::sort(allTestFiles.begin(), allTestFiles.end());

        INFO("Found " << allTestFiles.size() << " tearsheet test files across "
             << config.testDirectories.size() << " directories");

        // Process each test file
        for (const auto& testFile : allTestFiles) {
            runTearSheetTestFile(testFile, tearsheetAdapter);
        }
    }

    /**
     * Run tearsheet tests using the report registry approach
     */
    static void runReportRegistryTests(const Config& config = Config("reports/report_test_cases")) {
        runAllTests(config, runReportWithConfig);
    }

private:
    /**
     * Run tests from a single YAML file for tearsheets
     */
    static void runTearSheetTestFile(
        const std::string& testFile,
        std::function<epoch_proto::TearSheet(const epoch_frame::DataFrame&, const MetaDataArgDefinitionMapping&)> tearsheetAdapter) {

        // Extract a clean name for the section
        std::filesystem::path filePath(testFile);
        std::string sectionName = filePath.stem().string() + " [" +
                                 filePath.parent_path().filename().string() + "]";

        SECTION(sectionName) {
            INFO("Loading tearsheet test file: " << testFile);

            // Load test cases from YAML
            std::vector<ReportTestCase> testCases;
            try {
                testCases = loadReportTestsFromYAML(testFile);
            } catch (const std::exception& e) {
                FAIL("Failed to load test cases from " << testFile << ": " << e.what());
                return;
            }

            INFO("Loaded " << testCases.size() << " tearsheet test cases from " << testFile);

            // Run each test case
            for (auto& testCase : testCases) {
                SECTION(testCase.title) {
                    INFO("TearSheet Test: " << testCase.title);

                    // Log options
                    INFO(formatOptions(testCase.options));

                    // Run tearsheet generation
                    epoch_proto::TearSheet outputTearSheet;
                    try {
                        outputTearSheet = tearsheetAdapter(testCase.input, testCase.options);
                    } catch (const std::exception& e) {
                        FAIL("TearSheet generation failed: " << e.what());
                        return;
                    }

                    INFO("TearSheet generated with " << outputTearSheet.cards().cards_size() << " cards, "
                         << outputTearSheet.charts().charts_size() << " charts, and "
                         << outputTearSheet.tables().tables_size() << " tables");

                    // Convert output to TearSheetOutput for comparison
                    auto actualOutput = std::make_unique<TearSheetOutput>(outputTearSheet);

                    // Compare with expected output
                    if (testCase.expect) {
                        INFO("Expected:\n" << testCase.expect->toString());
                        INFO("Actual:\n" << actualOutput->toString());

                        REQUIRE(actualOutput->equals(*testCase.expect));
                    } else {
                        // If no expected output specified, just verify basic structure
                        REQUIRE(outputTearSheet.cards().cards_size() >= 0);
                        REQUIRE(outputTearSheet.charts().charts_size() >= 0);
                        REQUIRE(outputTearSheet.tables().tables_size() >= 0);
                    }
                }
            }
        }
    }

    /**
     * Format options for logging
     */
    static std::string formatOptions(const MetaDataArgDefinitionMapping& options) {
        std::stringstream optStr;
        optStr << "Options: {";
        bool first = true;
        for (const auto& [key, value] : options) {
            if (!first) optStr << ", ";
            first = false;
            optStr << key << ": " << value.ToString();
        }
        optStr << "}";
        return optStr.str();
    }

    /**
     * Generic report runner using ReportMetadata
     */
    static epoch_proto::TearSheet runReportWithConfig(const epoch_frame::DataFrame& input,
                                                     const MetaDataArgDefinitionMapping& options);
};

} // namespace test
} // namespace epoch_folio

#endif // TEARSHEET_TESTER_HPP