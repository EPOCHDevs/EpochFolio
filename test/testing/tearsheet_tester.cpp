#include "tearsheet_tester.hpp"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include "portfolio/model.h"
#include "reports/gap_report.h"
#include <sstream>
#include <filesystem>

namespace epoch_folio {
namespace test {

using namespace epoch_frame;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::index;

// TearSheetOutput implementation
bool TearSheetOutput::equals(const IOutputType& other) const {
    if (other.getType() != "tearsheet") {
        return false;
    }

    const auto& otherTearSheet = static_cast<const TearSheetOutput&>(other);

    // Compare basic structure counts
    if (data.cards().cards_size() != otherTearSheet.data.cards().cards_size() ||
        data.charts().charts_size() != otherTearSheet.data.charts().charts_size() ||
        data.tables().tables_size() != otherTearSheet.data.tables().tables_size()) {
        return false;
    }

    // For now, we'll do a basic comparison. In practice, you might want more sophisticated comparison
    // that allows for small numerical differences in calculations

    // Compare cards
    for (int i = 0; i < data.cards().cards_size(); ++i) {
        const auto& card1 = data.cards().cards(i);
        const auto& card2 = otherTearSheet.data.cards().cards(i);

        if (card1.data_size() != card2.data_size()) {
            return false;
        }

        for (int j = 0; j < card1.data_size(); ++j) {
            const auto& cardData1 = card1.data(j);
            const auto& cardData2 = card2.data(j);

            if (cardData1.title() != cardData2.title()) {
                return false;
            }

            // Compare values with tolerance for floating point
            if (cardData1.value().has_decimal_value() && cardData2.value().has_decimal_value()) {
                double diff = std::abs(cardData1.value().decimal_value() - cardData2.value().decimal_value());
                if (diff > 1e-6) {  // Small tolerance for floating point comparison
                    return false;
                }
            } else if (cardData1.value().has_integer_value() && cardData2.value().has_integer_value()) {
                if (cardData1.value().integer_value() != cardData2.value().integer_value()) {
                    return false;
                }
            } else if (cardData1.value().has_string_value() && cardData2.value().has_string_value()) {
                if (cardData1.value().string_value() != cardData2.value().string_value()) {
                    return false;
                }
            } else {
                return false; // Different value types
            }
        }
    }

    // Compare charts (basic count comparison for now)
    // A more sophisticated implementation would compare chart content

    // Compare tables (basic count and title comparison)
    for (int i = 0; i < data.tables().tables_size(); ++i) {
        const auto& table1 = data.tables().tables(i);
        const auto& table2 = otherTearSheet.data.tables().tables(i);

        if (table1.title() != table2.title()) {
            return false;
        }

        if (table1.columns_size() != table2.columns_size()) {
            return false;
        }
    }

    return true; // Equal if all components match
}

std::string TearSheetOutput::toString() const {
    std::stringstream ss;
    ss << "TearSheet {\n";
    ss << "  Cards: " << data.cards().cards_size() << "\n";
    ss << "  Charts: " << data.charts().charts_size() << "\n";
    ss << "  Tables: " << data.tables().tables_size() << "\n";

    // Print card details
    for (int i = 0; i < data.cards().cards_size(); ++i) {
        const auto& card = data.cards().cards(i);
        ss << "  Card " << i << ":\n";
        for (int j = 0; j < card.data_size(); ++j) {
            const auto& cardData = card.data(j);
            ss << "    " << cardData.title() << ": ";
            if (cardData.value().has_decimal_value()) {
                ss << cardData.value().decimal_value();
            } else if (cardData.value().has_integer_value()) {
                ss << cardData.value().integer_value();
            } else if (cardData.value().has_string_value()) {
                ss << cardData.value().string_value();
            }
            ss << "\n";
        }
    }

    // Print chart titles
    for (int i = 0; i < data.charts().charts_size(); ++i) {
        const auto& chart = data.charts().charts(i);
        ss << "  Chart " << i << ": ";
        if (chart.has_bar_def()) {
            ss << chart.bar_def().chart_def().title();
        } else if (chart.has_histogram_def()) {
            ss << chart.histogram_def().chart_def().title();
        } else if (chart.has_pie_def()) {
            ss << chart.pie_def().chart_def().title();
        } else if (chart.has_lines_def()) {
            ss << chart.lines_def().chart_def().title();
        }
        ss << "\n";
    }

    // Print table titles
    for (int i = 0; i < data.tables().tables_size(); ++i) {
        const auto& table = data.tables().tables(i);
        ss << "  Table " << i << ": " << table.title() << "\n";
    }

    ss << "}";
    return ss.str();
}

std::unique_ptr<IOutputType> TearSheetOutput::fromYAML(const YAML::Node& node) {
    auto output = std::make_unique<TearSheetOutput>();

    // Parse expected tearsheet structure from YAML
    if (node["cards"]) {
        const auto& cardsNode = node["cards"];
        auto* cardsCollection = output->data.mutable_cards();

        for (const auto& cardNode : cardsNode) {
            auto* card = cardsCollection->add_cards();

            if (cardNode["data"]) {
                for (const auto& dataNode : cardNode["data"]) {
                    auto* cardData = card->add_data();

                    if (dataNode["title"]) {
                        cardData->set_title(dataNode["title"].as<std::string>());
                    }

                    if (dataNode["value"]) {
                        const auto& valueNode = dataNode["value"];
                        auto* value = cardData->mutable_value();

                        if (valueNode.IsScalar()) {
                            // Try to determine the type
                            std::string valueStr = valueNode.as<std::string>();

                            // Try integer first
                            try {
                                int64_t intVal = std::stoll(valueStr);
                                value->set_integer_value(intVal);
                            } catch (...) {
                                // Try double
                                try {
                                    double doubleVal = std::stod(valueStr);
                                    value->set_decimal_value(doubleVal);
                                } catch (...) {
                                    // Default to string
                                    value->set_string_value(valueStr);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Parse charts if present
    if (node["charts"]) {
        const auto& chartsNode = node["charts"];
        auto* chartsCollection = output->data.mutable_charts();

        for (const auto& chartNode : chartsNode) {
            auto* chart = chartsCollection->add_charts();

            // Determine chart type and parse accordingly
            if (chartNode["type"]) {
                std::string chartType = chartNode["type"].as<std::string>();

                if (chartType == "bar") {
                    auto* barDef = chart->mutable_bar_def();
                    auto* chartDef = barDef->mutable_chart_def();
                    if (chartNode["title"]) {
                        chartDef->set_title(chartNode["title"].as<std::string>());
                    }
                } else if (chartType == "histogram") {
                    auto* histDef = chart->mutable_histogram_def();
                    auto* chartDef = histDef->mutable_chart_def();
                    if (chartNode["title"]) {
                        chartDef->set_title(chartNode["title"].as<std::string>());
                    }
                } else if (chartType == "pie") {
                    auto* pieDef = chart->mutable_pie_def();
                    auto* chartDef = pieDef->mutable_chart_def();
                    if (chartNode["title"]) {
                        chartDef->set_title(chartNode["title"].as<std::string>());
                    }
                } else if (chartType == "lines") {
                    auto* linesDef = chart->mutable_lines_def();
                    auto* chartDef = linesDef->mutable_chart_def();
                    if (chartNode["title"]) {
                        chartDef->set_title(chartNode["title"].as<std::string>());
                    }
                }
            }
        }
    }

    // Parse tables if present
    if (node["tables"]) {
        const auto& tablesNode = node["tables"];
        auto* tablesCollection = output->data.mutable_tables();

        for (const auto& tableNode : tablesNode) {
            auto* table = tablesCollection->add_tables();

            if (tableNode["title"]) {
                table->set_title(tableNode["title"].as<std::string>());
            }

            // Parse columns if present
            if (tableNode["columns"]) {
                for (const auto& colNode : tableNode["columns"]) {
                    auto* col = table->add_columns();
                    if (colNode["name"]) {
                        col->set_name(colNode["name"].as<std::string>());
                    }
                }
            }
        }
    }

    return std::move(output);
}

// Helper function to parse timestamps from various formats
int64_t parseTimestamp(const YAML::Node& node) {
    return epoch_frame::DateTime::from_str(node.as<std::string>(), "UTC", "%Y-%m-%dT%H:%M:%S").m_nanoseconds.count();
}

// Generic DataFrame creation from YAML using metadata
epoch_frame::DataFrame GetDataFrameFromYAML(const YAML::Node& node,
                                           const epoch_metadata::transforms::TransformsMetaData& metadata) {
    if (!node["type"]) {
        throw std::runtime_error("Input node must specify 'type'");
    }

    std::string inputType = node["type"].as<std::string>();
    // Note: inputType can be any type - we use metadata to determine inputs, not the type name

    // Parse timestamps
    std::vector<int64_t> timestamps;
    if (node["timestamps"]) {
        for (const auto& ts : node["timestamps"]) {
            timestamps.push_back(parseTimestamp(ts));
        }
    }

    if (timestamps.empty()) {
        // Return empty DataFrame
        auto empty_index = from_range(0);
        return MakeDataFrame({}, {});
    }

    auto index = make_datetime_index(timestamps);
    std::vector<Series> series_list;
    std::vector<std::string> column_names;

    // Create helper function for type-aware parsing
    auto createSeriesFromYAML = [&](const YAML::Node& data, const epoch_metadata::transforms::IOMetaData& inputSpec) -> Series {
        switch (inputSpec.type) {
            case epoch_core::IODataType::Boolean: {
                std::vector<epoch_frame::Scalar> values;
                for (const auto& val : data) {
                    if (val.IsNull()) {
                        values.push_back(epoch_frame::Scalar{arrow::MakeNullScalar(arrow::boolean())});
                    } else {
                        values.push_back(epoch_frame::Scalar{val.as<bool>()});
                    }
                }
                // Create the series using the factory that handles Scalar vectors
                return epoch_frame::Series(index, epoch_frame::factory::array::make_chunked_array(values, arrow::boolean()), inputSpec.id);
            }
            case epoch_core::IODataType::Decimal: {
                std::vector<epoch_frame::Scalar> values;
                for (const auto& val : data) {
                    if (val.IsNull()) {
                        values.push_back(epoch_frame::Scalar{arrow::MakeNullScalar(arrow::float64())});
                    } else {
                        values.push_back(epoch_frame::Scalar{val.as<double>()});
                    }
                }
                return epoch_frame::Series(index, epoch_frame::factory::array::make_chunked_array(values, arrow::float64()), inputSpec.id);
            }
            case epoch_core::IODataType::Integer: {
                std::vector<epoch_frame::Scalar> values;
                for (const auto& val : data) {
                    if (val.IsNull()) {
                        values.push_back(epoch_frame::Scalar{arrow::MakeNullScalar(arrow::int64())});
                    } else if (inputSpec.id.find("timestamp") != std::string::npos) {
                        // Special handling for timestamp columns
                        values.push_back(epoch_frame::Scalar{parseTimestamp(val)});
                    } else {
                        values.push_back(epoch_frame::Scalar{val.as<int64_t>()});
                    }
                }
                return epoch_frame::Series(index, epoch_frame::factory::array::make_chunked_array(values, arrow::int64()), inputSpec.id);
            }
            case epoch_core::IODataType::String: {
                std::vector<epoch_frame::Scalar> values;
                for (const auto& val : data) {
                    if (val.IsNull()) {
                        values.push_back(epoch_frame::Scalar{arrow::MakeNullScalar(arrow::utf8())});
                    } else {
                        values.push_back(epoch_frame::Scalar{val.as<std::string>()});
                    }
                }
                return epoch_frame::Series(index, epoch_frame::factory::array::make_chunked_array(values, arrow::utf8()), inputSpec.id);
            }
            default:
                throw std::runtime_error("Unsupported IODataType for input: " + inputSpec.id);
        }
    };

    // Process each input from metadata with validation
    std::vector<std::string> missingInputs;
    for (const auto& inputSpec : metadata.inputs) {
        // Check if this input exists in YAML
        if (node[inputSpec.id]) {
            try {
                series_list.push_back(createSeriesFromYAML(node[inputSpec.id], inputSpec));
                column_names.push_back(inputSpec.id);
            } catch (const std::exception& e) {
                throw std::runtime_error("Failed to parse input '" + inputSpec.id + "': " + e.what());
            }
        } else {
            // Track missing inputs for validation
            missingInputs.push_back(inputSpec.id);
        }
    }

    // Validate required data sources
    std::vector<std::string> missingDataSources;
    for (const auto& dataSource : metadata.requiredDataSources) {
        if (!node[dataSource]) {
            missingDataSources.push_back(dataSource);
        }
    }

    // Report validation errors
    if (!missingInputs.empty() || !missingDataSources.empty()) {
        std::string errorMsg = "Missing required data for report:";
        if (!missingInputs.empty()) {
            errorMsg += "\n  Missing inputs: ";
            for (size_t i = 0; i < missingInputs.size(); ++i) {
                if (i > 0) errorMsg += ", ";
                errorMsg += missingInputs[i];
            }
        }
        if (!missingDataSources.empty()) {
            errorMsg += "\n  Missing data sources: ";
            for (size_t i = 0; i < missingDataSources.size(); ++i) {
                if (i > 0) errorMsg += ", ";
                errorMsg += missingDataSources[i];
            }
        }
        throw std::runtime_error(errorMsg);
    }

    // Handle required data sources (like "c" for close)
    for (const auto& dataSource : metadata.requiredDataSources) {
        if (node[dataSource]) {
            std::vector<double> values;
            for (const auto& val : node[dataSource]) {
                values.push_back(val.as<double>());
            }
            series_list.push_back(make_series(index, values, dataSource));
            column_names.push_back(dataSource);
        }
    }

    return MakeDataFrame(series_list, column_names);
}

// Load gap test cases from YAML file
std::vector<ReportTestCase> loadReportTestsFromYAML(const std::string& filePath) {
    std::vector<ReportTestCase> testCases;

    try {
        YAML::Node rootNode = YAML::LoadFile(filePath);

        if (!rootNode["tests"]) {
            throw std::runtime_error("YAML file must contain 'tests' array");
        }

        for (const auto& testNode : rootNode["tests"]) {
            ReportTestCase testCase;

            // Parse title
            if (testNode["title"]) {
                testCase.title = testNode["title"].as<std::string>();
            }

            // Parse options first to get report type for metadata
            if (testNode["options"]) {
                const auto& optionsNode = testNode["options"];
                for (auto const& opt : optionsNode) {
                    std::string key = opt.first.as<std::string>();
                    const auto& value = opt.second;

                    testCase.options[key] = MetaDataOptionDefinition{YAML::Dump(value)};
                }
            }

            // Get report metadata for input parsing
            std::string reportType = "gap_report"; // Default fallback
            auto reportTypeIt = testCase.options.find("report_type");
            if (reportTypeIt != testCase.options.end()) {
                reportType = reportTypeIt->second.GetSelectOption();
            }

            auto metadata = epoch_metadata::transforms::ITransformRegistry::GetInstance().GetMetaData(reportType);
            if (!metadata) {
                throw std::runtime_error("No metadata found for report type: " + reportType);
            }

            // Parse input with metadata
            if (testNode["input"]) {
                testCase.input = GetDataFrameFromYAML(testNode["input"], metadata->get());
            }

            // Parse expected output
            if (testNode["expect"]) {
                const auto& expectNode = testNode["expect"];
                if (expectNode["type"]) {
                    std::string type = expectNode["type"].as<std::string>();
                    testCase.expect = OutputTypeRegistry::instance().create(type, expectNode);
                }
            }

            testCases.push_back(std::move(testCase));
        }
    } catch (const YAML::Exception& e) {
        throw std::runtime_error("YAML parsing error: " + std::string(e.what()));
    }

    return testCases;
}

// Generic report runner using TransformRegistry - same pattern as transforms
epoch_proto::TearSheet YamlTearSheetTester::runReportWithConfig(const epoch_frame::DataFrame& input,
                                                               const MetaDataArgDefinitionMapping& options) {
    // Get the report type from options
    auto reportTypeIt = options.find("report_type");
    if (reportTypeIt == options.end()) {
        throw std::runtime_error("report_type not specified in options");
    }

    std::string reportType = reportTypeIt->second.GetSelectOption();

    // Get instance ID from options
    auto instanceIdIt = options.find("instance_id");
    std::string instanceId = (instanceIdIt != options.end())
        ? instanceIdIt->second.GetSelectOption()
        : "report_test";

    // Build configuration using registry approach (like transforms)
    YAML::Node config;
    config["id"] = instanceId;
    config["type"] = reportType;
    config["timeframe"] = "1D";
    config["sessionRange"] = YAML::Node();

    // Add options from test case, applying defaults for missing options
    YAML::Node optionsNode;

    // First, get the metadata to know what options are available and their defaults
    auto metadata = epoch_metadata::transforms::ITransformRegistry::GetInstance().GetMetaData(reportType);
    if (!metadata) {
        throw std::runtime_error("No metadata found for report type: " + reportType);
    }

    // Apply defaults for all defined options
    for (const auto& optionDef : metadata->get().options) {
        if (optionDef.defaultValue.has_value()) {
            const auto& defaultVal = optionDef.defaultValue.value();
            if (defaultVal.IsType<bool>()) {
                optionsNode[optionDef.id] = defaultVal.GetBoolean();
            } else if (defaultVal.IsType<double>()) {
                optionsNode[optionDef.id] = defaultVal.GetDecimal();
            } else if (defaultVal.IsType<std::string>()) {
                optionsNode[optionDef.id] = defaultVal.GetSelectOption();
            }
        }
    }

    // Override with test case specific options
    for (const auto& option : options) {
        if (option.first == "report_type" || option.first == "instance_id") {
            continue; // Skip meta options
        }

        const auto& optionDef = option.second;
        if (optionDef.IsType<bool>()) {
            optionsNode[option.first] = optionDef.GetBoolean();
        } else if (optionDef.IsType<double>()) {
            optionsNode[option.first] = optionDef.GetDecimal();
        } else if (optionDef.IsType<std::string>()) {
            optionsNode[option.first] = optionDef.GetSelectOption();
        }
    }
    config["options"] = optionsNode;

    // Use the metadata we already fetched to determine required inputs

    // Create generic 1:1 input mapping for any report type
    YAML::Node inputs;
    for (const auto& inputSpec : metadata->get().inputs) {
        // Use the id (field name) for mapping, not the display name
        // This creates a 1:1 mapping: id -> "test#id"
        inputs[inputSpec.id] = "test#" + inputSpec.id;
    }
    config["inputs"] = inputs;

    // Create TransformConfiguration (reports are transforms)
    epoch_metadata::TransformDefinition definition{config};
    epoch_metadata::transform::TransformConfiguration transformConfig(std::move(definition));

    // Create report using registry (same as transforms)
    auto transformPtr = epoch_metadata::transform::TransformRegistry::GetInstance().Get(transformConfig);
    if (!transformPtr) {
        throw std::runtime_error("Failed to create report: " + reportType);
    }

    // Cast to IReporter to call tearsheet methods
    auto reporter = dynamic_cast<IReporter*>(transformPtr.get());
    if (!reporter) {
        throw std::runtime_error("Transform is not a reporter: " + reportType);
    }

    // Generate tearsheet using the testing method (now available on base IReporter)
    reporter->generateTearsheetForTesting(input);
    return reporter->GetTearSheet();
}

} // namespace test
} // namespace epoch_folio