//
// Created by adesola on 1/13/25.
//
#include <epoch_core/catch_defs.h>
#include "portfolio/pos.h"
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/scalar_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>
#include <unordered_map>
#include <limits>

using namespace epoch_folio;
using namespace epoch_frame;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::scalar;

TEST_CASE("Position Test") {
    auto dates = date_range({.start="2015-01-01"_date, .periods=20, .offset=offset::days(1)});

    SECTION("Test Get Percent Alloc") {
        // Create test data similar to Python test - 5 days with 3 assets
        auto index = date_range({.start="2015-01-01"_date, .periods=5, .offset=offset::days(1)});
        std::vector raw_data{
            std::vector<double>{-0.0, -3.0, -6.0, -9.0, -12.0},
            std::vector<double>{1.0, 4.0, 7.0, 10.0, 13.0},
            std::vector<double>{2.0, 5.0, 8.0, 11.0, 14.0}
        };
        auto frame = make_dataframe(index,
            raw_data,
            std::vector<std::string>{"A", "B", "C"});
        
        auto result = GetPercentAlloc(frame);

        std::vector<double> sums{
            raw_data[0][0] + raw_data[1][0] + raw_data[2][0],
            raw_data[0][1] + raw_data[1][1] + raw_data[2][1],
            raw_data[0][2] + raw_data[1][2] + raw_data[2][2],
            raw_data[0][3] + raw_data[1][3] + raw_data[2][3],
            raw_data[0][4] + raw_data[1][4] + raw_data[2][4]
        };

        for (size_t i = 0; i < raw_data.size(); i++) {
            for (size_t j = 0; j < raw_data[i].size(); j++) {
                raw_data[i][j] /= sums[j];
            }
        }
        
        // Manually calculate expected values
        auto expected = make_dataframe(index,
            raw_data,
            std::vector<std::string>{"A", "B", "C"});

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals(expected));
    }
    
    SECTION("Test Get Max Median Position Concentration") {
        auto dates = date_range({.start="2015-01-01"_date, .periods=20, .offset=offset::days(1)});
        
        // Define a struct to hold test parameters
        struct TestCase {
            std::string name;
            std::vector<double> positions_data;
            std::vector<double> expected_data;
        };
        
        // Vector of test cases
        std::vector<TestCase> test_cases = {
            // All positive positions
            {
                "All positive positions",
                {
                    1.0, 2.0, 3.0, 14.0
                },
                {
                    0.15, 0.1, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()
                }
            },
            // Mixed positions (long and short)
            {
                "Mixed positions (long and short)",
                {
                    1.0, -2.0, -13.0, 15.0
                },
                {
                    1.0, 1.0, -7.5, -13.0
                }
            },
            // With NaN values
            {
                "With NaN values",
                {
                    std::numeric_limits<double>::quiet_NaN(), 2.0, std::numeric_limits<double>::quiet_NaN(), 8.0
                },
                {
                    0.2, 0.2, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()
                }
            }
        };

        // Loop through test cases
        for (const auto& [i, test_case] : std::views::enumerate(test_cases)) {
            DYNAMIC_SECTION(test_case.name) {

                std::vector<std::vector<double>> raw_data, expected_data;
                for (const auto& row : test_case.positions_data) {
                    raw_data.push_back(std::vector<double>(dates->size(), row));
                }
                for (const auto& row : test_case.expected_data) {
                    expected_data.push_back(std::vector<double>( dates->size(), row));
                }

                auto positions = make_dataframe(dates, raw_data,{"0", "1", "2", "cash"});
                
                auto result = GetMaxMedianPositionConcentration(positions);
                
                auto expected = make_dataframe(dates,
                    expected_data,
                    {"max_long", "median_long", "median_short", "max_short"});
                
                INFO(result << "\n!=\n" << expected);
                REQUIRE(result.equals(expected));
            }
        }
    }
    
    SECTION("Test Get Sector Exposures") {

        struct Param {
            std::string name;
            std::vector<double> positions;
            std::unordered_map<std::string, std::string> mapping;
             std::vector<double> expected;
        };

        std::vector<Param> params = {
            Param{
                "Complete mapping",
                {1.0, 2.0, 3.0},
                {{"0", "A"}, {"1", "B"}, {"2", "A"}},
                {4.0, 2.0, 10.0},
            },
            Param{
                "Partial mapping",
                {1.0, 2.0, 3.0},
                {{"0", "A"}, {"1", "B"}},
                {1.0, 2.0, 10.0},
            },
        };

        for (const auto&  [name, positions, mapping, expected] : params) {
            DYNAMIC_SECTION(name) {
                std::vector<std::vector<double>> raw_data;
                for (const auto& row : positions) {
                    raw_data.push_back(std::vector<double>(dates->size(), row));
                }
                auto positions_df = make_dataframe(dates, raw_data, {"0", "1", "2"});
                auto result_sector_exposure = GetSectorExposure(positions_df, mapping);

                raw_data.clear();
                for (const auto& row : expected) {
                    raw_data.push_back(std::vector<double>(dates->size(), row));
                }
                auto expected_df = make_dataframe(dates, raw_data, {"A", "B"});

                INFO(result_sector_exposure << "\n!=\n" << expected_df);
                REQUIRE(expected_df.equals(result_sector_exposure.sort_columns()));
            }
        }
    }
    
    SECTION("Test Get Top Long Short Abs") {
        // Create test data with mix of long and short positions
        auto index = date_range({.start="2015-01-01"_date, .periods=1, .offset=offset::days(1)});
        
        auto positions = make_dataframe(index,
            std::vector{
                std::vector<double>{10.0},
                std::vector<double>{5.0},
                std::vector<double>{-7.0},
                std::vector<double>{-3.0},
                std::vector<double>{8.0},
                std::vector<double>{-15.0},
                std::vector<double>{20.0},
                std::vector<double>{1.0},
                std::vector<double>{-2.0},
                std::vector<double>{-9.0},
                std::vector<double>{3.0},
                std::vector<double>{6.0},
                std::vector<double>{100.0}  // cash
            },
            std::vector<std::string>{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "cash"});
        
        // Get top 5 positions
        auto [top_long, top_short, top_abs] = GetTopLongShortAbs(positions, 5);

        INFO(top_long << "\n" << top_short << "\n" << top_abs);
        
        // Expected top long: 20.0, 10.0, 8.0, 6.0, 5.0
        REQUIRE(top_long.size() == 5);
        REQUIRE(top_long.iloc(0).value<double>() == 20.0);
        REQUIRE(top_long.iloc(1).value<double>() == 10.0);
        REQUIRE(top_long.iloc(2).value<double>() == 8.0);
        REQUIRE(top_long.iloc(3).value<double>() == 6.0);
        REQUIRE(top_long.iloc(4).value<double>() == 5.0);
        
        // Expected top short: -15.0, -9.0, -7.0, -3.0, -2.0
        REQUIRE(top_short.size() == 5);
        REQUIRE(top_short.iloc(0).value<double>() == -15.0);
        REQUIRE(top_short.iloc(1).value<double>() == -9.0);
        REQUIRE(top_short.iloc(2).value<double>() == -7.0);
        REQUIRE(top_short.iloc(3).value<double>() == -3.0);
        REQUIRE(top_short.iloc(4).value<double>() == -2.0);
        
        // Expected top absolute: 20.0, 15.0, 10.0, 9.0, 8.0
        REQUIRE(top_abs.size() == 5);
        REQUIRE(top_abs.iloc(0).value<double>() == 20.0);
        REQUIRE(top_abs.iloc(1).value<double>() == 15.0);
        REQUIRE(top_abs.iloc(2).value<double>() == 10.0);
        REQUIRE(top_abs.iloc(3).value<double>() == 9.0);
        REQUIRE(top_abs.iloc(4).value<double>() == 8.0);
    }
}