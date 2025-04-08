//
// Created by adesola on 1/13/25.
//
#include <epoch_core/catch_defs.h>
#include "portfolio/timeseries.h"
#include "../common_utils.h"
#include <valarray>
#include <epoch_frame/serialization.h>
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/scalar_factory.h>
#include <epoch_frame/scalar.h>
#include <epoch_frame/common.h>
#include <random>

using namespace epoch_folio;
using namespace epoch_frame;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;


TEST_CASE("Drawdown Test") {
    auto make_index = [](int64_t size) {
        return date_range({.start="2000-01-03"_date, .periods=size, .offset=offset::days(1)});
    };

    SECTION("Test Max Drawdown Begins First Day") {
        std::vector<double> drawdownList = {10.0, 9.0, 7.5};
        auto drawdownSeries = make_series(make_index(3), drawdownList, "");

        auto rets = drawdownSeries.pct_change();
        auto drawdowns = GenerateDrawDownTable(rets, 1);
        REQUIRE(drawdowns.size() == 1);
        REQUIRE(drawdowns[0].netDrawdown == 25.0_scalar);
    }

    SECTION("Test Max Drawdown Relative") {
        std::vector<double> drawdownList = {
                100, 110, 120, 150, 180, 200, 100, 120, 160, 180, 200,
                300, 400, 500, 600, 800, 900, 1000, 650, 600
        };
        auto drawdownSeries = make_series(make_index(20), drawdownList, "") / 10.0_scalar;

        auto rets = drawdownSeries.pct_change();
        auto drawdowns = GenerateDrawDownTable(rets, 2);
        REQUIRE(drawdowns.size() == 2);
        REQUIRE(drawdowns[0].netDrawdown == 50.0_scalar);
        REQUIRE(drawdowns[0].peakDate == "2000-01-08"__date.date);
        REQUIRE(drawdowns[0].valleyDate == "2000-01-09"__date.date);
        REQUIRE(drawdowns[0].recoveryDate == "2000-01-13"__date.date);

        REQUIRE(drawdowns[1].netDrawdown == 40.0_scalar);
        REQUIRE(drawdowns[1].peakDate == "2000-01-20"__date.date);
        REQUIRE(drawdowns[1].valleyDate == "2000-01-22"__date.date);
        REQUIRE_FALSE(drawdowns[1].recoveryDate.has_value());
    }

    SECTION("Test Get Max Drawdown") {
        struct TestCase {
            std::vector<double> prices;
            Date peakDate;
            Date valleyDate;
            std::optional<Date> recoveryDate;
        };

        std::vector<TestCase> cases = {
                {{100, 120, 100, 80, 70, 110, 180, 150}, Date(2000y, January, 4d), Date(2000y, January, 7d), Date(2000y, January, 9d)},
                {{100, 120, 100, 80, 70, 80,  90,  90},  Date(2000y, January, 4d), Date(2000y, January, 7d), std::nullopt}
        };

        for (const auto &[i, testCase]: std::views::enumerate(cases)) {
            DYNAMIC_SECTION("Drawdown empyrical " << i)
            {
                auto dt = date_range({.start="2000-01-03"_date, .periods=testCase.prices.size(), .offset = offset::days(1)});
                auto series = make_series(dt, testCase.prices) / 100.0_scalar;
                auto rets = series.pct_change();

                auto drawdowns = GenerateDrawDownTable(rets, 1);
                REQUIRE(drawdowns.size() > 0);
                REQUIRE(drawdowns[0].peakDate == testCase.peakDate);
                REQUIRE(drawdowns[0].valleyDate == testCase.valleyDate);
                if (testCase.recoveryDate.has_value()) {
                    REQUIRE(drawdowns[0].recoveryDate == testCase.recoveryDate);
                } else {
                    REQUIRE_FALSE(drawdowns[0].recoveryDate.has_value());
                }
            }
        }
    }

    SECTION("Test Top Drawdowns") {
        std::vector<double> px_list_1 = {100, 120, 100, 80, 70, 110, 180, 150};
        auto dt = make_index(8);
        auto rets = make_series(dt, px_list_1) / 100.0_scalar;

        auto top_drawdowns = GetTopDrawDownsFromReturns(rets, 1);
        REQUIRE(top_drawdowns.size() == 1);
        REQUIRE(top_drawdowns[0].peak == Scalar{Date(2000y, January, 3d)});
        REQUIRE(top_drawdowns[0].valley == Scalar{Date(2000y, January, 3d)});
        REQUIRE(top_drawdowns[0].recovery == Scalar{Date(2000y, January, 3d)});
    }

    SECTION("Test Gen Drawdown table") {
        struct TestCase {
            std::vector<double> prices;
            Date expectsPeakDate;
            Date expectsValleyDate;
            std::optional<Date> expectsRecoveryDate;
            std::optional<int64_t> duration;
        };

        std::vector<double> px_list_1{100.0, 120, 100, 80, 70, 110, 180, 150};
        std::vector<double> px_list_2{100, 120, 100, 80, 70, 80,   90,   90};
        std::vector<TestCase> cases = {
                {px_list_2,  Date(2000y, January, 4d), Date(2000y, January, 7d), std::nullopt, std::nullopt},
                {px_list_1, Date(2000y, January, 4d), Date(2000y, January, 7d), Date(2000y, January, 9d), 4}
        };

        for (const auto &[i, testCase]: std::views::enumerate(cases)) {
            DYNAMIC_SECTION("Drawdown empyrical " << i)
            {
                auto dt = make_index(testCase.prices.size());
                auto series = make_series(dt, testCase.prices) / 100.0_scalar;
                auto rets = series.pct_change().iloc({.start=1});

                auto drawdowns = GenerateDrawDownTable(rets, 1);
                REQUIRE(drawdowns.size() == 1);
                REQUIRE(drawdowns[0].peakDate == testCase.expectsPeakDate);
                REQUIRE(drawdowns[0].valleyDate == testCase.expectsValleyDate);
                if (testCase.expectsRecoveryDate.has_value()) {
                    REQUIRE(drawdowns[0].recoveryDate == testCase.expectsRecoveryDate);
                    REQUIRE(drawdowns[0].duration.value<size_t>() == *testCase.duration);
                } else {
                    REQUIRE_FALSE(drawdowns[0].recoveryDate.has_value());
                    REQUIRE(drawdowns[0].duration.is_null());
                }
            }
        }
    }

    SECTION("Test Overlap") {
        // Use a fixed seed for reproducibility
        int64_t n_samples = 252 * 5;
        std::mt19937 gen(1337);
        std::student_t_distribution<double> dist(3.1);
        
        std::vector<double> returns;
        returns.reserve(n_samples);
        for (int i = 0; i < 252 * 5; ++i) {
            returns.push_back(dist(gen));
        }
        
        auto spy_returns = make_series(
            date_range({
                .start="2005-01-02"_date,
                .periods=n_samples,
                .offset=offset::days(1)}), returns);
        
        // Get top 20 drawdowns and sort by peak date
        auto drawdowns = GenerateDrawDownTable(spy_returns, 20);
        std::ranges::sort(drawdowns, [](const auto& a, const auto& b) {
            return a.peakDate < b.peakDate;
        });
        
        // Compare recovery date of each drawdown with peak of the next
        // Skip the last pair if the last drawdown hasn't recovered
        REQUIRE(drawdowns.size() > 1); // Ensure we have at least 2 drawdowns to compare

        for (size_t i = 0; i < drawdowns.size() - 1; ++i) {
            const auto& current = drawdowns[i];
            const auto& next = drawdowns[i + 1];
            
            if (current.recoveryDate.has_value()) {
                REQUIRE(current.recoveryDate.value() <= next.peakDate);
            }
        }
    }
}

TEST_CASE("Gross Leverage Test") {
    if constexpr (!s3_testing_available()) {
        SKIP("S3 test bucket not configured");
    }

    auto test_pos = epoch_frame::read_csv_file(get_s3_test_path("test_pos.csv"), {
        .index_column = "index"
    });

    SliceType slice{Scalar{"2004-02-01"__date}.cast(arrow::timestamp(arrow::TimeUnit::NANO, "UTC")), Scalar{}};

    auto test_gross_lev = epoch_frame::read_csv_file(get_s3_test_path("test_gross_lev.csv"), {
        .has_header = false,
    }).set_index("f0").to_series().loc(slice);

    auto result = GrossLeverage(test_pos).loc(slice);

    INFO(result << "\n!=\n" << test_gross_lev);
    REQUIRE(result.is_approx_equal(test_gross_lev, arrow::EqualOptions{}.nans_equal(true)));
}


TEST_CASE("Stats") {
    // Simple returns: [0.1, 0.1, 0.1, 0, 0, ... (497 zeros)]
    std::vector<double> simple_rets_data(500, 0.0);
    std::fill_n(simple_rets_data.begin(), 3, 0.1);
    auto simple_rets = make_series(
        date_range({.start="2000-01-03"_date, .periods=500, .offset=offset::days(1)}),
        simple_rets_data
    );

    // Simple benchmark returns: [0.03, 0.03, 0.03, 0.03, 0, 0, ... (496 zeros)]
    std::vector<double> simple_benchmark_data(500, 0.0);
    std::fill_n(simple_benchmark_data.begin(), 4, 0.03);
    auto simple_benchmark = make_series(
        date_range({.start="2000-01-01"_date, .periods=500, .offset=offset::days(1)}),
        simple_benchmark_data
    );

    // Price lists
    std::vector<double> px_list = {0.10, -0.10, 0.10}; // Ends in drawdown
    auto dt = date_range({.start="2000-01-03"_date, .periods=3, .offset=offset::days(1)});

    std::vector<double> px_list_2 = {1.0, 1.2, 1.0, 0.8, 0.7, 0.8, 0.8, 0.8};
    auto dt_2 = date_range({.start="2000-01-03"_date, .periods=8, .offset=offset::days(1)});

    SECTION("Test Rolling Sharpe") {
        // Test data: first 5 elements of simple_rets
        auto returns = simple_rets.iloc({.stop=5});
        constexpr int rolling_sharpe_window = 2;
        
        // Expected values from Python: [np.nan, np.inf, np.inf, 11.224972160321, np.nan]
        auto rolling_sharpe_result = RollingSharpe(returns, rolling_sharpe_window);
        rolling_sharpe_result = rolling_sharpe_result.where(!Array{arrow::compute::IsNan(rolling_sharpe_result.array())->chunked_array()->chunk(0)}, Scalar{});
        auto expected_result = factory::array::make_array(
            std::vector<double>{std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(), 11.224972160321, std::numeric_limits<double>::quiet_NaN()}
        );
        
        INFO(rolling_sharpe_result << "\n!=\n" << expected_result->ToString());
        REQUIRE(rolling_sharpe_result.array()->ApproxEquals(*expected_result, arrow::EqualOptions{}.nans_equal(true)));
    }

    // SECTION("Test Rolling Beta") {
    //     // Test data: first 5 elements of simple_rets and simple_benchmark
    //     auto returns = simple_rets.iloc({.stop=5}).rename("strategy");
    //     auto benchmark_rets = simple_benchmark.rename("benchmark");
    //     constexpr int rolling_window = 2;
    //
    //     // Expected beta value at index 2 is 0
    //     auto rolling_beta_result = RollingBeta(concat({.frames={returns, benchmark_rets}, .axis=AxisType::Column}), rolling_window);
    //
    //     // Check specific value at index 2
    //     INFO(rolling_beta_result);
    //     REQUIRE(rolling_beta_result.iloc(2).value()->ApproxEquals(*arrow::MakeScalar(0))); // Should be approximately 0
    // }
}