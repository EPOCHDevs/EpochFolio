//
// Created by adesola on 1/6/25.
//
#include <epoch_core/catch_defs.h>
#include <epoch_frame/frame_or_series.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/scalar_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>
#include <empyrical/stats.h>
#include <empyrical/utils.h>
#include "empyrical/alpha_beta.h"
#include "empyrical/annual_returns.h"
#include "empyrical/annual_volatility.h"
#include "empyrical/calmar_ratio.h"
#include "empyrical/down_side_risk.h"
#include "empyrical/excess_sharpe.h"
#include "empyrical/kurtosis.h"
#include "empyrical/max_drawdown.h"
#include "empyrical/omega_ratio.h"
#include "empyrical/sharpe_ratio.h"
#include "empyrical/skew.h"
#include "empyrical/sortino_ratio.h"
#include "empyrical/stability_of_timeseries.h"
#include "empyrical/tail_ratio.h"
#include "empyrical/var.h"
#include "test_utils.h"
#include <sstream>


using namespace epoch_frame;
using namespace epoch_frame::factory;
using namespace epoch_folio::ep;
using Catch::Approx;
using namespace epoch_core;

struct ParamBase{
    std::string title;
    Series input;
    std::vector<double> expected;
};

void AssertIndicesMatch(IndexPtr const& lhs, IndexPtr const& rhs) {
    INFO(lhs->repr() << "\n" << rhs->repr());
    REQUIRE(lhs->equals(rhs));
}

inline void AlmostClose(double lhs, double rhs, int64_t dp) {
    INFO(lhs << " != " << rhs);

    if (std::isnan(lhs) || std::isnan(rhs)) {
        REQUIRE(((std::isnan(lhs)) && (std::isnan(rhs))));
    }
    else {
        REQUIRE_THAT(lhs, Catch::Matchers::WithinAbs(rhs, std::pow(10, -dp)));
    }
}

inline void AlmostClose(std::vector<double> const& lhs, std::vector<double> const& rhs, int64_t dp) {
    for (int i = 0; i < lhs.size(); i++) {
        AlmostClose(lhs.at(i), rhs.at(i), dp);
    }
}

inline void AlmostClose(Series const& lhs, Series const& rhs, int64_t dp) {
    for (int i = 0; i < lhs.size(); i++) {
        AlmostClose(lhs.iloc(i).as_double(), rhs.iloc(i).as_double(), dp);
    }
    AssertIndicesMatch(lhs.index(), rhs.index());
}

#define ALMOST_CLOSE(lhs, rhs, dp) AlmostClose(lhs, rhs, dp)

TEST_CASE("Test Simple Returns")
{
    TestUtils test_utils;

    std::vector<ParamBase> params{
            {"Flat Line",     test_utils.flat_line_1, std::vector<double>(9 - 1)},
            {"Positive Line", test_utils.pos_line,    std::views::iota(0, 999) | std::views::transform(
                    [](size_t x) { return 1.0 / static_cast<double>(x); }) | ranges::to_vector_v}
    };

    for (auto const &[name, prices, expected]: params) {
        auto result = SimpleReturns(prices);

        DYNAMIC_SECTION(name)
        {
            auto expected_series = make_series(prices.index()->iloc({.start=1}), expected);
            INFO(result << "\nexpected:\n" << expected_series);
            ALMOST_CLOSE(result, expected_series, 4);
        }
    }
}

struct ParamBaseWithStartValue{
    std::string title;
    Series input;
    double startValue{};
    std::vector<double> expected;
};

TEST_CASE("Test Cum Returns")
{
    TestUtils test_utils;
    std::vector<ParamBaseWithStartValue> params{
            {"Empty Returns",                      test_utils.empty_returns,    0,   std::vector<double>{}},
            {"Mixed Returns",                      test_utils.mixed_returns,    0,   std::vector{0.0, 0.01, 0.111, 0.066559,
                                                                                       0.08789, 0.12052, 0.14293,
                                                                                       0.15436, 0.03893}},
            {"Mixed Returns with start value 100", test_utils.mixed_returns,    100, std::vector{100.0, 101.0, 111.1, 106.65599,
                                                                                       108.78912, 112.05279, 114.29384,
                                                                                       115.43678, 103.89310}},
            {"Negative Returns",                   test_utils.negative_returns, 0,   std::vector{0.0, -0.06, -0.1258, -0.13454,
                                                                                       -0.21243, -0.22818, -0.27449,
                                                                                       -0.33253, -0.36590}}
    };

    for (auto const &[name, prices, start, expected]: params) {
        auto result = epoch_folio::ep::CumReturns(prices, start);
        DYNAMIC_SECTION(name)
        {
            REQUIRE(result.size() == expected.size());
            if (!expected.empty()) {
                auto actual = result.contiguous_array().to_vector<double>();
                for (size_t i = 0; i < expected.size(); i++) {
                    ALMOST_CLOSE(actual[i], expected[i], 4);
                }
            }
        }
    }
}

TEST_CASE("Test Cum Returns Final")
{
    TestUtils test_utils;
    std::vector<ParamBaseWithStartValue> params{
            {"Empty Returns",                      test_utils.empty_returns,    0,   std::vector<double>{std::numeric_limits<double>::quiet_NaN()}},
            {"One Return",                         test_utils.one_return,       0,   test_utils.one_return.contiguous_array().to_vector<double>()},
            {"Mixed Returns",                      test_utils.mixed_returns,    0,   std::vector<double>{0.03893}},
            {"Mixed Returns with start value 100", test_utils.mixed_returns,    100, std::vector<double>{103.89310}},
            {"Negative Returns",                   test_utils.negative_returns, 0,   std::vector<double>{-0.36590}}
    };

    for (auto const &[name, prices, start, expected]: params) {
        auto result = CumReturnsFinal(prices, start);
        DYNAMIC_SECTION(name)
        {
            ALMOST_CLOSE(result, expected[0], 4);
        }
    }
}

constexpr double DECIMAL = 1e-8;
TEST_CASE("Test Aggregate Return")
{
    struct TestData {
        std::string name;
        Series input;
        EmpyricalPeriods period;
        std::vector<double> expected;
    };

    TestUtils test_utils;
    std::vector<TestData> testCases{
            {"Simple Benchmark (Weekly)",    test_utils.simple_benchmark, EmpyricalPeriods::weekly,    {0.0, 0.040604010000000024, 0.0}},
            {"Simple Benchmark (Monthly)",   test_utils.simple_benchmark, EmpyricalPeriods::monthly,   {0.01, 0.03030099999999991}},
            {"Simple Benchmark (Quarterly)", test_utils.simple_benchmark, EmpyricalPeriods::quarterly, {0.04060401}},
            {"Simple Benchmark (Yearly)",    test_utils.simple_benchmark, EmpyricalPeriods::yearly,    {0.040604010000000024}},
            {"Weekly Returns (Monthly)",     test_utils.weekly_returns,   EmpyricalPeriods::monthly,   {0.0, 0.087891200000000058, -0.04500459999999995}},
            {"Weekly Returns (Yearly)",      test_utils.weekly_returns,   EmpyricalPeriods::yearly,    {0.038931091700480147}},
            {"Monthly Returns (Yearly)",     test_utils.monthly_returns,  EmpyricalPeriods::yearly,    {0.038931091700480147}},
            {"Monthly Returns (Quarterly)",  test_utils.monthly_returns,  EmpyricalPeriods::quarterly, {0.11100000000000021, 0.008575999999999917, -0.072819999999999996}}
    };

    for (const auto&[name, input, period, expected] : testCases) {
        auto result = AggregateReturns(input, period);
        DYNAMIC_SECTION(name) {
            auto resultValues = result.contiguous_array().to_vector<double>();
            INFO(result);
            ALMOST_CLOSE(resultValues, expected, 4);
        }
    }
}

TEST_CASE("Test Max Drawdown")
{
    struct TestData {
        std::string name;
        Series input;
        double expected;
    };

    TestUtils test_utils;
    std::vector<TestData> testCases{
            {"Empty Returns",           test_utils.empty_returns,        std::numeric_limits<double>::quiet_NaN()},
            {"One Return",              test_utils.one_return,           0.0},
            {"Simple Benchmark",        test_utils.simple_benchmark,     0.0},
            {"Mixed Returns",           test_utils.mixed_returns,        -0.1},
            {"Positive Returns",        test_utils.positive_returns,     -0.0},
            {"Negative Returns",        test_utils.negative_returns,     CumReturnsFinal(test_utils.negative_returns)},
            {"All Negative Returns",    test_utils.all_negative_returns, CumReturnsFinal(test_utils.all_negative_returns)},
            {"Custom Series",           make_series(test_utils.dateRange2, std::vector<double>{0.10, -0.10, 0.10},
                                                   "custom_series"),      -0.10}
    };

    for (const auto& testCase : testCases) {
        auto result = MaxDrawDown()(testCase.input);
        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(result, testCase.expected, DECIMAL);
        }
    }
}

TEST_CASE("Test Max Drawdown Translation")
{
    struct TestData {
        std::string name;
        Series returns;
        Scalar constant;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise with 0.0001",       test_utils.noise,         0.0001_scalar},
            {"test_utils.noise with 0.001",        test_utils.noise,         0.001_scalar},
            {"test_utils.noise Uniform with 0.01", test_utils.noise_uniform, 0.01_scalar},
            {"test_utils.noise Uniform with 0.1",  test_utils.noise_uniform, 0.1_scalar},
    };

    epoch_folio::ep::MaxDrawDown maxDrawDown;
    for (const auto &[name, returns, constant] : testCases) {
        auto depressed_returns = returns - constant;
        auto raised_returns = returns + constant;

        double max_dd = maxDrawDown(returns);
        double depressed_dd = maxDrawDown(depressed_returns);
        double raised_dd = maxDrawDown(raised_returns);

        DYNAMIC_SECTION(name) {
            REQUIRE(max_dd <= raised_dd);
            REQUIRE(depressed_dd <= max_dd);
        }
    }
}

TEST_CASE("Test Annual Return")
{
    struct TestData {
        std::string name;
        Series returns;
        EmpyricalPeriods period;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Mixed Returns - Daily", test_utils.mixed_returns, EmpyricalPeriods::daily, 1.9135925373194231},
            {"Weekly Returns - Weekly", test_utils.weekly_returns, EmpyricalPeriods::weekly, 0.24690830513998208},
            {"Monthly Returns - Monthly", test_utils.monthly_returns, EmpyricalPeriods::monthly, 0.052242061386048144},
    };

    for (const auto& testCase : testCases) {
        double annual_ret = epoch_folio::ep::AnnualReturns(testCase.period)(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(annual_ret == Approx(testCase.expected).epsilon(DECIMAL));
        }
    }
}

TEST_CASE("Test Annual Volatility")
{
    struct TestData {
        std::string name;
        Series returns;
        EmpyricalPeriods period;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Flat Line 1 TZ - Daily",    test_utils.flat_line_1_tz,  EmpyricalPeriods::daily,   0.0},
            {"Mixed Returns - Daily",     test_utils.mixed_returns,   EmpyricalPeriods::daily,   0.9136465399704637},
            {"Weekly Returns - Weekly",   test_utils.weekly_returns,  EmpyricalPeriods::weekly,  0.38851569394870583},
            {"Monthly Returns - Monthly", test_utils.monthly_returns, EmpyricalPeriods::monthly, 0.18663690238892558},
    };

    for (const auto &testCase: testCases) {
        double annual_vol = AnnualVolatility(testCase.period)(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(annual_vol == Approx(testCase.expected).epsilon(DECIMAL));
        }
    }
}

TEST_CASE("Test Calmar Ratio")
{
    struct TestData {
        std::string name;
        Series returns;
        EmpyricalPeriods period;
        double expected;
    };
    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns - Daily", test_utils.empty_returns, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"One Return - Daily", test_utils.one_return, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Returns - Daily", test_utils.mixed_returns, EmpyricalPeriods::daily, 19.135925373194233},
            {"Weekly Returns - Weekly", test_utils.weekly_returns, EmpyricalPeriods::weekly, 2.4690830513998208},
            {"Monthly Returns - Monthly", test_utils.monthly_returns, EmpyricalPeriods::monthly, 0.52242061386048144},
    };

    for (const auto& testCase : testCases) {
        double calmar = epoch_folio::ep::CalmarRatio{testCase.period}(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            if (std::isnan(testCase.expected)) {
                REQUIRE(std::isnan(calmar));
            } else {
                REQUIRE(calmar == Approx(testCase.expected).epsilon(DECIMAL));
            }
        }
    }
}

TEST_CASE("Test Omega Ratio")
{
    struct TestData {
        std::string name;
        Series returns;
        double riskFree;
        double requiredReturn;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns",                         test_utils.empty_returns,    0.0,   0.0,  std::numeric_limits<double>::quiet_NaN()},
            {"One Return",                            test_utils.one_return,       0.0,   0.0,  std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Returns",                         test_utils.mixed_returns,    0.0,   10.0, 0.83354263497557934},
            {"Positive Returns",                      test_utils.positive_returns, 0.01,  0.0,  std::numeric_limits<double>::quiet_NaN()},
            {"Positive Returns with required return", test_utils.positive_returns, 0.011, 0.0,  1.125},
    };

    for (const auto &testCase: testCases) {
        double omega = epoch_folio::ep::OmegaRatio(testCase.riskFree, testCase.requiredReturn)(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(omega, testCase.expected, DECIMAL);
        }
    }
}

TEST_CASE("Test Omega Ratio for Different Required Returns")
{
    struct TestData {
        std::string name;
        Series returns;
        double required_return_less;
        double required_return_more;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise Uniform", test_utils.noise_uniform, 0.0, 0.001},
            {"test_utils.noise", test_utils.noise, 0.001, 0.002},
    };

    for (const auto& testCase : testCases) {
        double omega_less = epoch_folio::ep::OmegaRatio(0.0, testCase.required_return_less)(testCase.returns);
        double omega_more = epoch_folio::ep::OmegaRatio(0.0, testCase.required_return_more)(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(omega_less > omega_more);
        }
    }
}

TEST_CASE("Test Sharpe Ratio")
{
    struct TestData {
        std::string name;
        Series returns;
        std::variant<epoch_frame::Scalar, Series> risk_free;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns",                          test_utils.empty_returns,    epoch_frame::Scalar{0}, std::numeric_limits<double>::quiet_NaN()},
            {"One Return",                             test_utils.one_return,       epoch_frame::Scalar{0}, std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Returns with Mixed Benchmark", test_utils.mixed_returns, test_utils.mixed_returns, std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Returns with Zero Risk-Free Rate", test_utils.mixed_returns,    epoch_frame::Scalar{0}, 1.7238613961706866},
            {"Mixed Returns with Simple Benchmark", test_utils.mixed_returns, test_utils.simple_benchmark, 0.34111411441060574},
            {"Positive Returns",                       test_utils.positive_returns, epoch_frame::Scalar{0}, 52.915026221291804},
            {"Negative Returns",                       test_utils.negative_returns, epoch_frame::Scalar{0}, -24.406808633910085},
            {"Flat Line 1",                            test_utils.flat_line_1,      epoch_frame::Scalar{0}, std::numeric_limits<double>::infinity()},
    };

    for (const auto &testCase: testCases) {
        double sharpe = epoch_folio::ep::SharpeRatio(testCase.risk_free)(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            if (std::isnan(testCase.expected)) {
                REQUIRE(std::isnan(sharpe));
            }
            else if (std::isinf(testCase.expected)) {
                INFO(sharpe);
                REQUIRE(std::isinf(sharpe));
            }
            else {
                REQUIRE(sharpe == Approx(testCase.expected).epsilon(1e-8));
            }
        }
    }
}

TEST_CASE("Test Sharpe Ratio Translation (Same Translation Amount)")
{
    struct TestData {
        std::string name;
        Series returns;
        Scalar required_return;
        Scalar translation;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise Uniform, Translation 0.005",               test_utils.noise_uniform, 0_scalar,     0.005_scalar},
            {"test_utils.noise Uniform, Translation 0.005 Same Required", test_utils.noise_uniform, 0.005_scalar, 0.005_scalar},
    };

    for (const auto &testCase: testCases) {
        double sr = SharpeRatio(testCase.required_return)(testCase.returns);
        double sr_depressed = SharpeRatio(testCase.required_return - testCase.translation)(
                testCase.returns - testCase.translation);
        double sr_raised = SharpeRatio(testCase.required_return + testCase.translation)(
                testCase.returns + testCase.translation);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(sr == Approx(sr_depressed).epsilon(1e-8));
            REQUIRE(sr == Approx(sr_raised).epsilon(1e-8));
        }
    }
}

TEST_CASE("Test Sharpe Ratio Translation (Different Translation Amounts)")
{
    struct TestData {
        std::string name;
        Series returns;
        Scalar required_return;
        Scalar translation_returns;
        Scalar translation_required;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise Uniform, Different Translations", test_utils.noise_uniform, epoch_frame::Scalar{0}, 0.0002_scalar, 0.0001_scalar},
            {"test_utils.noise Uniform, Reverse Translations", test_utils.noise_uniform, epoch_frame::Scalar{0.005}, 0.0001_scalar, 0.0002_scalar},
    };

    for (const auto& testCase : testCases) {
        double sr = SharpeRatio(testCase.required_return)(testCase.returns);
        double sr_depressed = SharpeRatio(testCase.required_return - testCase.translation_required)(testCase.returns - testCase.translation_returns);
        double sr_raised = SharpeRatio(testCase.required_return + testCase.translation_required)(testCase.returns + testCase.translation_returns);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(sr != sr_depressed);
            REQUIRE(sr != sr_raised);
        }
    }
}

TEST_CASE("Test Sharpe Ratio for Different test_utils.noise Levels")
{
    struct TestData {
        std::string name;
        double small_std;
        double large_std;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Small Std 0.001, Large Std 0.002", 0.001, 0.002},
            {"Small Std 0.01, Large Std 0.02",   0.01,  0.02},
    };

    for (const auto &testCase: testCases) {
        IndexPtr index = date_range({.start="2000-01-30"_date, .periods=1000, .offset = offset::days(1)});
        Series smaller_normal = make_randn_series(index, "smaller_normal", 0.01, testCase.small_std);
        Series larger_normal = make_randn_series(index, "larger_normal", 0.01, testCase.large_std);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(epoch_folio::ep::SharpeRatio(epoch_frame::Scalar{0.001})(smaller_normal) >
                    epoch_folio::ep::SharpeRatio(epoch_frame::Scalar{0.001})(larger_normal));
        }
    }
}

TEST_CASE("Test Downside Risk")
{
    struct TestData {
        std::string name;
        Series returns;
        SeriesOrScalar required_return;
        EmpyricalPeriods period;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns - Daily", test_utils.empty_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"One Return - Daily", test_utils.one_return, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, 0.0},
            {"Mixed Returns", test_utils.mixed_returns, test_utils.mixed_returns, EmpyricalPeriods::daily, 0},
            {"Mixed Returns - No Required Return", test_utils.mixed_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, 0.60448325038829653},
            {"Mixed Returns - Required Return 0.1", test_utils.mixed_returns, epoch_frame::Scalar{0.1}, EmpyricalPeriods::daily, 1.7161730681956295},
            {"Weekly Returns - Required Return 0.0", test_utils.weekly_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::weekly, 0.25888650451930134},
            {"Weekly Returns - Required Return 0.1", test_utils.weekly_returns, epoch_frame::Scalar{0.1}, EmpyricalPeriods::weekly, 0.7733045971672482},
            {"Monthly Returns - Required Return 0.0", test_utils.monthly_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::monthly, 0.1243650540411842},
            {"Monthly Returns - Required Return 0.1", test_utils.monthly_returns, epoch_frame::Scalar{0.1}, EmpyricalPeriods::monthly, 0.37148351242013422}
    };

    for (const auto& testCase : testCases) {
        auto result = epoch_folio::ep::DownsideRisk(testCase.required_return, testCase.period)(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            if (std::isnan(testCase.expected)) {
                REQUIRE(std::isnan(result));
            }else {
                REQUIRE(result == Approx(testCase.expected).epsilon(DECIMAL));
            }
        }
    }
}

TEST_CASE("Test Downside Risk with Noisy Returns")
{
    struct TestData {
        std::string name;
        Series noise;
        Series flat_line;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise vs Flat Line 0",         test_utils.noise,         test_utils.flat_line_0},
            {"test_utils.noise Uniform vs Flat Line 0", test_utils.noise_uniform, test_utils.flat_line_0},
    };

    for (const auto &testCase: testCases) {
        auto index = testCase.flat_line.index();

        auto noisy_returns_1 = testCase.noise.iloc({0, 250}).reindex(index, 0.0_scalar) +
                               testCase.flat_line.iloc({.start=250}).reindex(index, 0.0_scalar) ;
        auto noisy_returns_2 = testCase.noise.iloc({0, 500}).reindex(index, 0.0_scalar)  +
                               testCase.flat_line.iloc({.start=500}).reindex(index, 0.0_scalar) ;
        auto noisy_returns_3 = testCase.noise.iloc({0, 750}).reindex(index, 0.0_scalar)  +
                               testCase.flat_line.iloc({.start=750}).reindex(index, 0.0_scalar) ;

        auto dr_1 = epoch_folio::ep::DownsideRisk(epoch_frame::Scalar{0})(noisy_returns_1);
        auto dr_2 = epoch_folio::ep::DownsideRisk(epoch_frame::Scalar{0})(noisy_returns_2);
        auto dr_3 = epoch_folio::ep::DownsideRisk(epoch_frame::Scalar{0})(noisy_returns_3);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(dr_1 <= dr_2);
            REQUIRE(dr_2 <= dr_3);
        }
    }
}

TEST_CASE("Test Downside Risk with Translating Required Return")
{
    struct TestData {
        std::string name;
        Series returns;
        epoch_frame::Scalar required_return;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"test_utils.noise Translation - 0.005", test_utils.noise, epoch_frame::Scalar{0.005}},
            {"test_utils.noise Uniform Translation - 0.005", test_utils.noise_uniform, epoch_frame::Scalar{0.005}},
    };

    for (const auto& testCase : testCases) {
        auto dr_0 = epoch_folio::ep::DownsideRisk(-1_scalar * testCase.required_return)(testCase.returns);
        auto dr_1 = epoch_folio::ep::DownsideRisk(epoch_frame::Scalar{0})(testCase.returns);
        auto dr_2 = epoch_folio::ep::DownsideRisk(testCase.required_return)(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(dr_0 <= dr_1);
            REQUIRE(dr_1 <= dr_2);
        }
    }
}

TEST_CASE("Test Downside Risk with Standard Deviation")
{
    struct TestData {
        std::string name;
        double smaller_std;
        double larger_std;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Small Std 0.001, Large Std 0.002", 0.001, 0.002},
            {"Small Std 0.001, Large Std 0.01",  0.001, 0.01},
            {"Zero Std, Large Std 0.001",        0.0,   0.001},
    };

    auto index = from_range(1000);
    for (const auto& testCase : testCases) {
        Series less_noise = (testCase.smaller_std > 0) ?
        make_randn_series(index, "", 0.0, testCase.smaller_std) :
                                make_series(index, std::vector<double>(1000, 0.0));

        Series more_noise = (testCase.larger_std > 0) ?
        make_randn_series(index, "", 0.0, testCase.larger_std)  :
                                make_series(index, std::vector<double>(1000, 0.0));

        DYNAMIC_SECTION(testCase.name) {
            REQUIRE(epoch_folio::ep::DownsideRisk()(less_noise) <
                    epoch_folio::ep::DownsideRisk()(more_noise));
        }
    }
}

TEST_CASE("Test Sortino Ratio")
{
    struct TestData {
        std::string name;
        Series returns;
        SeriesOrScalar required_return;
        EmpyricalPeriods period;
        double expected; // Scalar expected value for simplicity
    };
    TestUtils test_utils;
    std::vector<TestData> testCases{
            {"Empty Returns - Daily",       test_utils.empty_returns,    epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"One Return - Daily",          test_utils.one_return,       epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Return",                test_utils.mixed_returns,       test_utils.mixed_returns, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Returns - Daily",       test_utils.mixed_returns,    epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, 2.605531251673693},
            {"Mixed Returns - Flat Line 1", test_utils.mixed_returns,    test_utils.flat_line_1, EmpyricalPeriods::daily, -1.3934779588919977},
            {"Positive Returns - Daily",    test_utils.positive_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::numeric_limits<double>::infinity()},
            {"Negative Returns - Daily",    test_utils.negative_returns, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, -13.532743075043401},
            {"Simple Benchmark - Daily",    test_utils.simple_benchmark, epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::numeric_limits<double>::infinity()},
            {"Weekly Returns - Weekly",     test_utils.weekly_returns,   epoch_frame::Scalar{0.0}, EmpyricalPeriods::weekly, 1.1158901056866439},
            {"Monthly Returns - Monthly",   test_utils.monthly_returns,  epoch_frame::Scalar{0.0}, EmpyricalPeriods::monthly, 0.53605626741889756}
    };

    for (const auto& testCase : testCases) {
        auto result = epoch_folio::ep::SortinoRatio(testCase.required_return, testCase.period)(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            if (std::isnan(testCase.expected)) {
                REQUIRE(std::isnan(result));
            } else if (std::isinf(testCase.expected)) {
                REQUIRE(std::isinf(result));
            } else {
                REQUIRE(result == Approx(testCase.expected).epsilon(1e-8));
            }
        }
    }
}

TEST_CASE("Test Sortino Ratio - Translation Same")
{
    struct TestData {
        std::string name;
        Series returns;
        Scalar required_return;
        Scalar translation;
    };

    TestUtils test_utils;
    std::vector<TestData> testCases{
            {"test_utils.noise Uniform - Translation 0.005", test_utils.noise_uniform, epoch_frame::Scalar{0}, 0.005_scalar},
            {"test_utils.noise Uniform - Same Required",     test_utils.noise_uniform, epoch_frame::Scalar{0.005}, 0.005_scalar},
    };

    for (const auto& testCase : testCases) {
        double sr = epoch_folio::ep::SortinoRatio(testCase.required_return)(testCase.returns);
        double sr_depressed = epoch_folio::ep::SortinoRatio(testCase.required_return - testCase.translation)(testCase.returns - testCase.translation);
        double sr_raised = epoch_folio::ep::SortinoRatio(testCase.required_return + testCase.translation)(testCase.returns + testCase.translation);

        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(sr, sr_depressed, DECIMAL);
            ALMOST_CLOSE(sr, sr_raised, DECIMAL);
        }
    }
}

TEST_CASE("Test Stability of TimeSeries")
{
    struct TestData {
        std::string name;
        Series returns;
        double expected; // Scalar expected value for simplicity
    };

    TestUtils test_utils;
    std::vector<TestData> testCases{
            {"Empty Returns ", test_utils.empty_returns, std::numeric_limits<double>::quiet_NaN()},
            {"One Return ",    test_utils.one_return,    std::numeric_limits<double>::quiet_NaN()},
            {"Mixed Return",   test_utils.mixed_returns, 0.1529973665111273},
            {"Flat line 1",    test_utils.flat_line_1,   1.0}
    };

    for (const auto &testCase: testCases) {
        auto result = StabilityOfTimeseries()(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(result, testCase.expected, DECIMAL);
        }
    }
}

TEST_CASE("Test Tail Ratio of TimeSeries")
{
    struct TestData {
        std::string name;
        Series returns;
        double expected; // Scalar expected value for simplicity
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns ", test_utils.empty_returns, std::numeric_limits<double>::quiet_NaN()},
            {"One Return ",    test_utils.one_return,    1},
            {"Mixed Return",   test_utils.mixed_returns, 0.9473684210526313}
    };

    for (const auto &testCase: testCases) {
        auto result = TailRatio()(testCase.returns);

        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(result, testCase.expected, DECIMAL);
        }
    }
}

TEST_CASE("Test CAGR")
{
    struct TestData {
        std::string name;
        Series returns;
        EmpyricalPeriods period;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns",    test_utils.empty_returns, EmpyricalPeriods::daily, std::numeric_limits<double>::quiet_NaN()},
            {"One Return",       test_utils.one_return,    EmpyricalPeriods::daily, 11.274002099240244},
            {"Mixed Returns",    test_utils.mixed_returns, EmpyricalPeriods::daily, 1.9135925373194231},
            {"Flat Line 1 TZ",   test_utils.flat_line_1_tz, EmpyricalPeriods::daily, 11.274002099240256},
    };

    for (const auto& testCase : testCases) {
        auto result = epoch_folio::ep::CAGR(testCase.period)(testCase.returns);
        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(result, testCase.expected, 8);
        }
    }
}

TEST_CASE("Test Value at Risk (VaR)")
{
    std::vector<std::pair<std::vector<double>, double>> testCases = {
            {{1.0, 2.0}, 0.0},
            {{1.0, 2.0}, 0.3},
            {{1.0, 2.0}, 1.0},
            {{1, 81, 82, 83, 84, 85}, 0.1},
            {{1, 81, 82, 83, 84, 85}, 0.2},
            {{1, 81, 82, 83, 84, 85}, 0.3}
    };

    std::vector<double> expectedValues = {
            1.0, 1.3, 2.0, 41, 81, 81.5
    };

    for (size_t i = 0; i < testCases.size(); ++i) {
        auto [returns, cutoff] = testCases[i];

        std::stringstream ss;
        ss << "[";
        for(auto const& r: returns)
        {
            ss << r << " ";
        }
        ss << "]";

        DYNAMIC_SECTION("Returns: " << ss.str()  << " - Cutoff: " << cutoff )
        {
            double result = epoch_folio::ep::ValueAtRisk(cutoff)(make_series(from_range(returns.size()), returns));
            ALMOST_CLOSE(result, expectedValues[i], 8);
        }
    }

    // Test a returns stream of 21 data points at different cutoffs.
    auto returns = make_randn_series(from_range(21),  "", 0.0, 0.02);
    for (double cutoff : {0.0, 0.0499, 0.05, 0.20, 0.999, 1.0}) {
        DYNAMIC_SECTION("Extended with Cutoff: " << cutoff )
        {
            auto expected = returns.quantile(arrow::compute::QuantileOptions{cutoff}).as_double();
            REQUIRE(epoch_folio::ep::ValueAtRisk(cutoff)(returns) == Approx(expected).epsilon(1e-8));
        }
    }
}

TEST_CASE("Test Conditional Value at Risk (CVaR)")
{
    using value_at_risk = epoch_folio::ep::ValueAtRisk;
    using conditional_value_at_risk = epoch_folio::ep::ConditionalValueAtRisk;

    // Single-valued array
    auto single_return =  make_randn_series(from_range(1), "",  0.0, 0.02);
    double expected_cvar = single_return.iloc(0).as_double();
    ALMOST_CLOSE(conditional_value_at_risk(0.0)(single_return), expected_cvar, 8);
    ALMOST_CLOSE(conditional_value_at_risk(1.0)(single_return), expected_cvar, 8);

    // 21 data points with different cutoffs
    auto returns = make_randn_series(from_range(21), "", 0.0, 0.02);
    for (double cutoff : {0.0, 0.0499, 0.05, 0.20, 0.999, 1.0}) {
        Scalar var{value_at_risk(cutoff)(returns)};
        expected_cvar = returns.loc(returns <= var).mean().as_double();
        ALMOST_CLOSE(conditional_value_at_risk(cutoff)(returns), expected_cvar, 8);
    }
}

TEST_CASE("Test Skew")
{
    struct TestData {
        std::string name;
        std::vector<double> values;
        bool bias{true};
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Test Case with nan",                      {0, 1, 2, 3, 4, 5, 6, 7, NAN_SCALAR, 9},  true,  NAN_SCALAR},

            {"Single Value",              {4},                                      true,  std::numeric_limits<double>::quiet_NaN()},
            {"Test Case",                 {1,     2,      3,      4},               true,  0},
            {"Skewness of constant is nan", {1,     1,      1,      1},               true,  std::numeric_limits<double>::quiet_NaN()},
            {"Test Case with bias false", {1.165, 0.6268, 0.0751, 0.3516, -0.6965}, false, -0.437111105023940},
            {"Test Case with bias true", {1.165, 0.6268, 0.0751, 0.3516, -0.6965}, true,  -0.29322304336607}
    };

    for (const auto &[name, values, bias, expected]: testCases) {
        auto result = epoch_folio::ep::Skew(bias)(make_series(from_range(values.size()), values));
        DYNAMIC_SECTION(name) {
            ALMOST_CLOSE(result, expected, 8);
        }
    }
}


TEST_CASE("Test Kurtosis")
{
    struct TestData {
        std::string name;
        std::vector<double> values;
        bool fisher{true};
        bool bias{true};
        double expected;
    };

    std::vector mathworks{1.165,
                          0.6268,
                          0.0751,
                          0.3516,
                          -0.6965};

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Test Case with nan",                      {0, 1, 2, 3, 4, 5, 6, 7, NAN_SCALAR, 9}, false, true,  NAN_SCALAR},
            {"Test Case",                               {1, 2, 3, 4},                            false, true,  1.64},
            {"Test Case with bias true and fisher off", mathworks,                               false, true,  2.1658856802973},
            {"Test Case with bias off and fisher off",  mathworks,                               false, false, 3.663542721189047},
    };

    for (const auto &[name, values, fisher, bias, expected]: testCases) {
        auto result = epoch_folio::ep::Kurtosis(fisher, bias)(make_series(from_range(values.size()), values));
        DYNAMIC_SECTION(name) {
            ALMOST_CLOSE(result, expected, 8);
        }
    }
}

TEST_CASE("Test Alpha and Beta") {
    struct TestData {
        std::string name;
        Series returns;
        Series benchmark;
        std::pair<double, double> expected;  // {alpha, beta}
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns",     test_utils.empty_returns, test_utils.simple_benchmark, {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()}},
            {"One Return",        test_utils.one_return,    test_utils.one_return,       {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::quiet_NaN()}},
            {"Mixed Returns",     test_utils.mixed_returns, test_utils.negative_returns.iloc(UnResolvedIntegerSliceBound{1}).reindex(
                    test_utils.negative_returns.index()),                {-0.9997853834885004,                      -0.7129629629629631}},
            {"Self Benchmark",    test_utils.mixed_returns, test_utils.mixed_returns,    {0.0,                                      1.0}},
            {"Inverse Benchmark", test_utils.mixed_returns, -test_utils.mixed_returns,   {0.0,                                      -1.0}}
    };

    for (const auto &testCase: testCases) {
        auto df = make_dataframe(testCase.returns.index(), {testCase.returns.array(), testCase.benchmark.array()}, {"strategy", "benchmark"});
        auto [alpha, beta] = epoch_folio::ep::AlphaBeta()(df);

        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(alpha, testCase.expected.first, 8);
            ALMOST_CLOSE(beta, testCase.expected.second, 8);
        }
    }
}

TEST_CASE("Test Alpha") {
    struct TestData {
        std::string name;
        Series returns;
        Series benchmark;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns", test_utils.empty_returns, test_utils.simple_benchmark, std::numeric_limits<double>::quiet_NaN()},
            {"One Return", test_utils.one_return, test_utils.one_return, std::numeric_limits<double>::quiet_NaN()},
            // {"Flat Line Benchmark", test_utils.mixed_returns, test_utils.flat_line_1, std::numeric_limits<double>::quiet_NaN()},
            {"Self Benchmark", test_utils.mixed_returns, test_utils.mixed_returns, 0.0},
            {"Inverse Benchmark", test_utils.mixed_returns, -test_utils.mixed_returns, 0.0}
    };

    for (const auto& testCase : testCases) {
        auto df = make_dataframe(testCase.returns.index(), {testCase.returns.array(), testCase.benchmark.array()}, {"strategy", "benchmark"});
        double alpha = Alpha()(df);
        DYNAMIC_SECTION(testCase.name) {
            ALMOST_CLOSE(alpha, testCase.expected, 8);
        }
    }
}

TEST_CASE("Test Beta") {
    struct TestData {
        std::string name;
        Series returns;
        Series benchmark;
        double expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns", test_utils.empty_returns, test_utils.simple_benchmark, std::numeric_limits<double>::quiet_NaN()},
            {"One Return", test_utils.one_return, test_utils.one_return, std::numeric_limits<double>::quiet_NaN()},
            {"Flat Line Benchmark", test_utils.mixed_returns, test_utils.flat_line_1, std::numeric_limits<double>::quiet_NaN()},
            {"test_utils.noise", test_utils.noise, test_utils.noise, 1.0},
            {"Double test_utils.noise", epoch_frame::Scalar{2} * test_utils.noise, test_utils.noise, 2.0},
            {"Inverse test_utils.noise", test_utils.noise, test_utils.inv_noise(), -1.0},
            {"Double Inverse test_utils.noise", epoch_frame::Scalar{2} * test_utils.noise, test_utils.inv_noise(), -2.0}
    };

    for (const auto& testCase : testCases) {
        auto df = make_dataframe(testCase.returns.index(), {testCase.returns.array(), testCase.benchmark.array()}, {"strategy", "benchmark"});
        INFO(df);

        double beta = epoch_folio::ep::Beta()(df);

        DYNAMIC_SECTION(testCase.name) {
            if (std::isnan(testCase.expected)) {
                REQUIRE(std::isnan(beta));
            } else {
                REQUIRE(beta == Approx(testCase.expected).epsilon(1e-8));
            }
        }
    }
}

TEST_CASE("Test Rolling Max Drawdown") {
    struct TestData {
        std::string name;
        Series returns;
        int64_t window;
        std::vector<double> expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns", test_utils.empty_returns, 6, {}},
            {"Negative Returns", test_utils.negative_returns, 6, {-0.2282, -0.2745, -0.2899, -0.2747}},
    };

    for (const auto& [name, returns, window, expected] : testCases) {
        auto result = RollMaxDrawDown{}(returns, window);
        int64_t n = returns.size();

        DYNAMIC_SECTION(name) {
            INFO(result);
            ALMOST_CLOSE(result.contiguous_array().to_vector<double>(), expected, 4);

            if (!expected.empty())
            {
                REQUIRE(result.index()->equals(returns.index()->iloc({n-expected.size()})));
            }
        }
    }
}


TEST_CASE("Test Rolling Sharpe Ratio") {
    struct TestData {
        std::string name;
        Series returns;
        size_t window;
        std::vector<double> expected;
    };

    TestUtils test_utils; std::vector<TestData> testCases{
            {"Empty Returns", test_utils.empty_returns, 6, {}},
            {"Negative Returns", test_utils.negative_returns, 6, {-18.0916, -26.7990, -26.6914, -25.7230}},
            {"Mixed Returns", test_utils.mixed_returns, 6, {7.5745, 8.2278, 8.2278, -3.1375}},
    };

    for (const auto& [name, returns, window, expected] : testCases) {
        auto result = RollSharpeRatio(epoch_frame::Scalar{0.0}, EmpyricalPeriods::daily, std::nullopt)(returns, window);
        int64_t n = returns.size();

        DYNAMIC_SECTION(name) {
            ALMOST_CLOSE(result.contiguous_array().to_vector<double>(), expected, 4);

            if (!result.empty())
            {
                REQUIRE(result.index()->equals(returns.index()->iloc({n-expected.size()})));
            }
        }
    }
}
