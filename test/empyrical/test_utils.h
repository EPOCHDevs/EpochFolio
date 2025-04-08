//
// Created by adesola on 1/7/25.
//

#pragma once
#include <epoch_core/ranges_to.h>
#include <random>
#include <epoch_frame/frame_or_series.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/scalar_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>

using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame::factory;
using namespace epoch_frame;

inline std::vector<double> operator/(std::vector<double> const& x, double y) {
        return x | std::views::transform([y](double a) {
            return a / y;
        }) | epoch_core::ranges::to_vector_v;
}

inline Series make_randn_series(IndexPtr const& index, std::string const& name, double mean, double std) {
        size_t size = index->size();
        std::vector<double> result(size);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::normal_distribution<> d(mean, std);
        for (size_t i = 0; i < size; ++i) {
            result[i] = d(gen);
        }
    return make_series(index, result, name);
}

struct TestUtils {
        // Helper to build a epoch_frame::Series from a vector of doubles.
        Series linspace(double start, double end, const IndexPtr & index, bool endPoint=true) {
                int64_t num = static_cast<int64_t>(index->size());
                std::vector<double> result(num);
                double step;

                if (endPoint) {
                        step = (end - start) / static_cast<double>(num - 1);
                } else {
                        step = (end - start) / static_cast<double>(num);
                }

                for (int i = 0; i < num; ++i) {
                        result[i] = start + i * step;
                }

                // If endpoint is false and there are at least 2 elements, make sure the last element is not equal to end.
                if (!endPoint && num >= 2) {
                        result[num - 1] = result[num - 2] + step;
                }

                return make_series(index, result, "");
        }

        const IndexPtr dateRange1 = date_range({.start="2000-01-30"_date, .periods=9, .offset = offset::days(1)});
        const IndexPtr dateRange2 = date_range({.start="2000-01-30"_date, .periods=3, .offset = offset::days(1)});

        const IndexPtr oneDateRange = date_range({.start="2000-01-30"_date, .periods=1, .offset = offset::days(1)});
        const IndexPtr emptyDateRange = date_range({.start="2000-01-30"_date, .periods=0, .offset = offset::days(1)});
        const IndexPtr thousandDateRange = date_range({.start="2000-01-30"_date, .periods=1000, .offset = offset::days(1)});

        const IndexPtr dateRangeWeek = date_range({.start="2000-01-30"_date, .periods=9, .offset = offset::weeks(1)});
        const IndexPtr dateRangeMonth = date_range({.start="2000-01-30"_date, .periods=9, .offset = offset::month_end(1)});

        static constexpr auto nan_ = std::numeric_limits<double>::quiet_NaN();

        const epoch_frame::Series simple_benchmark = make_series(dateRange1,
            std::vector{0., 1., 0., 1., 0., 1., 0., 1., 0.} / 100.0, "simple_benchmark");

        const epoch_frame::Series positive_returns = make_series(dateRange1,
            std::vector{1., 2., 1., 1., 1., 1., 1., 1., 1.} / 100, "positive_returns");


        const epoch_frame::Series negative_returns = make_series(dateRange1,
            std::vector{0., -6., -7., -1., -9., -2., -6., -8., -5.} / 100);

        const epoch_frame::Series all_negative_returns = make_series(dateRange1,
            std::vector{-2., -6., -7., -1., -9., -2., -6., -8., -5.} / 100);

        const epoch_frame::Series mixed_returns = make_series(dateRange1,
            std::vector{nan_, 1., 10., -4., 2., 3., 2., 1., -10.} / 100.0);

        const epoch_frame::Series flat_line_1 = make_series(dateRange1,
            std::vector{1., 1., 1., 1., 1., 1., 1., 1., 1.} / 100.0);

        const epoch_frame::Series weekly_returns = make_series(dateRangeWeek,
            std::vector{0., 1., 10., -4., 2., 3., 2., 1., -10.} / 100.0);

        const epoch_frame::Series monthly_returns = make_series(dateRangeMonth,
            std::vector{0., 1., 10., -4., 2., 3., 2., 1., -10.} / 100.0);

        const epoch_frame::Series one_return  = make_series(oneDateRange,
            std::vector{1.0} / 100.0, "one_return");

        const epoch_frame::Series empty_returns  = make_series(emptyDateRange,
            std::vector<double>{} / 100.0, "one_return");


        const epoch_frame::Series noise  = make_randn_series(thousandDateRange, "noise", 0, 0.001);

        const epoch_frame::Series noise_uniform  = make_randn_series(thousandDateRange, "noise_uniform", -0.01, 0.01);

        Series inv_noise() const {
                return noise * Scalar{-1};
        };

        const epoch_frame::Series flat_line_0  = make_series(thousandDateRange,
            std::vector<double>(1000), "flat_line_0");

        const epoch_frame::Series flat_line_1_tz  = make_series(thousandDateRange,
            std::vector<double>(1000, 0.01), "flat_line_1");

        const Series pos_line  = linspace(0, 1, thousandDateRange);
        const epoch_frame::Series neg_line  = linspace(0, -1, thousandDateRange);

        const std::vector<double> one {
                -0.00171614, 0.01322056, 0.03063862, -0.01422057, -0.00489779,
                0.01268925, -0.03357711, 0.01797036
        };

        const std::vector<double> two {
                0.01846232, 0.00793951, -0.01448395, 0.00422537, -0.00339611,
                0.03756813, 0.0151531, 0.03549769
        };
};