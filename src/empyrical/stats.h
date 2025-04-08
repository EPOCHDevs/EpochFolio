//
// Created by adesola on 1/6/25.
//

#pragma once
#include "periods.h"
#include <epoch_frame/series.h>
#include <epoch_frame/dataframe.h>
#include <epoch_core/common_utils.h>
#include <numeric>


namespace epoch_folio::ep {
    constexpr double NAN_SCALAR = std::numeric_limits<double>::quiet_NaN();
    constexpr double INF_SCALAR = std::numeric_limits<double>::infinity();
    constexpr double EPSILON_SCALAR = std::numeric_limits<double>::epsilon();

    using SeriesOrScalar = std::variant<epoch_frame::Scalar, epoch_frame::Series>;

    epoch_frame::Series AggregateReturns(epoch_frame::Series const& returns, epoch_core::EmpyricalPeriods convertTo);

    inline int AnnualizationFactor(epoch_core::EmpyricalPeriods period,
                                   std::optional<int> const &annualization) {
        return annualization.value_or(epoch_core::lookup(ANNUALIZATION_FACTORS, period));
    }

    epoch_frame::Series CumReturns(const epoch_frame::Series &returns, double startingValue = 0);

    double CumReturnsFinal(const epoch_frame::Series &returns, double startingValue = 0);

    epoch_frame::Series DrawDownSeries(epoch_frame::Series const &returns);

    inline epoch_frame::Series SimpleReturns(epoch_frame::Series const &prices) {
        return prices.pct_change().iloc({.start=1});
    }

    inline epoch_frame::Series AdjustReturns(epoch_frame::Series const& returns, SeriesOrScalar const& adjFactor) {
        return std::visit([&]<typename T>(T const &x) {
            if constexpr (std::is_same_v<T, epoch_frame::Scalar>) {
                return x == epoch_frame::Scalar{0} ? returns : returns - x;
            } else {
                return returns - x;
            }
        }, adjFactor);
    }

    inline epoch_frame::Series Clip(epoch_frame::Series const& x, double min, double max) {
        arrow::compute::ElementWiseAggregateOptions option{false};
        return {x.index(), epoch_frame::AssertResultIsOk(arrow::compute::MaxElementWise(
                {epoch_frame::AssertResultIsOk(arrow::compute::MinElementWise({x.array(), arrow::Datum(max)}, option)),
                 arrow::Datum(min)}, option)).chunked_array()};
    }

    /**
 * \brief Compute the k-th central moment of a 1D epoch_frame::Series.
 *
 *  - If meanVal is NaN, we compute the mean from the data.
 *  - If order == 0, returns 1 (by definition).
 *  - If order == 1 and meanVal is not provided, returns 0 (the first central moment).
 *  - Otherwise, uses exponentiation by squaring to compute (x - mean)^order in fewer multiplications.
 *  - Issues a warning if potential precision loss is detected (data nearly identical).
 */
    double Moment(epoch_frame::Series const &data,
                  int order,
                  double meanVal = NAN_SCALAR);

    double RValue(epoch_frame::Array const& x, epoch_frame::Array const& y);
}