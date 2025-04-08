#pragma once

#include "stats.h"
#include <cmath>
#include <epoch_frame/factory/index_factory.h>

namespace epoch_folio::ep {

/**
 * \class StabilityOfTimeseries
 * \brief Computes the R-squared of a linear fit on the cumulative log returns.
 */
    class StabilityOfTimeseries {
    public:
        StabilityOfTimeseries() = default;

        double operator()(epoch_frame::Series const &returns) const {
            // If fewer than 2 data points, return NaN
            if (returns.size() < 2) {
                return NAN_SCALAR;
            }

            // Convert to array, drop NaNs, etc. We assume epoch_frame::Series can do that:
            epoch_frame::Series cleaned = returns.drop_null();
            if (cleaned.empty()) {
                return NAN_SCALAR;
            }

            // cum_log_returns = log1p(returns).cumsum()
            epoch_frame::Series cumLogReturns = cleaned.log1p().cumulative_sum();

            auto rangeIndex = epoch_frame::factory::index::from_range(static_cast<int64_t>(cumLogReturns.size()));
            auto rHat = RValue(rangeIndex->array(), cumLogReturns.contiguous_array());
            return std::pow(rHat, 2);
        }
    };

} // namespace epoch_folio::ep
