#pragma once

#include "stats.h"
#include <cmath>
#include <algorithm>

namespace epoch_folio::ep {

/**
 * \class TailRatio
 * \brief Determines the ratio between the 95th percentile (right tail)
 *        and the 5th percentile (left tail) of returns.
 *
 * For example, a ratio of 0.25 means losses are four times as bad as profits.
 */
    class TailRatio {
    public:
        TailRatio() = default;

        double operator()(epoch_frame::Series const &returns) const {
            if (returns.empty()) {
                return NAN_SCALAR;
            }

            // Drop NaNs
            epoch_frame::Series cleaned = returns.drop_null();
            if (cleaned.empty()) {
                return NAN_SCALAR;
            }

            // percentile(95) and percentile(5)
            auto p95 = cleaned.quantile(arrow::compute::QuantileOptions{0.95}).as_double();
            auto p05 = cleaned.quantile(arrow::compute::QuantileOptions{0.05}).as_double();

            return std::abs(p95) / std::abs(p05);
        }
    };

} // namespace epoch_folio::ep
