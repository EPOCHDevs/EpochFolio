#pragma once

#include "stats.h"
#include <cmath>

namespace epoch_folio::ep {

/**
 * \class ExcessSharpe
 * \brief Computes the Excess Sharpe of a strategy against a factor or benchmark.
 *
 * Excess Sharpe = mean(active_return) / std(active_return)
 * where active_return = returns - factor_returns
 */
    class ExcessSharpe {
    public:
        ExcessSharpe() = default; // No specific constructor needed here

        /**
         * \brief Compute the Excess Sharpe
         *
         * \param returns A epoch_frame::Series of the strategy’s returns
         * \param factorReturns A epoch_frame::Series of the factor’s returns
         * \return Excess Sharpe as a double
         */
        double operator()(epoch_frame::Series const &returns, epoch_frame::Series const &factorReturns) const {
            // Align or ensure same length if needed.
            // We assume they match or your library can handle an automatic alignment.

            if (returns.size() < 2) {
                return NAN_SCALAR;
            }

            // active_return = returns - factorReturns
            epoch_frame::Series activeReturn = returns - factorReturns;

            double meanActive = activeReturn.mean().as_double();
            double stdActive = activeReturn.stddev(arrow::compute::VarianceOptions{1}).as_double();

            if (std::fabs(stdActive) < EPSILON_SCALAR) {
                return NAN_SCALAR;
            }

            return meanActive / stdActive;
        }
    };

} // namespace epoch_folio::ep
