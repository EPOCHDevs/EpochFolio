#pragma once

#include "ireturn_stat.h"
#include "stats.h"         // for NAN_SCALAR, annualizationFactor, etc.
#include "periods.h"       // for EmpyricalPeriods
#include <cmath>           // std::sqrt
#include <limits>
#include <utility>

namespace epoch_folio::ep {

/**
 * \class SharpeRatio
 * \brief Computes the Sharpe ratio of a strategy.
 *
 * Sharpe ratio = (mean of excess returns / std of excess returns) * sqrt(annual factor)
 */
    class SharpeRatio {
    public:
        /**
         * \brief Constructor
         *
         * \param riskFree Constant risk-free return.
         * \param period  For the periodicity of the data (daily, monthly, etc.).
         * \param annualization Optional override for the annualization factor.
         */
        SharpeRatio(std::variant<epoch_frame::Scalar, epoch_frame::Series> riskFree = epoch_frame::Scalar{0.0},
                    epoch_core::EmpyricalPeriods period = epoch_core::EmpyricalPeriods::daily,
                    std::optional<int> annualization = std::nullopt)
                : m_riskFree(std::move(riskFree)),
                  m_period(period),
                  m_annualization(annualization) {}

        /**
         * \brief Compute the Sharpe ratio for the given returns
         *
         * \param returns A epoch_frame::Series of daily (or periodic) returns.
         * \return Sharpe ratio as a double (or NAN if invalid).
         */
        double operator()(epoch_frame::Series const &returns) const {
            // If fewer than 2 data points, return NaN to match Python
            if (returns.size() < 2) {
                return NAN_SCALAR;
            }

            // Adjust returns by subtracting the risk-free rate
            epoch_frame::Series excessReturns = AdjustReturns(returns, m_riskFree);

            // Annualization factor
            int annFactor = AnnualizationFactor(m_period, m_annualization);

            // Mean of excess returns
            double meanExcess = excessReturns.mean().as_double();

            // Std dev of excess returns (sample std)
            double stdExcess = excessReturns.stddev(arrow::compute::VarianceOptions{1}).as_double();

            // Sharpe ratio = (mean / std) * sqrt(annual factor)
            return (meanExcess / stdExcess) * std::sqrt(static_cast<double>(annFactor));
        }

    private:
        std::variant<epoch_frame::Scalar, epoch_frame::Series> m_riskFree;
        epoch_core::EmpyricalPeriods m_period;
        std::optional<int> m_annualization;
    };
    using RollSharpeRatio = RollingReturnsStat<SharpeRatio, epoch_frame::Scalar, epoch_core::EmpyricalPeriods, std::optional<int>>;

} // namespace epoch_folio::ep
