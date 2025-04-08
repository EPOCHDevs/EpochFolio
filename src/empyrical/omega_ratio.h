#pragma once

#include "ireturn_stat.h"
#include "periods.h"
#include "stats.h"        // for NAN_SCALAR, etc.
#include <cmath>          // for std::pow, std::fabs
#include <limits>         // for std::numeric_limits

namespace epoch_folio::ep {

/**
 * \class OmegaRatio
 * \brief Determines the Omega ratio of a strategy.
 *
 * The Omega Ratio is defined as:
 *
 *    Omega = ( Sum of returns above threshold ) / ( Absolute sum of returns below threshold )
 *
 * The threshold is adjusted for the risk-free rate and the required return,
 * optionally annualized. 
 *
 * This class can be used directly:
 *
 *     OmegaRatio omega(riskFree, requiredReturn, annualization);
 *     double result = omega(returns);
 *
 * \note Equivalent to the Python function:
 * \code
 * def omega_ratio(returns, risk_free=0.0, required_return=0.0, annualization=252):
 *     # ...
 * \endcode
 */
    class OmegaRatio {
    public:
        /**
         * \brief Constructor.
         *
         * \param riskFree A constant risk-free return throughout the period.
         * \param requiredReturn Minimum acceptance return of the investor (annual).
         * \param annualization Number of periods per year (e.g. 252 for daily data).
         */
        OmegaRatio(double riskFree = 0.0,
                   double requiredReturn = 0.0,
                   double annualization = APPROX_BDAYS_PER_YEAR)
                : m_riskFree(std::move(riskFree)),
                  m_requiredReturn(std::move(requiredReturn)),
                  m_annualization(std::move(annualization)) {}

        /**
         * \brief Compute the Omega Ratio for the given \p returns.
         *
         * \param returns A epoch_frame::Series of noncumulative returns.
         * \return The Omega Ratio as a double, or NAN_SCALAR if it cannot be computed.
         */
        double operator()(epoch_frame::Series const &returns) const {
            using namespace epoch_frame;
            // If there are fewer than 2 data points, Python version returns NaN.
            if (returns.size() < 2) {
                return NAN_SCALAR;
            }

            // Compute the per-period threshold.
            // If annualization == 1, no compounding; otherwise, convert requiredReturn to a single-period threshold.
            Scalar returnThreshold;
            auto requiredReturn = m_requiredReturn.as_double();
            if (std::fabs(m_annualization - 1.0) < 1e-12) {
                returnThreshold = m_requiredReturn;
            } else if (requiredReturn <= -1.0) {
                // If required_return <= -1, early exit with NaN (same as Python).
                return NAN_SCALAR;
            } else {
                // (1 + required_return)^(1/annualization) - 1
                returnThreshold = epoch_frame::Scalar{std::pow(1.0 + requiredReturn, 1.0 / m_annualization) - 1.0};
            }

            // returns_less_thresh = returns - risk_free - returnThreshold
            // We'll do this by creating a temporary copy and subtracting values.
            const auto returnsLessThreshold = returns - m_riskFree - returnThreshold;

            // Calculate numerator = sum of positive values
            //         denominator = absolute sum of negative values (multiplied by -1)
            epoch_frame::Scalar zero{0.0};
            Scalar numer = returnsLessThreshold.loc(returnsLessThreshold > zero).sum();
            Scalar denom = -1.0_scalar * returnsLessThreshold.loc(returnsLessThreshold < zero).sum();

            if (denom > zero) {
                return (numer / denom).value<double>().value_or(NAN_SCALAR);
            }
            return NAN_SCALAR; // If there are no negatives, ratio is undefined.
        }

    private:
        epoch_frame::Scalar m_riskFree;
        epoch_frame::Scalar m_requiredReturn;
        double m_annualization;
    };

// If you want a rolling version of the Omega Ratio (similar to roll_max_drawdown, etc.),
// you can define a RollingReturnsStat for it:
    using RollOmegaRatio = RollingReturnsStat<OmegaRatio, double, double, double>;

} // namespace epoch_folio::ep
