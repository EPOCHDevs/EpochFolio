#pragma once

#include "stats.h"     // For NAN_SCALAR, etc.
#include <cmath>       // For std::fabs, etc.

namespace epoch_folio::ep {

/**
 * \class ValueAtRisk
 * \brief Computes the Value at Risk (VaR) of a returns stream at a specified cutoff.
 *
 * Value at Risk at cutoff X% is essentially the X-th percentile of the distribution
 * of returns.
 */
    class ValueAtRisk {
    public:
        /**
         * \param cutoff A decimal representing the percentage cutoff (e.g. 0.05 for 5%).
         */
        explicit ValueAtRisk(double cutoff = 0.05)
                : m_cutoff(cutoff) {}

        /**
         * \brief Compute VaR on the given returns
         * \param returns A epoch_frame::Series of daily returns (1-D)
         * \return The VaR value, or NAN_SCALAR if invalid.
         */
        double operator()(epoch_frame::Series const &returns) const {
            return returns.quantile(arrow::compute::QuantileOptions{m_cutoff}).as_double();
        }

    private:
        double m_cutoff;
    };

    /**
 * \class ConditionalValueAtRisk
 * \brief Computes the Conditional Value at Risk (CVaR), also known as Expected Shortfall.
 *
 * CVaR measures the mean loss (or return) for the worst-performing X% of days.
 */
    class ConditionalValueAtRisk {
    public:
        /**
         * \param cutoff Decimal representing the bottom percentile cutoff (e.g. 0.05 for worst 5%).
         */
        explicit ConditionalValueAtRisk(double cutoff = 0.05)
                : m_cutoff(cutoff) {}

        /**
         * \brief Compute CVaR on the given returns
         * \param returns A epoch_frame::Series of daily returns (1-D)
         * \return The CVaR value, or NAN_SCALAR if invalid.
         */
        double operator()(epoch_frame::Series const &returns) const {
            const auto n = static_cast<double >(returns.size());
            const auto cutoffIndex = static_cast<int64_t>((n - 1.0) * m_cutoff);

            auto rVector = returns.contiguous_array();
            auto nthSortedIndices = epoch_frame::Array{epoch_frame::AssertResultIsOk(arrow::compute::NthToIndices(*rVector, cutoffIndex))};
            auto nthSorted = rVector.take(nthSortedIndices);
            return nthSorted[{0, cutoffIndex+1}].mean().value<double>().value_or(NAN_SCALAR);
        }

    private:
        double m_cutoff;
    };

    class PyFolioValueAtRisk {
    public:
        explicit PyFolioValueAtRisk(double sigma=2.0):m_sigma(std::move(sigma)){}

        /**
         * \brief Compute VaR on the given returns
         * \param returns A epoch_frame::Series of daily returns (1-D)
         * \return The VaR value, or NAN_SCALAR if invalid.
         */
        double operator()(epoch_frame::Series const &returns) const {
            return (returns.mean() - m_sigma*returns.stddev(arrow::compute::VarianceOptions{1}) ).as_double();
        }
    private:
        epoch_frame::Scalar m_sigma;
    };
} // namespace epoch_folio::ep
