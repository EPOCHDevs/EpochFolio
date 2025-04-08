#pragma once

#include "stats.h"      // For NAN_SCALAR
#include <cmath>

namespace epoch_folio::ep {

/**
 * \class Skew
 * \brief Computes the sample skewness (Fisher-Pearson coefficient).
 *
 * If bias=false, uses the adjusted Fisher-Pearson standardized moment coefficient.
 */
    class Skew {
    public:
        /**
         * \param bias Whether to apply bias correction (default: true).
         */
        explicit Skew(bool bias = true)
                : m_bias(bias) {}

        /**
         * \brief Compute skew on the given returns (1D).
         * \param data A 1-D epoch_frame::Series
         * \return The skew value, or NAN if invalid
         */
        double operator()(epoch_frame::Series const &data) const {
            // Drop NA
            if (data.count_null() > epoch_frame::Scalar{0})
            {
                return NAN_SCALAR;
            }

            size_t n = data.size();
            if (n < 2) {
                return NAN_SCALAR;
            }

            // 1) Compute mean
            double mean = data.mean().as_double();

            // 2) m2, m3
            double m2 = Moment(data, 2, mean);
            double m3 = Moment(data, 3, mean);

            auto zero = m2 <= std::pow(EPSILON_SCALAR * mean, 2);

            // raw skew = m3 / (m2^(3/2))
            double vals = zero ? NAN_SCALAR : m3 / std::pow(m2, 1.5);

            if (!m_bias) {
                bool canCorrect = (!zero) & (n > 2);
                if (canCorrect) {
                    double factor = std::sqrt((double) n * (n - 1.0)) / (n - 2.0);
                    vals *= factor;
                }
            }
            return vals;
        }

    private:
        bool m_bias;
    };

} // namespace epoch_folio::ep
