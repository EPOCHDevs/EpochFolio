#pragma once

#include "stats.h"
#include <cmath>

namespace epoch_folio::ep {

/**
 * \class Kurtosis
 * \brief Computes the kurtosis (Fisher or Pearson), with optional bias correction.
 *
 * If fisher=true => normal => 0.0
 * If fisher=false => normal => 3.0
 */
    class Kurtosis {
    public:
        /**
         * \param fisher If true, subtract 3 => normal => 0.0
         * \param bias Whether to apply the bias correction
         */
        Kurtosis(bool fisher = true, bool bias = true)
                : m_fisher(fisher), m_bias(bias) {}

        double operator()(epoch_frame::Series const &data) const {
            // Drop NA

            if (data.count_null().as_int64() > 0)
            {
                return NAN_SCALAR;
            }

            size_t n = data.size();
            if (n < 2) {
                return NAN_SCALAR;
            }

            // 1) mean
            double avg = data.mean().as_double();

            // 2) m2, m4
            double m2 = Moment(data, 2, avg);
            double m4 = Moment(data, 4, avg);

            if (std::fabs(m2) < 1e-30) {
                return NAN_SCALAR; // no variance => kurt undefined
            }

            // raw kurt = m4 / (m2^2)
            double kurt = m4 / (m2 * m2);

            // if bias == false => apply correction factor
            // replicate the Python logic:
            // nval = 1/(n-2)/(n-3)*((n^2 - 1)*kurt - 3*(n-1)^2) + 3
            // then if fisher => subtract 3
            if (!m_bias && n > 3) {
                double n_d = static_cast<double>(n);
                double nval = ((n_d*n_d - 1.0)*kurt - 3.0*(n_d-1.0)*(n_d-1.0))
                              / ((n_d - 2.0)*(n_d - 3.0));
                kurt = nval + 3.0;  // corrected Pearson kurtosis
            }

            if (m_fisher) {
                // subtract 3 => normal => 0
                kurt -= 3.0;
            }

            return kurt;
        }

    private:
        bool m_fisher;
        bool m_bias;
    };

} // namespace epoch_folio::ep
