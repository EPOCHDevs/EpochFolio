#pragma once

#include "ireturn_stat.h"
#include "stats.h"         // For NAN_SCALAR, annualizationFactor, etc.
#include "periods.h"
#include <cmath>

namespace epoch_folio::ep {

/**
 * \class DownsideRisk
 * \brief Computes the annualized downside risk below a required return.
 */
    class DownsideRisk {
    public:
        DownsideRisk(std::variant<epoch_frame::Scalar, epoch_frame::Series> requiredReturn = epoch_frame::Scalar{0.0},
                     epoch_core::EmpyricalPeriods period = epoch_core::EmpyricalPeriods::daily,
                     std::optional<int> annualization = std::nullopt)
                : m_requiredReturn(requiredReturn),
                  m_period(period),
                  m_annualization(annualization) {}

        double operator()(epoch_frame::Series const &returns) const {
            if (returns.empty()) {
                return NAN_SCALAR;
            }

            int annFactor = AnnualizationFactor(m_period, m_annualization);

            // Clip any returns above zero to zero, focusing on negative part
            // The python: downside_diff = np.clip(returns - requiredReturn, -inf, 0)
            epoch_frame::Series diff = Clip(AdjustReturns(returns, m_requiredReturn), -INF_SCALAR, 0.0);

            // Square the negative differences
            epoch_frame::Series squared = diff.power(epoch_frame::Scalar{2});

            // Mean and sqrt
            double meanSq = squared.mean().as_double();
            double stdev = std::sqrt(meanSq);

            // Annualize
            stdev *= std::sqrt(static_cast<double>(annFactor));
            return stdev;
        }

    private:
        std::variant<epoch_frame::Scalar, epoch_frame::Series> m_requiredReturn;
        epoch_core::EmpyricalPeriods m_period;
        std::optional<int> m_annualization;
    };

} // namespace epoch_folio::ep
