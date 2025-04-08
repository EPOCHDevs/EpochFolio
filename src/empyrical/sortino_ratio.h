#pragma once

#include "stats.h"        // For NAN_SCALAR, annualizationFactor, etc.
#include "periods.h"
#include <cmath>
#include "down_side_risk.h"


namespace epoch_folio::ep {

/**
 * \class SortinoRatio
 * \brief Computes the Sortino ratio of a strategy.
 *
 * Sortino ratio = (Annualized average return above required_return) / (Annualized downside risk).
 */
    class SortinoRatio {
    public:
        /**
         * \brief Constructor
         *
         * \param requiredReturn Minimum acceptable return (annual).
         * \param period For the periodicity of the data (daily, monthly, etc.).
         * \param annualization Optional override for the annualization factor.
         */
        SortinoRatio(SeriesOrScalar const& requiredReturn = epoch_frame::Scalar{0.0},
                     epoch_core::EmpyricalPeriods period = epoch_core::EmpyricalPeriods::daily,
                     std::optional<int> annualization = std::nullopt,
                     std::optional<double> risk=std::nullopt)
                : m_risk(requiredReturn, period, annualization),
                  m_requiredReturn(requiredReturn),
                  m_period(period),
                  m_annualization(annualization),
                  m_riskValue(risk){}

        /**
         * \brief Compute the Sortino ratio for the given returns
         *
         * \param returns A epoch_frame::Series of daily (or periodic) returns.
         * \return Sortino ratio as a double (or NAN if invalid).
         */
        double operator()(epoch_frame::Series const &returns) const {
            if (returns.size() < 2) {
                return NAN_SCALAR;
            }

            // Annualize factor
            int annFactor = AnnualizationFactor(m_period, m_annualization);

            // 1) Adjust returns by subtracting the requiredReturn
            //    If your library doesn’t handle the array-broadcast, you might do something else.
            //    Otherwise, we assume: adjusted = returns - requiredReturn / (periodic scale).
            //    For simplicity, we treat it as a direct shift each period (like the Python code).
            epoch_frame::Series adjusted = AdjustReturns(returns, m_requiredReturn);

            // 2) Annualized average return above threshold
            double avgReturn = adjusted.mean().as_double() * annFactor;

            // 3) Annualized downside risk
            double drisk = m_riskValue.value_or(m_risk(returns));
            return avgReturn / drisk;
        }

    private:
        // We'll use a helper class or function for downside risk (shown below).
        DownsideRisk m_risk;

        SeriesOrScalar m_requiredReturn;
        epoch_core::EmpyricalPeriods m_period;
        std::optional<int> m_annualization;
        std::optional<double> m_riskValue;
    };

} // namespace epoch_folio::ep
