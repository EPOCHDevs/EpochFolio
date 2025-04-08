//
// Created by adesola on 1/6/25.
//

#pragma once
#include "ireturn_stat.h"
#include "periods.h"
#include "stats.h"


namespace epoch_folio::ep {
    class AnnualVolatility {
    public:
        AnnualVolatility(epoch_core::EmpyricalPeriods period = epoch_core::EmpyricalPeriods::daily,
                         double alpha = 2.0,
                         std::optional<int> annualization = std::nullopt) :
                m_period(period), m_alpha(alpha), m_annualization(annualization) {}

        double operator()(epoch_frame::Series const &returns) const {
            if (returns.size() < 2) return NAN_SCALAR;

            auto annFactor = AnnualizationFactor(m_period, m_annualization);
            return returns.stddev(arrow::compute::VarianceOptions{1}).as_double() * std::pow(annFactor, 1.0 / m_alpha);
        }

    private:
        epoch_core::EmpyricalPeriods m_period;
        double m_alpha;
        std::optional<int> m_annualization;
    };

    using RollAnnualVolatility = RollingReturnsStat<AnnualVolatility, epoch_core::EmpyricalPeriods, double, std::optional<int>>;
}