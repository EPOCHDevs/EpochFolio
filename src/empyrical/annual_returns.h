//
// Created by adesola on 1/6/25.
//

#pragma once
#include "ireturn_stat.h"
#include "periods.h"
#include "stats.h"


namespace epoch_folio::ep {
    class AnnualReturns {
    public:
        explicit AnnualReturns(epoch_core::EmpyricalPeriods period=epoch_core::EmpyricalPeriods::daily,
                      std::optional<int> annualization=std::nullopt) :
                      m_period(period),
                      m_annualization(annualization) {}

        double operator()(epoch_frame::Series const &returns) const {
            if (returns.empty()) return NAN_SCALAR;

            double annFactor = AnnualizationFactor(m_period, m_annualization);
            double numYears = static_cast<double>(returns.size()) / annFactor;
            auto endingValue = CumReturnsFinal(returns, 1);

            return std::pow(endingValue, (1.0 / numYears)) - 1;
        }

    private:
        epoch_core::EmpyricalPeriods m_period;
        std::optional<int> m_annualization;
    };

    using CAGR = AnnualReturns;
    using RollCAGR = RollingReturnsStat<CAGR, epoch_core::EmpyricalPeriods, std::optional<int>>;

}