//
// Created by adesola on 1/6/25.
//

#pragma once
#include "ireturn_stat.h"
#include "periods.h"
#include "annual_returns.h"
#include "max_drawdown.h"


namespace epoch_folio::ep {
    class CalmarRatio {
    public:
        CalmarRatio(epoch_core::EmpyricalPeriods period = epoch_core::EmpyricalPeriods::daily,
                    std::optional<int> annualization = std::nullopt) : m_annualReturns(period, annualization){}

        double operator()(epoch_frame::Series const &returns) const {
            auto maxDD = m_maxDrawDown(returns);
            double temp{};

            if (maxDD < 0) {
                temp = m_annualReturns(returns) / std::abs(maxDD);
            } else {
                return NAN_SCALAR;
            }

            return std::isinf(temp) ? NAN_SCALAR : temp;
        }

    private:
        AnnualReturns m_annualReturns;
        MaxDrawDown m_maxDrawDown;
    };

    using CAGR = AnnualReturns;
    using RollCAGR = RollingReturnsStat<CAGR, epoch_core::EmpyricalPeriods, std::optional<int>>;

}