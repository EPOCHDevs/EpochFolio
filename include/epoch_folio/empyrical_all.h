//
// Created by adesola on 1/6/25.
//

#pragma once
#include "epoch_folio/aliases.h"


namespace epoch_folio::ep {
    double CommonSenseRatio(epoch_frame::Series const &returns);

    enum class SimpleStat {
        CumReturn,
        AnnualReturn,
        AnnualVolatility,
        SharpeRatio,
        CalmarRatio,
        StabilityOfTimeSeries,
        MaxDrawDown,
        OmegaRatio,
        SortinoRatio,
        Skew,
        Kurtosis,
        TailRatio,
        CAGR,
        ValueAtRisk,
        ConditionalValueAtRisk,
        CommonSenseRatio,
    };

    enum class FactorStat {
        Alpha,
        Beta
    };

    std::unordered_map<SimpleStat, ReturnsStat> get_simple_stats();
    std::unordered_map<FactorStat, FactorReturnsStat> get_factor_stats();
    std::string get_stat_name(SimpleStat const&);
    std::string get_stat_name(FactorStat const&);

}