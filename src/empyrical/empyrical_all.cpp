//
// Created by adesola on 3/27/25.
//
#include "epoch_folio/empyrical_all.h"
#include "alpha_beta.h"
#include "annual_returns.h"
#include "annual_volatility.h"
#include "calmar_ratio.h"
#include "down_side_risk.h"
#include "excess_sharpe.h"
#include "kurtosis.h"
#include "max_drawdown.h"
#include "omega_ratio.h"
#include "sharpe_ratio.h"
#include "skew.h"
#include "sortino_ratio.h"
#include "stability_of_timeseries.h"
#include "tail_ratio.h"
#include "var.h"


namespace epoch_folio::ep {
     double CommonSenseRatio(epoch_frame::Series const &returns) {
        return ep::TailRatio{}(returns) * (1.0 + ep::AnnualReturns{}(returns));
    }

    std::unordered_map<SimpleStat, ReturnsStat> get_simple_stats() {
        static const std::unordered_map<SimpleStat, ReturnsStat> SIMPLE_STAT_FUNCS{
                        {SimpleStat::AnnualReturn,          AnnualReturns{}},
                        {SimpleStat::CumReturn,             [](epoch_frame::Series const &r) {
                            return CumReturnsFinal(r, 0);
                        }},
                        {SimpleStat::AnnualVolatility,      AnnualVolatility{}},
                        {SimpleStat::SharpeRatio,           SharpeRatio{}},
                        {SimpleStat::CalmarRatio,           CalmarRatio{}},
                        {SimpleStat::StabilityOfTimeSeries, StabilityOfTimeseries{}},
                        {SimpleStat::MaxDrawDown,           MaxDrawDown{}},
                        {SimpleStat::OmegaRatio,            OmegaRatio{}},
                        {SimpleStat::SortinoRatio,          SortinoRatio{}},
                        {SimpleStat::Skew,                  Skew{}},
                        {SimpleStat::Kurtosis,              Kurtosis{}},
                        {SimpleStat::TailRatio,             TailRatio{}},
                        {SimpleStat::CommonSenseRatio,      CommonSenseRatio},
                        {SimpleStat::ValueAtRisk,           PyFolioValueAtRisk{}}
        };
        return SIMPLE_STAT_FUNCS;
    }

    std::unordered_map<FactorStat, FactorReturnsStat> get_factor_stats() {
        static const std::unordered_map<FactorStat, FactorReturnsStat> FACTOR_STAT_FUNCS{
                        {FactorStat::Alpha, Alpha{}},
                        {FactorStat::Beta,  Beta{}}
        };
        return FACTOR_STAT_FUNCS;
    }

    std::unordered_map<std::variant<SimpleStat, FactorStat>, std::string> get_stat_names() {
         static const std::unordered_map<std::variant<SimpleStat, FactorStat>, std::string> STAT_FUNC_NAMES{
                    {SimpleStat::AnnualReturn,          "Annual Return"},
                    {SimpleStat::CumReturn,             "Cumulative Returns"},
                    {SimpleStat::AnnualVolatility,      "Annual Volatility"},
                    {SimpleStat::SharpeRatio,           "Sharpe Ratio"},
                    {SimpleStat::CalmarRatio,           "Calmar Ratio"},
                    {SimpleStat::StabilityOfTimeSeries, "Stability"},
                    {SimpleStat::MaxDrawDown,           "Max Drawdown"},
                    {SimpleStat::OmegaRatio,            "Omega Ratio"},
                    {SimpleStat::SortinoRatio,          "Sortino Ratio"},
                    {SimpleStat::Skew,                  "Skew"},
                    {SimpleStat::Kurtosis,              "Kurtosis"},
                    {SimpleStat::TailRatio,             "Tail Ratio"},
                    {SimpleStat::CommonSenseRatio,      "Common sense ratio"},
                    {SimpleStat::ValueAtRisk,           "Daily Value at Risk"},
                    {FactorStat::Alpha,                 "Alpha"},
                    {FactorStat::Beta,                  "Beta"}
         };
         return STAT_FUNC_NAMES;
    }

    std::string get_stat_name(SimpleStat const& name) {
         static const std::unordered_map<SimpleStat, std::string> STAT_FUNC_NAMES{
                        {SimpleStat::AnnualReturn,          "Annual Return"},
                        {SimpleStat::CumReturn,             "Cumulative Returns"},
                        {SimpleStat::AnnualVolatility,      "Annual Volatility"},
                        {SimpleStat::SharpeRatio,           "Sharpe Ratio"},
                        {SimpleStat::CalmarRatio,           "Calmar Ratio"},
                        {SimpleStat::StabilityOfTimeSeries, "Stability"},
                        {SimpleStat::MaxDrawDown,           "Max Drawdown"},
                        {SimpleStat::OmegaRatio,            "Omega Ratio"},
                        {SimpleStat::SortinoRatio,          "Sortino Ratio"},
                        {SimpleStat::Skew,                  "Skew"},
                        {SimpleStat::Kurtosis,              "Kurtosis"},
                        {SimpleStat::TailRatio,             "Tail Ratio"},
                        {SimpleStat::CommonSenseRatio,      "Common sense ratio"},
                        {SimpleStat::ValueAtRisk,           "Daily Value at Risk"},
         };
         return STAT_FUNC_NAMES.at(name);
    }

    std::string get_stat_name(FactorStat const& name) {
         static const std::unordered_map<FactorStat, std::string> STAT_FUNC_NAMES{
                        {FactorStat::Alpha,                 "Alpha"},
                        {FactorStat::Beta,                  "Beta"}
         };
         return STAT_FUNC_NAMES.at(name);
    }
}