//
// Created by adesola on 3/28/25.
//
#include "epoch_folio/metadata.h"


using namespace epoch_core;
namespace epoch_metadata::epoch_folio {
    std::vector<CategoryMetaData> GetCategoryMetaData() {
        return {
            CategoryMetaData{EpochFolioCategory::StrategyBenchmark, "Strategy Benchmark", "Comparison between strategy performance and benchmark metrics"},
            CategoryMetaData{EpochFolioCategory::RiskAnalysis, "Risk Analysis", "Analysis of portfolio risk metrics including drawdowns and volatility"},
            CategoryMetaData{EpochFolioCategory::ReturnsDistribution, "Returns Distribution", "Statistical distribution of returns across different time periods"},
            CategoryMetaData{EpochFolioCategory::Positions, "Positions", "Current and historical portfolio positions and allocations"},
            CategoryMetaData{EpochFolioCategory::Transactions, "Transactions", "Record of all buy and sell transactions in the portfolio"},
            CategoryMetaData{EpochFolioCategory::RoundTrip, "Round Trip", "Analysis of complete buy-to-sell cycles for individual positions"}
        };
      }
}