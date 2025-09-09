//
// Created by adesola on 3/28/25.
//
#include "epoch_folio/metadata.h"

namespace epoch_folio {
std::vector<CategoryMetaData> GetCategoryMetaData() {
  return {
      CategoryMetaData{
          "StrategyBenchmark", "Strategy Benchmark",
          "Comparison between strategy performance and benchmark metrics"},
      CategoryMetaData{"RiskAnalysis", "Risk Analysis",
                       "Analysis of portfolio risk metrics including drawdowns "
                       "and volatility"},
      CategoryMetaData{
          "ReturnsDistribution", "Returns Distribution",
          "Statistical distribution of returns across different time periods"},
      CategoryMetaData{
          "Positions", "Positions",
          "Current and historical portfolio positions and allocations"},
      CategoryMetaData{
          "Transactions", "Transactions",
          "Record of all buy and sell transactions in the portfolio"},
      CategoryMetaData{
          "RoundTrip", "Round Trip",
          "Analysis of complete buy-to-sell cycles for individual positions"}};
}
} // namespace epoch_folio