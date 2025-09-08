//
// Created by adesola on 3/28/25.
//
#include "epoch_folio/metadata.h"

namespace epoch_folio {
std::vector<CategoryMetaData> GetCategoryMetaData() {
  return {
      CategoryMetaData{
          epoch_proto::EpochFolioCategory::
              EPOCH_FOLIO_CATEGORY_STRATEGY_BENCHMARK,
          "Strategy Benchmark",
          "Comparison between strategy performance and benchmark metrics"},
      CategoryMetaData{epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS,
                       "Risk Analysis",
                       "Analysis of portfolio risk metrics including drawdowns "
                       "and volatility"},
      CategoryMetaData{
          epoch_proto::EPOCH_FOLIO_CATEGORY_RETURNS_DISTRIBUTION,
          "Returns Distribution",
          "Statistical distribution of returns across different time periods"},
      CategoryMetaData{
          epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS, "Positions",
          "Current and historical portfolio positions and allocations"},
      CategoryMetaData{
          epoch_proto::EPOCH_FOLIO_CATEGORY_TRANSACTIONS, "Transactions",
          "Record of all buy and sell transactions in the portfolio"},
      CategoryMetaData{
          epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP, "Round Trip",
          "Analysis of complete buy-to-sell cycles for individual positions"}};
}
} // namespace epoch_folio