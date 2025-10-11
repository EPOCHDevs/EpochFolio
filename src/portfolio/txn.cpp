//
// Created by adesola on 1/8/25.
//

#include "txn.h"

namespace epoch_folio {
epoch_frame::Series
GetTurnover(epoch_frame::DataFrame const &positions,
            epoch_frame::DataFrame const &transactions,
            epoch_core::TurnoverDenominator turnoverDenominator) {
  epoch_frame::Series tradedValue =
      GetTransactionVolume(transactions)["txn_volume"];

  epoch_frame::Series denom{EMPTY_SERIES};
  if (turnoverDenominator == epoch_core::TurnoverDenominator::AGB) {
    auto abg = ABG(positions);
    denom = abg.rolling_agg({.window_size = 2}).mean();
    denom = denom.assign(abg.iloc({0, 1}) / epoch_frame::Scalar{2.0});
  } else {
    denom = positions.sum(epoch_frame::AxisType::Column);
  }

  try {
    denom = denom.set_index(denom.index()->normalize());
  } catch (std::exception const &e) {
    SPDLOG_WARN("Failed to set normalized index: {}", e.what());
  }
  auto turnover = tradedValue / denom;
  return turnover.fillnull(epoch_frame::Scalar{0.0});
}

epoch_frame::DataFrame GetTransactionVolume(epoch_frame::DataFrame const &df) {
  epoch_frame::Series amounts = df["amount"].abs();
  epoch_frame::Series values =
      df.contains("txn_volume") ? df["txn_volume"] : amounts * df["price"];

  auto table = epoch_frame::concat(
      {.frames = {epoch_frame::FrameOrSeries{amounts.to_frame("txn_shares")},
                  epoch_frame::FrameOrSeries{values.to_frame("txn_volume")}},
       .axis = epoch_frame::AxisType::Column});

  auto normalized_index = df.index()->normalize();
  return table.set_index(normalized_index).group_by_agg(normalized_index->as_chunked_array()).sum();
}
} // namespace epoch_folio