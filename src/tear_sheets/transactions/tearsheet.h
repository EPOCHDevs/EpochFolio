//
// Created by adesola on 3/30/25.
//

#pragma once
#include "common/chart_def.h"
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"
#include <epoch_protos/tearsheet.pb.h>

namespace epoch_folio::txn {
class TearSheetFactory {

public:
  TearSheetFactory(epoch_frame::Series returns,
                   epoch_frame::DataFrame positions,
                   epoch_frame::DataFrame transactions);

  void Make(epoch_core::TurnoverDenominator turnoverDenominator, size_t binSize,
            std::string const &timezone,
            epoch_proto::FullTearSheet &output) const;

private:
  epoch_frame::Series m_returns;
  epoch_frame::DataFrame m_positions;
  epoch_frame::DataFrame m_transactions;

  epoch_proto::LinesDef
  MakeTurnoverOverTimeChart(epoch_frame::Series const &dfTurnover) const;
  epoch_proto::LinesDef MakeDailyVolumeChart() const;
  epoch_proto::HistogramDef
  MakeDailyTurnoverHistogram(epoch_frame::Series const &dfTurnover) const;
  BarDef MakeTransactionTimeHistogram(size_t binSize,
                                      std::string const &timezone) const;
};
} // namespace epoch_folio::txn