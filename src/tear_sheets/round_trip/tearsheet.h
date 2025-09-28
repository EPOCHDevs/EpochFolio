//
// Created by adesola on 3/30/25.
//

#pragma once
#include "epoch_dashboard/tearsheet/dashboard_builders.h"
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"
#include <epoch_protos/tearsheet.pb.h>

namespace epoch_folio::round_trip {
class TearSheetFactory {
public:
  TearSheetFactory(epoch_frame::DataFrame round_trip,
                   epoch_frame::Series returns,
                   epoch_frame::DataFrame positions,
                   SectorMapping sector_mapping);

  void Make(epoch_tearsheet::DashboardBuilder &output) const;

private:
  epoch_frame::DataFrame m_round_trip;
  epoch_frame::Series m_returns;
  epoch_frame::DataFrame m_positions;
  SectorMapping m_sector_mapping;

  epoch_frame::DataFrame ExtractRoundTrips() const;

  epoch_proto::Chart MakeXRangeDef(epoch_frame::DataFrame const &trades) const;

  epoch_proto::Chart
  MakeProbProfitChart(epoch_frame::DataFrame const &trades) const;

  epoch_proto::Chart
  MakeHoldingTimeChart(epoch_frame::DataFrame const &trades) const;

  epoch_proto::Chart
  MakePnlPerRoundTripDollarsChart(epoch_frame::DataFrame const &trades) const;

  epoch_proto::Chart MakeReturnsPerRoundTripDollarsChart(
      epoch_frame::DataFrame const &trades) const;

  epoch_proto::Chart
  MakeProfitabilityPieChart(epoch_frame::DataFrame const &trades) const;
};
} // namespace epoch_folio::round_trip