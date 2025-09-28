//
// Created by adesola on 1/13/25.
//

#pragma once
#include "epoch_dashboard/tearsheet/dashboard_builders.h"
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"
#include <epoch_protos/tearsheet.pb.h>

namespace epoch_folio::positions {
class TearSheetFactory {
public:
  TearSheetFactory(epoch_frame::Series cash, epoch_frame::DataFrame positions,
                   epoch_frame::Series returns,
                   std::unordered_map<std::string, std::string> sectorMappings);

  void Make(uint32_t k, epoch_tearsheet::DashboardBuilder &output) const;

protected:
  TearSheetFactory() = default;

  void SetStrategyReturns(epoch_frame::Series const &strategyReturns) {
    m_strategy = strategyReturns;
  }

  void SetCash(epoch_frame::Series const &cash) { m_cash = cash; }

  void SetPositions(epoch_frame::DataFrame positions) {
    m_positionsNoCash = std::move(positions);
  }

  std::vector<epoch_proto::Chart> MakeTopPositionsLineCharts(
      epoch_frame::DataFrame const &positions,
      epoch_frame::DataFrame const &topPositionAllocations) const;

private:
  epoch_frame::Series m_cash;
  epoch_frame::DataFrame m_positionsNoCash;
  epoch_frame::Series m_strategy;
  std::unordered_map<std::string, std::string> m_sectorMappings;

  epoch_proto::Chart
  MakeExposureOverTimeChart(epoch_frame::DataFrame const &positions,
                            epoch_frame::DataFrame const &isLong,
                            epoch_frame::DataFrame const &isShort) const;
  epoch_proto::Chart MakeAllocationOverTimeChart(
      epoch_frame::DataFrame const &topPositionAllocations) const;
  epoch_proto::Chart
  MakeAllocationSummaryChart(epoch_frame::DataFrame const &positions) const;
  epoch_proto::Chart
  MakeTotalHoldingsChart(epoch_frame::DataFrame const &positions) const;
  epoch_proto::Chart
  MakeLongShortHoldingsChart(epoch_frame::DataFrame const &isLong,
                             epoch_frame::DataFrame const &isShort) const;
  epoch_proto::Chart MakeGrossLeverageChart() const;
  epoch_proto::Chart MakeSectorExposureChart() const;
};
} // namespace epoch_folio::positions