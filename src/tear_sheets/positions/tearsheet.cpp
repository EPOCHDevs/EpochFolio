//
// Created by adesola on 1/14/25.
//

#include "tearsheet.h"

#include "portfolio/pos.h"
#include "portfolio/timeseries.h"
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/table_factory.h>

using namespace epoch_frame;
using namespace epoch_core;

namespace epoch_folio::positions {
const Scalar HUNDRED{100.0};
const Scalar ZERO{0.0};

TearSheetFactory::TearSheetFactory(
    epoch_frame::Series cash, epoch_frame::DataFrame positions,
    epoch_frame::Series returns,
    std::unordered_map<std::string, std::string> sectorMappings)
    : m_cash(std::move(cash).rename("cash")),
      m_positionsNoCash(std::move(positions)), m_strategy(std::move(returns)),
      m_sectorMappings(std::move(sectorMappings)) {}

Table MakeTopPositionsTable(std::string const &id, std::string const &name,
                            Series const &x, uint64_t k) {
  k = std::min(k, x.size());
  std::vector rows(2, std::vector<Scalar>(k));

  auto index = x.index();
  for (size_t i = 0; i < k; ++i) {
    rows[0][i] = index->at(i);
    rows[1][i] = x.iloc(i) * HUNDRED;
  }
  auto data = factory::table::make_table(
      rows, {string_field(id), float64_field("max")});
  return {EpochFolioDashboardWidget::DataTable,
          EpochFolioCategory::Positions,
          {ColumnDef{id, name, EpochFolioType::String},
           ColumnDef{"max", "Max", EpochFolioType::Percent}},
          data};
}

std::vector<Table>
MakeTopPositionsTables(std::array<Series, 3> const &topPositions, uint64_t k) {
  return {MakeTopPositionsTable("long", "Top 10 long positions of all time",
                                topPositions[0], k),
          MakeTopPositionsTable("short", "Top 10 short positions of all time",
                                topPositions[1], k),
          MakeTopPositionsTable("abs", "Top 10 positions of all time",
                                topPositions[2], k)};
}

LinesDef TearSheetFactory::MakeExposureOverTimeChart(
    epoch_frame::DataFrame const &positions,
    epoch_frame::DataFrame const &isLong,
    epoch_frame::DataFrame const &isShort) const {
  auto positionSum = positions.sum(AxisType::Column);

  auto longExposure = isLong.sum(AxisType::Column) / positionSum;
  auto shortExposure = isShort.sum(AxisType::Column) / positionSum;
  auto netExposure = m_positionsNoCash.sum(AxisType::Column) / positionSum;

  auto netExposureLine = MakeSeriesLine(netExposure, "Net");
  netExposureLine.dashStyle = "dot";

  return LinesDef{
      .chartDef =
          ChartDef{"exposure", "Exposure", EpochFolioDashboardWidget::Area,
                   EpochFolioCategory::Positions},
      .lines = MakeSeriesLines(longExposure, shortExposure, "Long", "Short"),
      .overlay = netExposureLine};
}

LinesDef TearSheetFactory::MakeAllocationOverTimeChart(
    epoch_frame::DataFrame const &topPositionAllocations) const {
  return LinesDef{
      .chartDef =
          ChartDef{"exposure",
                   "Portfolio allocation over time, only top 10 holdings",
                   EpochFolioDashboardWidget::Area,
                   EpochFolioCategory::Positions},
      .lines = MakeSeriesLines(topPositionAllocations),
      .stacked = true};
}

LinesDef TearSheetFactory::MakeAllocationSummaryChart(
    epoch_frame::DataFrame const &positions) const {
  auto allocationSummary = GetMaxMedianPositionConcentration(positions);
  return LinesDef{
      .chartDef = ChartDef{"allocSummary",
                           "Long/Short max and median position concentration",
                           EpochFolioDashboardWidget::Area,
                           EpochFolioCategory::Positions},
      .lines = MakeSeriesLines(allocationSummary),
      .stacked = true};
}

LinesDef TearSheetFactory::MakeTotalHoldingsChart(
    epoch_frame::DataFrame const &positionsNoCashNoZero) const {
  auto dailyHoldings = positionsNoCashNoZero.count_valid(AxisType::Column);
  auto holdingsByMonthOverlay = MakeSeriesLine(
      dailyHoldings.resample_by_agg({factory::offset::month_end(1)}).mean(),
      "Average daily holdings, by month");
  holdingsByMonthOverlay.lineWidth = 5;
  auto avgDailyHoldings = dailyHoldings.mean();
  return LinesDef{
      .chartDef = ChartDef{"totalHoldings", "Total Holdings",
                           EpochFolioDashboardWidget::Lines,
                           EpochFolioCategory::Positions},
      .lines = {MakeSeriesLine(dailyHoldings, "Daily holdings")},
      .straightLines = {{"Average daily holdings, overall", avgDailyHoldings}},
      .overlay = holdingsByMonthOverlay};
}

LinesDef TearSheetFactory::MakeLongShortHoldingsChart(
    epoch_frame::DataFrame const &isLong,
    epoch_frame::DataFrame const &isShort) const {
  auto longHoldings = isLong.count_valid(AxisType::Column);
  auto shortHoldings = isShort.count_valid(AxisType::Column);

  auto longHoldingLegend =
      std::format("Long (max: {}, min: {})", longHoldings.max().as_int64(),
                  longHoldings.min().as_int64());
  auto shortHoldingLegend =
      std::format("Short (max: {}, min: {})", shortHoldings.max().as_int64(),
                  shortHoldings.min().as_int64());
  return LinesDef{
      .chartDef = ChartDef{"longShortHoldings", "Long and short holdings",
                           EpochFolioDashboardWidget::Area,
                           EpochFolioCategory::Positions},
      .lines = MakeSeriesLines(longHoldings, shortHoldings, longHoldingLegend,
                               shortHoldingLegend)};
}

LinesDef TearSheetFactory::MakeGrossLeverageChart() const {
  auto grossLeverage = GrossLeverage(m_positionsNoCash.assign("cash", m_cash));
  auto glMean = grossLeverage.mean();
  LinesDef grossLeverageChart{
      .chartDef = ChartDef{"grossLeverage", "Gross Leverage",
                           EpochFolioDashboardWidget::Lines,
                           EpochFolioCategory::Positions},
      .lines = {MakeSeriesLine(grossLeverage, "Gross Leverage")},
      .straightLines = {{"", glMean}},
  };
  return grossLeverageChart;
}

LinesDef TearSheetFactory::MakeSectorExposureChart() const {
  auto sectorExposures = GetSectorExposure(m_positionsNoCash, m_sectorMappings);
  sectorExposures = sectorExposures.assign("cash", m_cash);
  auto sectorAlloc = GetPercentAlloc(sectorExposures).drop("cash");
  return LinesDef{.chartDef =
                      ChartDef{"sectorExposure", "Sector Allocation over time",
                               EpochFolioDashboardWidget::Area,
                               EpochFolioCategory::Positions},
                  .lines = MakeSeriesLines(sectorAlloc),
                  .stacked = true};
}

std::vector<Chart> TearSheetFactory::MakeTopPositionsLineCharts(
    DataFrame const &positions, DataFrame const &topPositionAllocations) const {
  auto positionsNoCashNoZero =
      m_positionsNoCash.where(m_positionsNoCash != ZERO, Scalar{});

  auto isLong =
      positionsNoCashNoZero.where(positionsNoCashNoZero > ZERO, Scalar{});
  auto isShort =
      positionsNoCashNoZero.where(positionsNoCashNoZero < ZERO, Scalar{});

  std::vector<Chart> result;
  result.emplace_back(MakeExposureOverTimeChart(positions, isLong, isShort));
  result.emplace_back(MakeAllocationOverTimeChart(topPositionAllocations));
  result.emplace_back(MakeAllocationSummaryChart(positions));
  result.emplace_back(MakeTotalHoldingsChart(positionsNoCashNoZero));
  result.emplace_back(MakeLongShortHoldingsChart(isLong, isShort));
  result.emplace_back(MakeGrossLeverageChart());
  result.emplace_back(MakeSectorExposureChart());

  return result;
}

void TearSheetFactory::Make(uint32_t k, FullTearSheet &output) const {
  // TODO: maybe resample by max also not just last
  auto positions = epoch_frame::concat(
      {.frames = {m_positionsNoCash, m_cash.to_frame("cash")},
       .axis = epoch_frame::AxisType::Column});
  auto normalized_index = positions.index()->normalize();
  if (normalized_index->has_duplicates()) {
    positions =
        positions.group_by_agg(normalized_index->as_chunked_array()).last();
  }

  auto positionsAlloc = GetPercentAlloc(positions);
  auto topPositions = GetTopLongShortAbs(positionsAlloc);

  auto columns = topPositions[2].index()->array();

  output.positions = TearSheet{
      .charts = MakeTopPositionsLineCharts(positions, positionsAlloc[columns]),
      .tables = MakeTopPositionsTables(topPositions, k)};
}
} // namespace epoch_folio::positions