//
// Created by adesola on 1/14/25.
//

#include "tearsheet.h"

#include "portfolio/pos.h"
#include "portfolio/timeseries.h"
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <spdlog/spdlog.h>

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
  try {
    k = std::min(k, x.size());
    if (k == 0) {
      SPDLOG_WARN("Empty series provided to MakeTopPositionsTable for {}",
                  name);
      return Table{EpochFolioDashboardWidget::DataTable,
                   EpochFolioCategory::Positions,
                   name,
                   {ColumnDef{id, name, EpochFolioType::String},
                    ColumnDef{"max", "Max", EpochFolioType::Percent}},
                   nullptr};
    }

    std::vector rows(2, std::vector<Scalar>(k));

    auto index = x.index();
    if (!index) {
      SPDLOG_ERROR("Null index in MakeTopPositionsTable for {}", name);
      return Table{EpochFolioDashboardWidget::DataTable,
                   EpochFolioCategory::Positions,
                   name,
                   {ColumnDef{id, name, EpochFolioType::String},
                    ColumnDef{"max", "Max", EpochFolioType::Percent}},
                   nullptr};
    }

    for (size_t i = 0; i < k; ++i) {
      if (i < index->size()) {
        rows[0][i] = index->at(i);
      } else {
        SPDLOG_WARN("Index access out of bounds at position {} for {}", i,
                    name);
        rows[0][i] = Scalar{};
      }
      rows[1][i] = x.iloc(i) * HUNDRED;
    }
    auto data = factory::table::make_table(
        rows, {string_field(id), float64_field("max")});
    return {EpochFolioDashboardWidget::DataTable,
            EpochFolioCategory::Positions,
            name,
            {ColumnDef{id, name, EpochFolioType::String},
             ColumnDef{"max", "Max", EpochFolioType::Percent}},
            data};
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeTopPositionsTable for {}: {}", name,
                 e.what());
    return Table{EpochFolioDashboardWidget::DataTable,
                 EpochFolioCategory::Positions,
                 name,
                 {ColumnDef{id, name, EpochFolioType::String},
                  ColumnDef{"max", "Max", EpochFolioType::Percent}},
                 nullptr};
  }
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
  try {
    auto positionSum = positions.sum(AxisType::Column);

    // Check for division by zero
    auto validPositionSum = positionSum.where(positionSum != ZERO, Scalar{1.0});

    auto longExposure = isLong.sum(AxisType::Column) / validPositionSum;
    auto shortExposure = isShort.sum(AxisType::Column) / validPositionSum;
    auto netExposure =
        m_positionsNoCash.sum(AxisType::Column) / validPositionSum;

    auto netExposureLine = MakeSeriesLine(netExposure, "Net");
    netExposureLine.dashStyle = "dot";

    return LinesDef{
        .chartDef =
            ChartDef{"exposure", "Exposure", EpochFolioDashboardWidget::Area,
                     EpochFolioCategory::Positions},
        .lines = MakeSeriesLines(longExposure, shortExposure, "Long", "Short"),
        .overlay = netExposureLine};
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeExposureOverTimeChart: {}", e.what());
    return LinesDef{.chartDef = ChartDef{"exposure", "Exposure",
                                         EpochFolioDashboardWidget::Area,
                                         EpochFolioCategory::Positions},
                    .lines = {},
                    .overlay = {}};
  }
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
  try {
    auto longHoldings = isLong.count_valid(AxisType::Column);
    auto shortHoldings = isShort.count_valid(AxisType::Column);

    std::string longHoldingLegend, shortHoldingLegend;

    try {
      auto longMax = longHoldings.max().as_int64();
      auto longMin = longHoldings.min().as_int64();
      longHoldingLegend =
          std::format("Long (max: {}, min: {})", longMax, longMin);
    } catch (std::exception const &e) {
      SPDLOG_WARN("Failed to format long holdings legend: {}", e.what());
      longHoldingLegend = "Long";
    }

    try {
      auto shortMax = shortHoldings.max().as_int64();
      auto shortMin = shortHoldings.min().as_int64();
      shortHoldingLegend =
          std::format("Short (max: {}, min: {})", shortMax, shortMin);
    } catch (std::exception const &e) {
      SPDLOG_WARN("Failed to format short holdings legend: {}", e.what());
      shortHoldingLegend = "Short";
    }

    return LinesDef{
        .chartDef = ChartDef{"longShortHoldings", "Long and short holdings",
                             EpochFolioDashboardWidget::Area,
                             EpochFolioCategory::Positions},
        .lines = MakeSeriesLines(longHoldings, shortHoldings, longHoldingLegend,
                                 shortHoldingLegend)};
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeLongShortHoldingsChart: {}", e.what());
    return LinesDef{.chartDef =
                        ChartDef{"longShortHoldings", "Long and short holdings",
                                 EpochFolioDashboardWidget::Area,
                                 EpochFolioCategory::Positions},
                    .lines = {}};
  }
}

LinesDef TearSheetFactory::MakeGrossLeverageChart() const {
  try {
    auto grossLeverage =
        GrossLeverage(m_positionsNoCash.assign("cash", m_cash));
    auto glMean = grossLeverage.mean();
    LinesDef grossLeverageChart{
        .chartDef = ChartDef{"grossLeverage", "Gross Leverage",
                             EpochFolioDashboardWidget::Lines,
                             EpochFolioCategory::Positions},
        .lines = {MakeSeriesLine(grossLeverage, "Gross Leverage")},
        .straightLines = {{"", glMean}},
    };
    return grossLeverageChart;
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeGrossLeverageChart: {}", e.what());
    return LinesDef{.chartDef = ChartDef{"grossLeverage", "Gross Leverage",
                                         EpochFolioDashboardWidget::Lines,
                                         EpochFolioCategory::Positions},
                    .lines = {},
                    .straightLines = {}};
  }
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

  try {
    result.emplace_back(MakeExposureOverTimeChart(positions, isLong, isShort));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create exposure over time chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeAllocationOverTimeChart(topPositionAllocations));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation over time chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeAllocationSummaryChart(positions));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation summary chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeTotalHoldingsChart(positionsNoCashNoZero));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create total holdings chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeLongShortHoldingsChart(isLong, isShort));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create long short holdings chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeGrossLeverageChart());
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create gross leverage chart: {}", e.what());
  }

  try {
    result.emplace_back(MakeSectorExposureChart());
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create sector exposure chart: {}", e.what());
  }

  return result;
}

void TearSheetFactory::Make(uint32_t k, FullTearSheet &output) const {
  try {
    // TODO: maybe resample by max also not just last
    auto positions = epoch_frame::concat(
        {.frames = {m_positionsNoCash, m_cash.to_frame("cash")},
         .axis = epoch_frame::AxisType::Column});

    auto positionsAlloc = GetPercentAlloc(positions);
    auto topPositions = GetTopLongShortAbs(positionsAlloc);

    // Check if topPositions has valid data
    if (topPositions[2].size() == 0) {
      SPDLOG_WARN("No top positions found, using empty columns");
      output.positions = TearSheet{.charts = {}, .tables = {}};
      return;
    }

    auto columns = topPositions[2].index()->array();
    if (columns.length() == 0) {
      SPDLOG_ERROR("Empty columns array in top positions");
      output.positions = TearSheet{.charts = {}, .tables = {}};
      return;
    }

    output.positions =
        TearSheet{.charts = MakeTopPositionsLineCharts(positions,
                                                       positionsAlloc[columns]),
                  .tables = MakeTopPositionsTables(topPositions, k)};
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in TearSheetFactory::Make: {}", e.what());
    output.positions = TearSheet{.charts = {}, .tables = {}};
  }
}
} // namespace epoch_folio::positions