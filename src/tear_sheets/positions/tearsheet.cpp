//
// Created by adesola on 1/14/25.
//

#include "tearsheet.h"

#include "common/type_helper.h"
#include "epoch_dashboard/tearsheet/dashboard_builders.h"
#include "epoch_folio/tearsheet.h"
#include "portfolio/pos.h"
#include "portfolio/timeseries.h"
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <spdlog/spdlog.h>

using namespace epoch_frame;
using namespace epoch_core;

namespace epoch_folio::positions {
const Scalar ZERO{0.0};

TearSheetFactory::TearSheetFactory(
    epoch_frame::Series cash, epoch_frame::DataFrame positions,
    epoch_frame::Series returns,
    std::unordered_map<std::string, std::string> sectorMappings)
    : m_cash(std::move(cash).rename("cash")),
      m_positionsNoCash(std::move(positions)), m_strategy(std::move(returns)),
      m_sectorMappings(std::move(sectorMappings)) {}

epoch_proto::Table MakeTopPositionsTable(std::string const &id,
                                         std::string const &name,
                                         Series const &x, uint64_t k) {
  try {
    k = std::min<uint64_t>(k, x.size());

    epoch_tearsheet::TableBuilder builder;
    builder.setType(epoch_proto::WidgetDataTable)
        .setCategory(epoch_folio::categories::Positions)
        .setTitle(name);

    // Add columns: id/name and max (%)
    builder.addColumn(id, name, epoch_proto::TypeString)
        .addColumn("max", "Max", epoch_proto::TypePercent);

    if (k == 0) {
      return builder.build();
    }

    // Build data incrementally using addRow
    auto index = x.index();
    for (size_t i = 0; i < k; ++i) {
      epoch_proto::TableRow row;
      *row.add_values() = epoch_tearsheet::ScalarFactory::create(index && i < index->size() ? index->at(i) : Scalar{});
      *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(x.iloc(i).as_double() * 100);
      builder.addRow(row);
    }
    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeTopPositionsTable for {}: {}", name,
                 e.what());
    return epoch_proto::Table{};
  }
}

std::vector<epoch_proto::Table>
MakeTopPositionsTables(std::array<Series, 3> const &topPositions, uint64_t k) {
  std::vector<epoch_proto::Table> out;
  out.reserve(3);
  try {
    out.emplace_back(MakeTopPositionsTable("asset", "Top Long Positions",
                                           topPositions[0], k));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed Top Long Positions table: {}", e.what());
  }
  try {
    out.emplace_back(MakeTopPositionsTable("asset", "Top Short Positions",
                                           topPositions[1], k));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed Top Short Positions table: {}", e.what());
  }
  try {
    out.emplace_back(MakeTopPositionsTable("asset", "Top Absolute Positions",
                                           topPositions[2], k));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed Top Absolute Positions table: {}", e.what());
  }
  return out;
}

epoch_proto::Chart TearSheetFactory::MakeExposureOverTimeChart(
    epoch_frame::DataFrame const &positions,
    epoch_frame::DataFrame const &isLong,
    epoch_frame::DataFrame const &isShort) const {
  try {
    auto positionSum = positions.sum(AxisType::Column);

    // Avoid division by zero
    auto validPositionSum = positionSum.where(positionSum != ZERO, Scalar{1.0});

    auto longExposure = isLong.sum(AxisType::Column) / validPositionSum;
    auto shortExposure = isShort.sum(AxisType::Column) / validPositionSum;
    auto netExposure =
        m_positionsNoCash.sum(AxisType::Column) / validPositionSum;

    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("exposure")
        .setTitle("Exposure")
        .setCategory(epoch_folio::categories::Positions);

    // Long/Short lines
    epoch_tearsheet::LineBuilder longLineBuilder;
    longLineBuilder.setName("Long").fromSeries(longExposure);
    builder.addLine(longLineBuilder.build());

    epoch_tearsheet::LineBuilder shortLineBuilder;
    shortLineBuilder.setName("Short").fromSeries(shortExposure);
    builder.addLine(shortLineBuilder.build());

    // Net overlay as an additional line
    epoch_tearsheet::LineBuilder netLineBuilder;
    netLineBuilder.setName("Net").fromSeries(netExposure);
    builder.addLine(netLineBuilder.build());

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeExposureOverTimeChart: {}", e.what());
    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("exposure")
        .setTitle("Exposure")
        .setCategory(epoch_folio::categories::Positions);
    return builder.build();
  }
}

epoch_proto::Chart TearSheetFactory::MakeAllocationOverTimeChart(
    epoch_frame::DataFrame const &topPositionAllocations) const {
  epoch_tearsheet::LinesChartBuilder builder;
  builder.setId("allocationOverTime")
      .setTitle("Allocation over time")
      .setCategory(epoch_folio::categories::Positions);

  // Add a line for each column in the DataFrame
  for (const auto &columnName : topPositionAllocations.column_names()) {
    epoch_tearsheet::LineBuilder lineBuilder;
    lineBuilder.setName(columnName)
        .fromSeries(topPositionAllocations[columnName]);
    builder.addLine(lineBuilder.build());
  }
  return builder.build();
}

epoch_proto::Chart TearSheetFactory::MakeAllocationSummaryChart(
    epoch_frame::DataFrame const &positions) const {
  // Minimal summary: net allocation over time
  auto net = positions.sum(AxisType::Column);

  epoch_tearsheet::LinesChartBuilder builder;
  builder.setId("allocationSummary")
      .setTitle("Allocation summary")
      .setCategory(epoch_folio::categories::Positions);

  epoch_tearsheet::LineBuilder lineBuilder;
  lineBuilder.setName("Net").fromSeries(net);
  builder.addLine(lineBuilder.build());

  return builder.build();
}

epoch_proto::Chart TearSheetFactory::MakeTotalHoldingsChart(
    epoch_frame::DataFrame const &positionsNoCashNoZero) const {
  auto dailyHoldings = positionsNoCashNoZero.count_valid(AxisType::Column);
  auto holdingsByMonth =
      dailyHoldings.resample_by_agg({factory::offset::month_end(1)}).mean();
  auto avgDailyHoldings = dailyHoldings.mean();

  epoch_tearsheet::LinesChartBuilder builder;
  builder.setId("totalHoldings")
      .setTitle("Total Holdings")
      .setCategory(epoch_folio::categories::Positions);

  // Daily holdings line
  epoch_tearsheet::LineBuilder dailyLineBuilder;
  dailyLineBuilder.setName("Daily holdings").fromSeries(dailyHoldings.cast(arrow::float64()));
  builder.addLine(dailyLineBuilder.build());

  // Monthly average line with thicker width
  epoch_tearsheet::LineBuilder monthlyLineBuilder;
  monthlyLineBuilder.setName("Average daily holdings, by month")
      .fromSeries(holdingsByMonth)
      .setLineWidth(5);
  builder.addLine(monthlyLineBuilder.build());

  // Add straight line for overall average
  epoch_proto::StraightLineDef straightLine;
  straightLine.set_title("Average daily holdings, overall");
  straightLine.set_value(avgDailyHoldings.cast_double().as_double());
  straightLine.set_vertical(false);
  builder.addStraightLine(straightLine);

  return builder.build();
}

epoch_proto::Chart TearSheetFactory::MakeLongShortHoldingsChart(
    epoch_frame::DataFrame const &isLong,
    epoch_frame::DataFrame const &isShort) const {
  try {
    auto longHoldings = isLong.count_valid(AxisType::Column);
    auto shortHoldings = isShort.count_valid(AxisType::Column);
    std::string longHoldingLegend, shortHoldingLegend;

    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("longShortHoldings")
        .setTitle("Long and short holdings")
        .setCategory(epoch_folio::categories::Positions);

    try {
      auto longMaxScalar = longHoldings.max();
      auto longMinScalar = longHoldings.min();
      if (!longMaxScalar.is_null() && !longMinScalar.is_null()) {
        auto longMax = longMaxScalar.as_int64();
        auto longMin = longMinScalar.as_int64();
        longHoldingLegend =
            std::format("Long (max: {}, min: {})", longMax, longMin);
      } else {
        longHoldingLegend = "Long";
      }
    } catch (std::exception const &e) {
      SPDLOG_WARN("Failed to format long holdings legend: {}", e.what());
      longHoldingLegend = "Long";
    }

    try {
      auto shortMaxScalar = shortHoldings.max();
      auto shortMinScalar = shortHoldings.min();
      if (!shortMaxScalar.is_null() && !shortMinScalar.is_null()) {
        auto shortMax = shortMaxScalar.as_int64();
        auto shortMin = shortMinScalar.as_int64();
        shortHoldingLegend =
            std::format("Short (max: {}, min: {})", shortMax, shortMin);
      } else {
        shortHoldingLegend = "Short";
      }
    } catch (std::exception const &e) {
      SPDLOG_WARN("Failed to format short holdings legend: {}", e.what());
      shortHoldingLegend = "Short";
    }

    // Add long holdings line
    epoch_tearsheet::LineBuilder longLineBuilder;
    longLineBuilder.setName(longHoldingLegend).fromSeries(longHoldings.cast(arrow::float64()));
    builder.addLine(longLineBuilder.build());

    // Add short holdings line
    epoch_tearsheet::LineBuilder shortLineBuilder;
    shortLineBuilder.setName(shortHoldingLegend).fromSeries(shortHoldings.cast(arrow::float64()));
    builder.addLine(shortLineBuilder.build());

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeLongShortHoldingsChart: {}", e.what());
    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("longShortHoldings")
        .setTitle("Long and short holdings")
        .setCategory(epoch_folio::categories::Positions);
    return builder.build();
  }
}

epoch_proto::Chart TearSheetFactory::MakeGrossLeverageChart() const {
  try {
    auto grossLeverage =
        GrossLeverage(m_positionsNoCash.assign("cash", m_cash));
    auto glMean = grossLeverage.mean();

    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("grossLeverage")
        .setTitle("Gross Leverage")
        .setCategory(epoch_folio::categories::Positions);

    epoch_tearsheet::LineBuilder lineBuilder;
    lineBuilder.setName("Gross Leverage").fromSeries(grossLeverage);
    builder.addLine(lineBuilder.build());

    epoch_proto::StraightLineDef straightLine;
    straightLine.set_title("");
    straightLine.set_value(glMean.cast_double().as_double());
    straightLine.set_vertical(false);
    builder.addStraightLine(straightLine);

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeGrossLeverageChart: {}", e.what());
    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("grossLeverage")
        .setTitle("Gross Leverage")
        .setCategory(epoch_folio::categories::Positions);
    return builder.build();
  }
}

epoch_proto::Chart TearSheetFactory::MakeSectorExposureChart() const {
  auto sectorExposures = GetSectorExposure(m_positionsNoCash, m_sectorMappings);
  sectorExposures = sectorExposures.assign("cash", m_cash);
  auto sectorAlloc = GetPercentAlloc(sectorExposures).drop("cash");

  epoch_tearsheet::LinesChartBuilder builder;
  builder.setId("sectorExposure")
      .setTitle("Sector Allocation over time")
      .setCategory(epoch_folio::categories::Positions);

  // Add a line for each sector column
  for (const auto &columnName : sectorAlloc.column_names()) {
    epoch_tearsheet::LineBuilder lineBuilder;
    lineBuilder.setName(columnName).fromSeries(sectorAlloc[columnName]);
    builder.addLine(lineBuilder.build());
  }

  return builder.build();
}

std::vector<epoch_proto::Chart> TearSheetFactory::MakeTopPositionsLineCharts(
    DataFrame const &positions, DataFrame const &topPositionAllocations) const {
  auto positionsNoCashNoZero =
      m_positionsNoCash.where(m_positionsNoCash != ZERO, Scalar{});

  auto isLong =
      positionsNoCashNoZero.where(positionsNoCashNoZero > ZERO, Scalar{});
  auto isShort =
      positionsNoCashNoZero.where(positionsNoCashNoZero < ZERO, Scalar{});

  std::vector<epoch_proto::Chart> result;

  try {
    result.push_back(MakeExposureOverTimeChart(positions, isLong, isShort));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create exposure over time chart: {}", e.what());
  }

  try {
    result.push_back(MakeAllocationOverTimeChart(topPositionAllocations));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation over time chart: {}", e.what());
  }

  try {
    result.push_back(MakeAllocationSummaryChart(positions));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation summary chart: {}", e.what());
  }

  try {
    result.push_back(MakeTotalHoldingsChart(positionsNoCashNoZero));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create total holdings chart: {}", e.what());
  }

  try {
    result.push_back(MakeLongShortHoldingsChart(isLong, isShort));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create long short holdings chart: {}", e.what());
  }

  try {
    result.push_back(MakeGrossLeverageChart());
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create gross leverage chart: {}", e.what());
  }

  try {
    result.push_back(MakeSectorExposureChart());
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create sector exposure chart: {}", e.what());
  }

  return result;
}

void TearSheetFactory::Make(uint32_t k,
                            epoch_tearsheet::DashboardBuilder &output) const {
  try {
    auto positions = epoch_frame::concat(
        {.frames = {m_positionsNoCash, m_cash.to_frame("cash")},
         .axis = epoch_frame::AxisType::Column});

    auto positionsAlloc = epoch_folio::GetPercentAlloc(positions);
    auto topPositions = epoch_folio::GetTopLongShortAbs(positionsAlloc);

    if (topPositions[2].size() == 0) {
      SPDLOG_WARN("No top positions found");
      return;
    }

    auto columns = topPositions[2].index()->array();

    // Charts for top positions and summaries
    try {
      auto charts =
          MakeTopPositionsLineCharts(positions, positionsAlloc[columns]);
      for (auto &chart : charts) {
        output.addChart(chart);
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to build positions charts: {}", e.what());
    }
    // Tables for top positions
    try {
      auto tables = MakeTopPositionsTables(topPositions, k);
      for (auto &table : tables) {
        output.addTable(table);
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to build positions tables: {}", e.what());
    }
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in TearSheetFactory::Make: {}", e.what());
  }
}
} // namespace epoch_folio::positions