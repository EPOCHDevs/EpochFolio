//
// Created by adesola on 1/14/25.
//

#include "tearsheet.h"

#include "common/table_helpers.h"
#include "common/type_helper.h"
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

epoch_proto::Table MakeTopPositionsTable(std::string const &id,
                                         std::string const &name,
                                         Series const &x, uint64_t k) {
  try {
    k = std::min<uint64_t>(k, x.size());
    epoch_proto::Table table_proto;
    table_proto.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_DATA_TABLE);
    table_proto.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
    table_proto.set_title(name);

    // Columns: id/name and max (%)
    {
      epoch_proto::ColumnDef c;
      c.set_id(id);
      c.set_name(name);
      c.set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);
      *table_proto.add_columns() = std::move(c);
    }
    {
      epoch_proto::ColumnDef c;
      c.set_id("max");
      c.set_name("Max");
      c.set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);
      *table_proto.add_columns() = std::move(c);
    }

    if (k == 0) {
      return table_proto;
    }

    std::vector<std::vector<Scalar>> rows(2, std::vector<Scalar>(k));
    auto index = x.index();
    for (size_t i = 0; i < k; ++i) {
      rows[0][i] = index && i < index->size() ? index->at(i) : Scalar{};
      rows[1][i] = x.iloc(i) * HUNDRED;
    }

    arrow::FieldVector fields;
    fields.emplace_back(string_field(id));
    fields.emplace_back(float64_field("max"));
    auto data = factory::table::make_table(rows, fields);
    *table_proto.mutable_data() = MakeTableDataFromArrow(data);
    return table_proto;
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeTopPositionsTable for {}: {}", name,
                 e.what());
    return epoch_proto::Table{};
  }
}

std::vector<Table>
MakeTopPositionsTables(std::array<Series, 3> const &topPositions, uint64_t k) {
  std::vector<Table> out;
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

LinesDef TearSheetFactory::MakeExposureOverTimeChart(
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

    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("exposure");
    cd->set_title("Exposure");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_AREA);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);

    // Long/Short lines
    auto ls = MakeSeriesLines(longExposure, shortExposure, "Long", "Short");
    for (auto &line : ls) {
      *out.add_lines() = std::move(line);
    }
    // Net overlay as an additional line
    *out.add_lines() = MakeSeriesLine(netExposure, "Net");
    return out;
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeExposureOverTimeChart: {}", e.what());
    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("exposure");
    cd->set_title("Exposure");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_AREA);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
    return out;
  }
}

LinesDef TearSheetFactory::MakeAllocationOverTimeChart(
    epoch_frame::DataFrame const &topPositionAllocations) const {
  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("allocationOverTime");
  cd->set_title("Allocation over time");
  cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);

  auto lines = MakeSeriesLines(topPositionAllocations);
  for (auto &line : lines) {
    *out.add_lines() = std::move(line);
  }
  return out;
}

LinesDef TearSheetFactory::MakeAllocationSummaryChart(
    epoch_frame::DataFrame const &positions) const {
  // Minimal summary: net allocation over time
  auto net = positions.sum(AxisType::Column);

  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("allocationSummary");
  cd->set_title("Allocation summary");
  cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
  *out.add_lines() = MakeSeriesLine(net, "Net");
  return out;
}

LinesDef TearSheetFactory::MakeTotalHoldingsChart(
    epoch_frame::DataFrame const &positionsNoCashNoZero) const {
  auto dailyHoldings = positionsNoCashNoZero.count_valid(AxisType::Column);
  auto holdingsByMonthOverlay = MakeSeriesLine(
      dailyHoldings.resample_by_agg({factory::offset::month_end(1)}).mean(),
      "Average daily holdings, by month");
  holdingsByMonthOverlay.set_line_width(5);
  auto avgDailyHoldings = dailyHoldings.mean();

  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("totalHoldings");
  cd->set_title("Total Holdings");
  cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
  *out.add_lines() = MakeSeriesLine(dailyHoldings, "Daily holdings");
  *out.add_straight_lines() = MakeStraightLine(
      "Average daily holdings, overall", avgDailyHoldings, false);
  return out;
}

LinesDef TearSheetFactory::MakeLongShortHoldingsChart(
    epoch_frame::DataFrame const &isLong,
    epoch_frame::DataFrame const &isShort) const {
  try {
    auto longHoldings = isLong.count_valid(AxisType::Column);
    auto shortHoldings = isShort.count_valid(AxisType::Column);
    std::string longHoldingLegend, shortHoldingLegend;
    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("longShortHoldings");
    cd->set_title("Long and short holdings");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_AREA);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);

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
    auto lines = MakeSeriesLines(longHoldings, shortHoldings, longHoldingLegend,
                                 shortHoldingLegend);
    for (auto &line : lines) {
      *out.add_lines() = std::move(line);
    }

    return out;
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeLongShortHoldingsChart: {}", e.what());
    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("longShortHoldings");
    cd->set_title("Long and short holdings");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_AREA);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
    return out;
  }
}

LinesDef TearSheetFactory::MakeGrossLeverageChart() const {
  try {
    auto grossLeverage =
        GrossLeverage(m_positionsNoCash.assign("cash", m_cash));
    auto glMean = grossLeverage.mean();

    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("grossLeverage");
    cd->set_title("Gross Leverage");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
    *out.add_lines() = MakeSeriesLine(grossLeverage, "Gross Leverage");
    *out.add_straight_lines() = MakeStraightLine("", glMean, false);
    return out;
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeGrossLeverageChart: {}", e.what());
    LinesDef out;
    auto *cd = out.mutable_chart_def();
    cd->set_id("grossLeverage");
    cd->set_title("Gross Leverage");
    cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
    cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
    return out;
  }
}

LinesDef TearSheetFactory::MakeSectorExposureChart() const {
  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("sectorExposure");
  cd->set_title("Sector Exposure");
  cd->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  cd->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_POSITIONS);
  return out;
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
    Chart c;
    *c.mutable_lines_def() =
        MakeExposureOverTimeChart(positions, isLong, isShort);
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create exposure over time chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() =
        MakeAllocationOverTimeChart(topPositionAllocations);
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation over time chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() = MakeAllocationSummaryChart(positions);
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create allocation summary chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() = MakeTotalHoldingsChart(positionsNoCashNoZero);
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create total holdings chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() = MakeLongShortHoldingsChart(isLong, isShort);
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create long short holdings chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() = MakeGrossLeverageChart();
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create gross leverage chart: {}", e.what());
  }

  try {
    Chart c;
    *c.mutable_lines_def() = MakeSectorExposureChart();
    result.push_back(std::move(c));
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create sector exposure chart: {}", e.what());
  }

  return result;
}

void TearSheetFactory::Make(uint32_t k,
                            epoch_proto::FullTearSheet &output) const {
  try {
    auto positions = epoch_frame::concat(
        {.frames = {m_positionsNoCash, m_cash.to_frame("cash")},
         .axis = epoch_frame::AxisType::Column});

    auto positionsAlloc = epoch_folio::GetPercentAlloc(positions);
    auto topPositions = epoch_folio::GetTopLongShortAbs(positionsAlloc);

    if (topPositions[2].size() == 0) {
      SPDLOG_WARN("No top positions found");
      output.mutable_positions()->Clear();
      return;
    }

    auto columns = topPositions[2].index()->array();

    epoch_proto::TearSheet ts;
    // Charts for top positions and summaries
    try {
      auto charts =
          MakeTopPositionsLineCharts(positions, positionsAlloc[columns]);
      for (auto &chart : charts) {
        *ts.add_charts() = std::move(chart);
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to build positions charts: {}", e.what());
    }
    // Tables for top positions
    try {
      auto tables = MakeTopPositionsTables(topPositions, k);
      for (auto &table : tables) {
        *ts.add_tables() = std::move(table);
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to build positions tables: {}", e.what());
    }

    *output.mutable_positions() = std::move(ts);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in TearSheetFactory::Make: {}", e.what());
    output.mutable_positions()->Clear();
  }
}
} // namespace epoch_folio::positions