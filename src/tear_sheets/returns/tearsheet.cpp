//
// Created by adesola on 1/13/25.
//

#include "tearsheet.h"

#include "common/chart_def.h"
#include "common/table_helpers.h"
#include "common/type_helper.h"
#include "epoch_folio/tearsheet.h"
#include <common/methods_helper.h>
#include <common/python_utils.h>

#include "portfolio/timeseries.h"
#include "portfolio/txn.h"
#include <epoch_folio/empyrical_all.h>
#include <epoch_frame/common.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/table_factory.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_folio;
using namespace factory::index;

namespace epoch_folio::returns {
const Scalar ONE{1.0};
StraightLineDef kStraightLineAtOne = MakeStraightLine("", ONE, false);

const Scalar ZERO{0.0};
const StraightLineDef kStraightLineAtZero = MakeStraightLine("", ZERO, false);

constexpr const char *kBenchmarkColumnName = "benchmark";
constexpr const char *kStrategyColumnName = "strategy";

void TearSheetFactory::SetStrategyReturns(
    epoch_frame::Series const &strategyReturns) {
  m_strategy = strategyReturns;
}

void TearSheetFactory::SetBenchmark(
    epoch_frame::Series const &benchmarkReturns) {
  m_benchmark = benchmarkReturns;
}

DataFrame TearSheetFactory::GetStrategyAndBenchmark() const {
  return MakeDataFrame({m_strategyCumReturns, m_benchmarkCumReturns},
                       {kStrategyColumnName, kBenchmarkColumnName});
  ;
}

void TearSheetFactory::AlignReturnsAndBenchmark(
    epoch_frame::Series const &returns, epoch_frame::Series const &benchmark) {
  auto merged =
      epoch_frame::concat({.frames = {returns.to_frame(kStrategyColumnName),
                                      benchmark.to_frame(kBenchmarkColumnName)},
                           .axis = epoch_frame::AxisType::Column})
          .ffill()
          .drop_null();
  std::tie(m_strategy, m_benchmark) =
      std::pair{merged[kStrategyColumnName], merged[kBenchmarkColumnName]};
}

TearSheetFactory::TearSheetFactory(epoch_frame::DataFrame positions,
                                   epoch_frame::DataFrame transactions,
                                   epoch_frame::Series cash,
                                   epoch_frame::Series strategy,
                                   epoch_frame::Series benchmark)
    : m_cash(std::move(cash)), m_positions(std::move(positions)),
      m_transactions(std::move(transactions)) {
  AlignReturnsAndBenchmark(std::move(strategy), std::move(benchmark));

  m_strategyCumReturns = ep::CumReturns(m_strategy, 1.0);
  m_benchmarkCumReturns = ep::CumReturns(m_benchmark, 1.0);

  m_strategyReturnsInteresting = ExtractInterestingDateRanges(m_strategy);
  m_benchmarkReturnsInteresting = ExtractInterestingDateRanges(m_benchmark);
}

std::vector<Chart> TearSheetFactory::MakeReturnsLineCharts(
    const epoch_frame::DataFrame &df) const {
  const auto stddevOptions = arrow::compute::VarianceOptions{1};
  const auto bmarkVol = m_benchmark.stddev(stddevOptions);
  const auto returns =
      (m_strategy / m_strategy.stddev(stddevOptions)) * bmarkVol;
  const auto volatilityMatchedCumReturns = ep::CumReturns(returns, 1.0);
  const auto cumFactorReturns = df[kBenchmarkColumnName];

  std::vector<Chart> result;

  // Cumulative returns chart
  {
    Chart c;
    auto *ld = c.mutable_lines_def();
    auto *cd = ld->mutable_chart_def();
    cd->set_id("cumReturns");
    cd->set_title("Cumulative returns");
    cd->set_type(epoch_proto::WidgetLines);
    cd->set_category(epoch_folio::categories::StrategyBenchmark);
    auto lines = MakeSeriesLines(df);
    for (auto &line : lines) {
      *ld->add_lines() = std::move(line);
    }
    *ld->add_straight_lines() = kStraightLineAtOne;
    result.push_back(std::move(c));
  }

  // Volatility matched returns chart
  {
    Chart c;
    auto *ld = c.mutable_lines_def();
    auto *cd = ld->mutable_chart_def();
    cd->set_id("cumReturnsVolMatched");
    cd->set_title("Cumulative returns volatility matched to benchmark");
    cd->set_type(epoch_proto::WidgetLines);
    cd->set_category(epoch_folio::categories::StrategyBenchmark);
    auto lines = MakeSeriesLines(volatilityMatchedCumReturns, cumFactorReturns,
                                 kStrategyColumnName, kBenchmarkColumnName);
    for (auto &line : lines) {
      *ld->add_lines() = std::move(line);
    }
    *ld->add_straight_lines() = kStraightLineAtOne;
    result.push_back(std::move(c));
  }

  // Log scale chart
  {
    Chart c;
    auto *ld = c.mutable_lines_def();
    auto *cd = ld->mutable_chart_def();
    cd->set_id("cumReturnsLogScale");
    cd->set_title("Cumulative returns on log scale");
    cd->set_type(epoch_proto::WidgetLines);
    cd->set_category(epoch_folio::categories::StrategyBenchmark);
    *cd->mutable_y_axis() =
        MakeLinearAxis(); // Note: log axis would need special handling
    auto lines = MakeSeriesLines(df);
    for (auto &line : lines) {
      *ld->add_lines() = std::move(line);
    }
    *ld->add_straight_lines() = kStraightLineAtOne;
    result.push_back(std::move(c));
  }

  // Returns chart
  {
    Chart c;
    auto *ld = c.mutable_lines_def();
    auto *cd = ld->mutable_chart_def();
    cd->set_id("returns");
    cd->set_title("Returns");
    cd->set_type(epoch_proto::WidgetLines);
    cd->set_category(epoch_folio::categories::StrategyBenchmark);
    *cd->mutable_y_axis() = MakePercentageAxis("Returns (%)");
    *ld->add_lines() =
        MakeSeriesLine(m_strategy * Scalar{100.0}, kStrategyColumnName);
    *ld->add_straight_lines() = kStraightLineAtZero;
    result.push_back(std::move(c));
  }

  return result;
}

void TearSheetFactory::MakeRollingBetaCharts(std::vector<Chart> &lines) const {
  const auto df =
      concat({.frames = {m_strategy, m_benchmark}, .axis = AxisType::Column});
  const auto rolling6MonthBeta =
      RollingBeta(df, 6 * ep::APPROX_BDAYS_PER_MONTH);
  const auto rolling6MonthMean = rolling6MonthBeta.mean();
  const auto rolling12MonthBeta =
      RollingBeta(df, 12 * ep::APPROX_BDAYS_PER_MONTH);

  const auto rollingBeta =
      epoch_frame::concat({.frames = {rolling6MonthBeta.to_frame("6-mo"),
                                      rolling12MonthBeta.to_frame("12-mo")},
                           .axis = AxisType::Column});

  Chart c;
  auto *ld = c.mutable_lines_def();
  auto *cd = ld->mutable_chart_def();
  cd->set_id("rolling_beta");
  cd->set_title("Rolling portfolio beta");
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::StrategyBenchmark);
  auto seriesLines = MakeSeriesLines(rollingBeta);
  for (auto &line : seriesLines) {
    *ld->add_lines() = std::move(line);
  }
  *ld->add_straight_lines() = kStraightLineAtOne;
  *ld->add_straight_lines() =
      MakeStraightLine("6-mo Average", rolling6MonthMean, false);
  lines.push_back(std::move(c));
}

void TearSheetFactory::MakeRollingSharpeCharts(
    std::vector<Chart> &lines) const {
  const auto strategySharpe =
      RollingSharpe(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);
  const auto benchmarkSharpe =
      RollingSharpe(m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);

  Chart c;
  auto *ld = c.mutable_lines_def();
  auto *cd = ld->mutable_chart_def();
  cd->set_id("rollingSharpe");
  cd->set_title("Rolling Sharpe ratio (6 Months)");
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::RiskAnalysis);
  auto seriesLines = MakeSeriesLines(strategySharpe, benchmarkSharpe, "Sharpe",
                                     "Benchmark Sharpe");
  for (auto &line : seriesLines) {
    *ld->add_lines() = std::move(line);
  }
  *ld->add_straight_lines() =
      MakeStraightLine("Average Sharpe", strategySharpe.mean(), false);
  *ld->add_straight_lines() = kStraightLineAtZero;
  lines.push_back(std::move(c));
}

void TearSheetFactory::MakeRollingVolatilityCharts(
    std::vector<Chart> &lines) const {
  auto strategyVol =
      RollingVolatility(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);
  auto benchmarkVol =
      RollingVolatility(m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);

  Chart c;
  auto *ld = c.mutable_lines_def();
  auto *cd = ld->mutable_chart_def();
  cd->set_id("rollingVol");
  cd->set_title("Rolling volatility (6 Months)");
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::RiskAnalysis);

  *ld->add_lines() = MakeSeriesLine(strategyVol, "Volatility");
  *ld->add_lines() = MakeSeriesLine(benchmarkVol, "Benchmark Volatility");
  *ld->add_straight_lines() =
      MakeStraightLine("Average Volatility", strategyVol.mean(), false);
  *ld->add_straight_lines() = kStraightLineAtZero;

  lines.push_back(std::move(c));
}

void TearSheetFactory::MakeInterestingDateRangeLineCharts(
    std::vector<Chart> &lines) const {
  for (auto const &[strategy, benchmark] : std::views::zip(
           m_strategyReturnsInteresting, m_benchmarkReturnsInteresting)) {
    auto event = strategy.first;

    Chart c;
    auto *ld = c.mutable_lines_def();
    auto *cd = ld->mutable_chart_def();
    cd->set_id(event);
    cd->set_title(event);
    cd->set_type(epoch_proto::WidgetLines);
    cd->set_category(epoch_folio::categories::StrategyBenchmark);

    *ld->add_lines() =
        MakeSeriesLine(ep::CumReturns(strategy.second), kStrategyColumnName);
    *ld->add_lines() =
        MakeSeriesLine(ep::CumReturns(benchmark.second), kBenchmarkColumnName);
    *ld->add_straight_lines() = kStraightLineAtOne;

    lines.push_back(std::move(c));
  }
}

std::vector<Chart> TearSheetFactory::MakeStrategyBenchmarkLineCharts() const {
  const DataFrame df = GetStrategyAndBenchmark();

  std::vector<Chart> lines = MakeReturnsLineCharts(df);
  MakeRollingBetaCharts(lines);
  MakeInterestingDateRangeLineCharts(lines);
  return lines;
}

CardDef TearSheetFactory::MakePerformanceStats(
    epoch_core::TurnoverDenominator turnoverDenominator) const {
  (void)turnoverDenominator; // Mark as used
  CardDef card;
  card.set_type(epoch_proto::WidgetCard);
  card.set_category(epoch_folio::categories::StrategyBenchmark);
  card.set_group_size(4);

  const std::unordered_set<std::string> pct_fns{
      "Annual Return", "Cumulative Returns",  "Annual Volatility",
      "Max Drawdown",  "Daily Value at Risk",
  };

  auto positions =
      concat({.frames = {m_positions, m_cash}, .axis = AxisType::Column});
  constexpr uint8_t kGroup0 = 0;
  constexpr uint8_t kGroup1 = 1;
  constexpr uint8_t kGroup2 = 2;
  constexpr uint8_t kGroup3 = 3;

  if (!m_strategy.empty()) {
    auto start = m_strategy.index()->at(0);
    auto end = m_strategy.index()->at(-1);

    auto *data_start = card.add_data();
    data_start->set_title("Start date");
    *data_start->mutable_value() = ToProtoScalar(start);
    data_start->set_type(epoch_proto::TypeDate);
    data_start->set_group(kGroup0);

    auto *data_end = card.add_data();
    data_end->set_title("End date");
    *data_end->mutable_value() = ToProtoScalar(end);
    data_end->set_type(epoch_proto::TypeDate);
    data_end->set_group(kGroup0);

    auto months = start.dt().months_between(end).cast_int32();
    auto *data_months = card.add_data();
    data_months->set_title("Total months");
    *data_months->mutable_value() = ToProtoScalar(months);
    data_months->set_type(epoch_proto::TypeInteger);
    data_months->set_group(kGroup0);

    for (auto const &[stat, func] : ep::get_simple_stats()) {
      try {
        auto scalar = func(m_strategy);
        auto type = epoch_proto::TypeDecimal;
        if (pct_fns.contains(ep::get_stat_name(stat))) {
          scalar *= 100;
          type = epoch_proto::TypePercent;
        }

        auto *data_stat = card.add_data();
        data_stat->set_title(ep::get_stat_name(stat));
        *data_stat->mutable_value() = ToProtoScalar(Scalar{std::move(scalar)});
        data_stat->set_type(type);
        data_stat->set_group(kGroup1);
      } catch (const std::exception &e) {
        // Skip this stat if it fails
        SPDLOG_WARN("Failed to compute stat {}: {}", ep::get_stat_name(stat),
                    e.what());
      }
    }
  }

  if (!positions.empty()) {
    try {
      auto *data_leverage = card.add_data();
      data_leverage->set_title("Gross Leverage");
      *data_leverage->mutable_value() =
          ToProtoScalar(GrossLeverage(positions).mean());
      data_leverage->set_type(epoch_proto::TypeDecimal);
      data_leverage->set_group(kGroup2);
    } catch (const std::exception &e) {
      SPDLOG_WARN("Failed to compute gross leverage: {}", e.what());
    }

    if (!m_transactions.empty()) {
      try {
        auto *data_turnover = card.add_data();
        data_turnover->set_title("Daily Turnover");
        *data_turnover->mutable_value() = ToProtoScalar(
            GetTurnover(positions, m_transactions, turnoverDenominator).mean() *
            100.0_scalar);
        data_turnover->set_type(epoch_proto::TypePercent);
        data_turnover->set_group(kGroup2);
      } catch (const std::exception &e) {
        SPDLOG_WARN("Failed to compute daily turnover: {}", e.what());
      }
    }
  }

  if (!m_benchmark.empty()) {
    try {
      auto merged_returns = epoch_frame::make_dataframe(
          m_strategy.index(), {m_strategy.array(), m_benchmark.array()},
          {kStrategyColumnName, kBenchmarkColumnName});
      for (auto const &[stat, func] : ep::get_factor_stats()) {
        try {
          auto *data_factor = card.add_data();
          data_factor->set_title(ep::get_stat_name(stat));
          *data_factor->mutable_value() =
              ToProtoScalar(epoch_frame::Scalar{func(merged_returns)});
          data_factor->set_type(epoch_proto::TypeDecimal);
          data_factor->set_group(kGroup3);
        } catch (const std::exception &e) {
          SPDLOG_WARN("Failed to compute factor stat {}: {}",
                      ep::get_stat_name(stat), e.what());
        }
      }
    } catch (const std::exception &e) {
      SPDLOG_WARN("Failed to create merged returns for benchmark stats: {}",
                  e.what());
    }
  }

  return card;
}

Table TearSheetFactory::MakeStressEventTable() const {
  std::vector<std::vector<Scalar>> stressEvents(4);

  stressEvents[0].reserve(m_strategyReturnsInteresting.size());
  stressEvents[1].reserve(m_strategyReturnsInteresting.size());
  stressEvents[2].reserve(m_strategyReturnsInteresting.size());
  stressEvents[3].reserve(m_strategyReturnsInteresting.size());

  const Scalar hundred{100.0};
  for (auto const &[event, strategy] : m_strategyReturnsInteresting) {
    stressEvents[0].emplace_back(Scalar{event});
    stressEvents[1].emplace_back(strategy.mean() * hundred);
    stressEvents[2].emplace_back(strategy.min() * hundred);
    stressEvents[3].emplace_back(strategy.max() * hundred);
  }

  auto data = factory::table::make_table(
      stressEvents, {string_field("event"), float64_field("mean"),
                     float64_field("min"), float64_field("max")});

  Table out;
  out.set_type(epoch_proto::WidgetDataTable);
  out.set_category(epoch_folio::categories::StrategyBenchmark);
  out.set_title("Stress Events Analysis");

  auto *event_col = out.add_columns();
  event_col->set_name("event");
  event_col->set_type(epoch_proto::TypeString);

  auto *mean_col = out.add_columns();
  mean_col->set_name("mean");
  mean_col->set_type(epoch_proto::TypePercent);

  auto *min_col = out.add_columns();
  min_col->set_name("min");
  min_col->set_type(epoch_proto::TypePercent);

  auto *max_col = out.add_columns();
  max_col->set_name("max");
  max_col->set_type(epoch_proto::TypePercent);

  *out.mutable_data() = MakeTableDataFromArrow(data);
  return out;
}

Table TearSheetFactory::MakeWorstDrawdownTable(int64_t top,
                                               DrawDownTable &data) const {
  data = GenerateDrawDownTable(m_strategy, top);
  std::vector<std::vector<Scalar>> tableData(6);
  tableData[0].reserve(data.size());
  tableData[1].reserve(data.size());
  tableData[2].reserve(data.size());
  tableData[3].reserve(data.size());
  tableData[4].reserve(data.size());
  tableData[5].reserve(data.size());

  for (auto &&row : data) {
    tableData[0].emplace_back(std::move(row.index));
    tableData[1].emplace_back(std::move(row.peakDate));
    tableData[2].emplace_back(std::move(row.valleyDate));
    if (row.recoveryDate.has_value()) {
      tableData[3].emplace_back(std::move(*row.recoveryDate));
    } else {
      tableData[3].emplace_back(
          arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::NANO)));
    }
    tableData[4].emplace_back(std::move(row.duration));
    tableData[5].emplace_back(std::move(row.netDrawdown));
  }

  Table out;
  out.set_type(epoch_proto::WidgetDataTable);
  out.set_category(epoch_folio::categories::RiskAnalysis);
  out.set_title("Worst Drawdown Periods");

  auto *index_col = out.add_columns();
  index_col->set_name("index");
  index_col->set_type(epoch_proto::TypeInteger);

  auto *net_col = out.add_columns();
  net_col->set_name("netDrawdown");
  net_col->set_type(epoch_proto::TypePercent);

  auto *peak_col = out.add_columns();
  peak_col->set_name("peakDate");
  peak_col->set_type(epoch_proto::TypeDate);

  auto *duration_col = out.add_columns();
  duration_col->set_name("duration");
  duration_col->set_type(epoch_proto::TypeDayDuration);

  auto *valley_col = out.add_columns();
  valley_col->set_name("valleyDate");
  valley_col->set_type(epoch_proto::TypeDate);

  auto *recovery_col = out.add_columns();
  recovery_col->set_name("recoveryDate");
  recovery_col->set_type(epoch_proto::TypeDate);

  *out.mutable_data() = MakeTableDataFromArrow(factory::table::make_table(
      tableData, {int64_field("index"), datetime_field("peakDate"),
                  datetime_field("valleyDate"), datetime_field("recoveryDate"),
                  uint64_field("duration"), float64_field("netDrawdown")}));
  return out;
}

epoch_proto::TearSheet TearSheetFactory::MakeStrategyBenchmark(
    TurnoverDenominator turnoverDenominator) const {
  epoch_proto::TearSheet ts;

  try {
    auto card = MakePerformanceStats(turnoverDenominator);
    *ts.mutable_cards()->add_cards() = std::move(card);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create performance stats: {}", e.what());
  }

  try {
    auto charts = MakeStrategyBenchmarkLineCharts();
    for (auto &chart : charts) {
      *ts.mutable_charts()->add_charts() = std::move(chart);
    }
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create strategy benchmark charts: {}", e.what());
  }

  try {
    auto table = MakeStressEventTable();
    *ts.mutable_tables()->add_tables() = std::move(table);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create stress event table: {}", e.what());
  }

  return ts;
}

void TearSheetFactory::MakeRollingMaxDrawdownCharts(
    std::vector<Chart> &lines, DrawDownTable &drawDownTable,
    int64_t topKDrawDowns) const {
  Chart c;
  auto *ld = c.mutable_lines_def();
  auto *cd = ld->mutable_chart_def();
  cd->set_id("drawdowns");
  cd->set_title(std::format("Top {} drawdown periods", topKDrawDowns));
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::RiskAnalysis);

  *ld->add_lines() = MakeSeriesLine(m_strategyCumReturns, "Strategy");
  *ld->add_straight_lines() = kStraightLineAtOne;

  // TODO: Fix band method names
  for (auto const &row : drawDownTable) {
    auto recovery =
        row.recoveryDate.value_or(m_strategy.index()->at(-1).to_date().date());
    auto *band = ld->add_x_plot_bands();
    *band->mutable_from() = ToProtoScalar(Scalar{row.peakDate});
    *band->mutable_to() = ToProtoScalar(Scalar{recovery});
  }

  lines.push_back(std::move(c));
}

void TearSheetFactory::MakeUnderwaterCharts(std::vector<Chart> &lines) const {
  auto underwaterData =
      Scalar{100} * GetUnderwaterFromCumReturns(m_strategyCumReturns);

  Chart c;
  auto *ld = c.mutable_lines_def();
  auto *cd = ld->mutable_chart_def();
  cd->set_id("underwater");
  cd->set_title("Underwater plot");
  cd->set_type(epoch_proto::WidgetArea);
  cd->set_category(epoch_folio::categories::RiskAnalysis);

  *ld->add_lines() = MakeSeriesLine(underwaterData, "Underwater");

  lines.push_back(std::move(c));
}

epoch_proto::TearSheet
TearSheetFactory::MakeRiskAnalysis(int64_t topKDrawDowns) const {
  DrawDownTable drawDownTable;
  auto table = MakeWorstDrawdownTable(topKDrawDowns, drawDownTable);

  std::vector<epoch_proto::Chart> lines;
  MakeRollingVolatilityCharts(lines);
  MakeRollingSharpeCharts(lines);
  MakeRollingMaxDrawdownCharts(lines, drawDownTable, topKDrawDowns);
  MakeUnderwaterCharts(lines);

  epoch_proto::TearSheet ts;
  for (auto &chart : lines) {
    *ts.mutable_charts()->add_charts() = std::move(chart);
  }
  *ts.mutable_tables()->add_tables() = std::move(table);
  return ts;
}

std::unordered_map<std::string, std::string> month_to_string{
    {"1", "Jan"}, {"2", "Feb"},  {"3", "Mar"},  {"4", "Apr"},
    {"5", "May"}, {"6", "Jun"},  {"7", "Jul"},  {"8", "Aug"},
    {"9", "Sep"}, {"10", "Oct"}, {"11", "Nov"}, {"12", "Dec"}};

HeatMapDef TearSheetFactory::BuildMonthlyReturnsHeatMap() const {
  Scalar hundred_percent{100.0};
  auto monthlyReturns =
      ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

  HeatMapDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("monthlyReturns");
  cd->set_title("Monthly returns");
  cd->set_type(epoch_proto::WidgetHeatMap);
  cd->set_category(epoch_folio::categories::ReturnsDistribution);

  auto *y_axis = cd->mutable_y_axis();
  y_axis->set_type(epoch_proto::AxisCategory);
  y_axis->set_label("Year");

  auto *x_axis = cd->mutable_x_axis();
  x_axis->set_type(epoch_proto::AxisCategory);
  x_axis->set_label("Month");

  out.mutable_points()->Reserve(monthlyReturns.size());
  auto index = monthlyReturns.index();

  std::unordered_map<std::string, size_t> year_map;
  std::unordered_map<std::string, size_t> month_map;

  for (size_t i : std::views::iota(0UL, monthlyReturns.size())) {
    auto t =
        std::dynamic_pointer_cast<arrow::StructScalar>(index->at(i).value());
    AssertFromFormat(t != nullptr, "Expected a struct scalar");
    auto year = AssertScalarResultIsOk(t->field(0))->ToString();
    auto month = AssertScalarResultIsOk(t->field(1))->ToString();

    size_t x{};
    size_t y{};
    if (year_map.contains(year)) {
      y = year_map.at(year);
    } else {
      y = year_map.size();
      year_map.emplace(year, y);
      y_axis->add_categories(year);
    }

    if (month_map.contains(month)) {
      x = month_map.at(month);
    } else {
      x = month_map.size();
      month_map.emplace(month, x);
      x_axis->add_categories(month_to_string.at(month));
    }

    auto value = monthlyReturns.iloc(i);
    auto *point = out.add_points();
    point->mutable_x()->set_uint64_value(x);
    point->mutable_y()->set_uint64_value(y);
    *point->mutable_value() = ToProtoScalar(value * hundred_percent);
  }

  return out;
}

BarDef TearSheetFactory::BuildAnnualReturnsBar() const {
  auto annualReturns =
      ep::AggregateReturns(m_strategy, EmpyricalPeriods::yearly) *
      Scalar{100.0};
  auto mean = annualReturns.mean();
  auto data = annualReturns.contiguous_array();
  auto categories = annualReturns.index()
                        ->array()
                        .cast(arrow::utf8())
                        .to_vector<std::string>();

  BarDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("annualReturns");
  cd->set_title("Annual returns");
  cd->set_type(epoch_proto::WidgetBar);
  cd->set_category(epoch_folio::categories::ReturnsDistribution);

  auto *x_axis = cd->mutable_x_axis();
  x_axis->set_type(epoch_proto::AxisCategory);
  x_axis->set_label("Year");
  for (const auto &cat : categories) {
    x_axis->add_categories(cat);
  }

  *cd->mutable_y_axis() = MakePercentageAxis("Returns");
  *out.mutable_data() = MakeArrayFromArrow(data.as_chunked_array());
  *out.add_straight_lines() = MakeStraightLine("Mean", mean, false);

  return out;
}

HistogramDef TearSheetFactory::BuildMonthlyReturnsHistogram() const {
  auto monthlyReturnsTable =
      ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly) *
      Scalar{100.0};
  auto mean = monthlyReturnsTable.mean();
  auto data = monthlyReturnsTable.contiguous_array();

  HistogramDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("monthlyReturns");
  cd->set_title("Distribution of monthly returns");
  cd->set_type(epoch_proto::WidgetHistogram);
  cd->set_category(epoch_folio::categories::ReturnsDistribution);
  *cd->mutable_y_axis() = MakeLinearAxis("Number of Months");
  *cd->mutable_x_axis() = MakePercentageAxis("Monthly Returns");

  *out.mutable_data() = MakeArrayFromArrow(data.as_chunked_array());
  *out.add_straight_lines() = MakeStraightLine("Mean", mean, false);
  out.set_bins_count(12);

  return out;
}

BoxPlotDef TearSheetFactory::BuildReturnQuantiles() const {
  BoxPlotDataPointDef data;
  auto is_weekly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::weekly);
  auto is_monthly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

  auto [returns_plot, returns_outliers] = MakeBoxPlotDataPoint(0, m_strategy);
  auto [weekly_plot, weekly_outliers] = MakeBoxPlotDataPoint(1, is_weekly);
  auto [monthly_plot, monthly_outliers] = MakeBoxPlotDataPoint(2, is_monthly);

  *data.add_points() = returns_plot;
  *data.add_points() = weekly_plot;
  *data.add_points() = monthly_plot;

  // Add outliers
  for (const auto &outlier : returns_outliers) {
    *data.add_outliers() = outlier;
  }
  for (const auto &outlier : weekly_outliers) {
    *data.add_outliers() = outlier;
  }
  for (const auto &outlier : monthly_outliers) {
    *data.add_outliers() = outlier;
  }

  BoxPlotDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("returnQuantiles");
  cd->set_title("Return quantiles");
  cd->set_type(epoch_proto::WidgetBoxPlot);
  cd->set_category(epoch_folio::categories::ReturnsDistribution);
  *cd->mutable_y_axis() = MakePercentageAxis("Returns");

  auto *x_axis = cd->mutable_x_axis();
  x_axis->set_type(epoch_proto::AxisCategory);
  x_axis->set_label("");
  x_axis->add_categories("Daily");
  x_axis->add_categories("Weekly");
  x_axis->add_categories("Monthly");

  *out.mutable_data() = data;
  return out;
}

epoch_proto::TearSheet TearSheetFactory::MakeReturnsDistribution() const {
  epoch_proto::TearSheet ts;

  epoch_proto::Chart heatmap_chart;
  *heatmap_chart.mutable_heat_map_def() = BuildMonthlyReturnsHeatMap();
  *ts.mutable_charts()->add_charts() = std::move(heatmap_chart);

  epoch_proto::Chart bar_chart;
  *bar_chart.mutable_bar_def() = BuildAnnualReturnsBar();
  *ts.mutable_charts()->add_charts() = std::move(bar_chart);

  epoch_proto::Chart histogram_chart;
  *histogram_chart.mutable_histogram_def() = BuildMonthlyReturnsHistogram();
  *ts.mutable_charts()->add_charts() = std::move(histogram_chart);

  epoch_proto::Chart boxplot_chart;
  *boxplot_chart.mutable_box_plot_def() = BuildReturnQuantiles();
  *ts.mutable_charts()->add_charts() = std::move(boxplot_chart);

  return ts;
}

void TearSheetFactory::Make(TurnoverDenominator turnoverDenominator,
                            int64_t topKDrawDowns,
                            epoch_proto::FullTearSheet &output) const {
  try {
    auto strategy_benchmark = MakeStrategyBenchmark(turnoverDenominator);
    (*output.mutable_categories())[epoch_folio::categories::StrategyBenchmark] =
        std::move(strategy_benchmark);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create strategy benchmark tearsheet: {}", e.what());
  }

  try {
    auto risk_analysis = MakeRiskAnalysis(topKDrawDowns);
    (*output.mutable_categories())[epoch_folio::categories::RiskAnalysis] =
        std::move(risk_analysis);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create risk analysis tearsheet: {}", e.what());
  }

  try {
    auto returns_distribution = MakeReturnsDistribution();
    (*output
          .mutable_categories())[epoch_folio::categories::ReturnsDistribution] =
        std::move(returns_distribution);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create returns distribution tearsheet: {}",
                 e.what());
  }
}
} // namespace epoch_folio::returns
