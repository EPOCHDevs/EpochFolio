//
// Created by adesola on 1/13/25.
//

#include "tearsheet.h"

#include "common/type_helper.h"
#include "epoch_folio/tearsheet.h"
#include <algorithm>
#include <spdlog/spdlog.h>

#include "epoch_frame/scalar.h"
#include "portfolio/timeseries.h"
#include "portfolio/txn.h"
#include <epoch_folio/empyrical_all.h>
#include <epoch_frame/common.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <optional>

#include <epoch_dashboard/tearsheet/area_chart_builder.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_folio;
using namespace factory::index;

using epoch_proto::Chart;
using epoch_proto::StraightLineDef;

namespace epoch_folio::returns {
  StraightLineDef MakeStraightLine(const std::string& title, const Scalar& value, bool vertical) {
    StraightLineDef straightLine;
    straightLine.set_title(title);
    straightLine.set_value(value.cast_double().as_double());
    straightLine.set_vertical(vertical);
    return straightLine;
  }

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
    if (!m_benchmark.has_value()) {
      return MakeDataFrame({m_strategyCumReturns}, {kStrategyColumnName});
    }
    return MakeDataFrame({m_strategyCumReturns, m_benchmarkCumReturns},
                         {kStrategyColumnName, kBenchmarkColumnName});
  }

  void TearSheetFactory::AlignReturnsAndBenchmark(
      epoch_frame::Series const &returns, std::optional<epoch_frame::Series> const &benchmark) {
    if (!benchmark.has_value()) {
      m_strategy = returns;
      m_benchmark = std::nullopt;
      return;
    }

    auto merged =
        epoch_frame::concat({.frames = {returns.to_frame(kStrategyColumnName),
                                        benchmark->to_frame(kBenchmarkColumnName)},
                             .axis = epoch_frame::AxisType::Column})
            .ffill()
            .drop_null();
    m_strategy = merged[kStrategyColumnName];
    m_benchmark = merged[kBenchmarkColumnName];
  }

  TearSheetFactory::TearSheetFactory(epoch_frame::DataFrame positions,
                                     epoch_frame::DataFrame transactions,
                                     epoch_frame::Series cash,
                                     epoch_frame::Series strategy,
                                     std::optional<epoch_frame::Series> benchmark)
      : m_cash(std::move(cash)), m_positions(std::move(positions)),
        m_transactions(std::move(transactions)) {
    AlignReturnsAndBenchmark(std::move(strategy), std::move(benchmark));

    m_strategyCumReturns = ep::CumReturns(m_strategy, 1.0);

    if (m_benchmark.has_value()) {
      m_benchmarkCumReturns = ep::CumReturns(*m_benchmark, 1.0);
      m_benchmarkReturnsInteresting = ExtractInterestingDateRanges(*m_benchmark);
    } else {
      m_benchmarkCumReturns = epoch_frame::Series{};
      m_benchmarkReturnsInteresting = InterestingDateRangeReturns{};
    }

    m_strategyReturnsInteresting = ExtractInterestingDateRanges(m_strategy);
  }

  std::vector<Chart> TearSheetFactory::MakeReturnsLineCharts(
      const epoch_frame::DataFrame &df) const {
    std::vector<Chart> result;

    // Only calculate volatility matched returns if benchmark exists
    epoch_frame::Series volatilityMatchedCumReturns;
    epoch_frame::Series cumFactorReturns;

    if (m_benchmark.has_value()) {
      const auto stddevOptions = arrow::compute::VarianceOptions{1};
      const auto bmarkVol = m_benchmark->stddev(stddevOptions);
      const auto returns =
          (m_strategy / m_strategy.stddev(stddevOptions)) * bmarkVol;
      volatilityMatchedCumReturns = ep::CumReturns(returns, 1.0);
      cumFactorReturns = df[kBenchmarkColumnName];
    }

    // Cumulative returns chart
    try {
      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("cumReturns")
          .setTitle("Cumulative returns")
          .setCategory(epoch_folio::categories::StrategyBenchmark);

      // Add lines from DataFrame - specify all columns as y columns
      auto columns = df.column_names();
      builder.fromDataFrame(df, columns);
      builder.addStraightLine(kStraightLineAtOne);
      result.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create cumulative returns chart: {}", e.what());
    }

    // Volatility matched returns chart (only if benchmark exists)
    if (m_benchmark.has_value()) {
      try {
        epoch_tearsheet::LinesChartBuilder builder;
        builder.setId("cumReturnsVolMatched")
            .setTitle("Cumulative returns volatility matched to benchmark")
            .setCategory(epoch_folio::categories::StrategyBenchmark);

        // Create lines manually for volatility matched returns
        epoch_tearsheet::LineBuilder strategyLine;
        strategyLine.setName(kStrategyColumnName).fromSeries(volatilityMatchedCumReturns);
        builder.addLine(strategyLine.build());

        epoch_tearsheet::LineBuilder benchmarkLine;
        benchmarkLine.setName(kBenchmarkColumnName).fromSeries(cumFactorReturns);
        builder.addLine(benchmarkLine.build());

        builder.addStraightLine(kStraightLineAtOne);
        result.push_back(builder.build());
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed to create volatility matched returns chart: {}", e.what());
      }
    }

    // Log scale chart
    try {
      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("cumReturnsLogScale")
          .setTitle("Cumulative returns on log scale")
          .setCategory(epoch_folio::categories::StrategyBenchmark);

      // Add lines from DataFrame - specify all columns as y columns
      auto columns = df.column_names();
      builder.fromDataFrame(df, columns);
      builder.addStraightLine(kStraightLineAtOne);
      result.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create log scale returns chart: {}", e.what());
    }

    // Returns chart
    try {
      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("returns")
          .setTitle("Returns")
          .setCategory(epoch_folio::categories::StrategyBenchmark);

      // Add strategy returns line (converted to percentage)
      epoch_tearsheet::LineBuilder strategyLine;
      strategyLine.setName(kStrategyColumnName).fromSeries(m_strategy * Scalar{100.0});
      builder.addLine(strategyLine.build());

      builder.addStraightLine(kStraightLineAtZero);
      result.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create returns chart: {}", e.what());
    }

    return result;
  }

  void TearSheetFactory::MakeRollingBetaCharts(std::vector<Chart> &lines) const {
    // Skip if no benchmark
    if (!m_benchmark.has_value()) {
      return;
    }

    try {
      const auto df =
          concat({.frames = {m_strategy, *m_benchmark}, .axis = AxisType::Column});
      const auto rolling6MonthBeta =
          RollingBeta(df, 6 * ep::APPROX_BDAYS_PER_MONTH);
      const auto rolling6MonthMean = rolling6MonthBeta.mean();
      const auto rolling12MonthBeta =
          RollingBeta(df, 12 * ep::APPROX_BDAYS_PER_MONTH);

      const auto rollingBeta =
          epoch_frame::concat({.frames = {rolling6MonthBeta.to_frame("6-mo"),
                                          rolling12MonthBeta.to_frame("12-mo")},
                               .axis = AxisType::Column});

      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("rolling_beta")
          .setTitle("Rolling portfolio beta")
          .setCategory(epoch_folio::categories::StrategyBenchmark);

      // Add lines from DataFrame - specify all columns as y columns
      auto columns = rollingBeta.column_names();
      builder.fromDataFrame(rollingBeta, columns);
      builder.addStraightLine(kStraightLineAtOne);
      builder.addStraightLine(MakeStraightLine("6-mo Average", rolling6MonthMean, false));

      lines.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create rolling beta chart: {}", e.what());
    }
  }

  void TearSheetFactory::MakeRollingSharpeCharts(
      std::vector<Chart> &lines) const {
    try {
      const auto strategySharpe =
          RollingSharpe(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);

      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("rollingSharpe")
          .setTitle("Rolling Sharpe ratio (6 Months)")
          .setCategory(epoch_folio::categories::RiskAnalysis);

      // Add strategy Sharpe line
      epoch_tearsheet::LineBuilder strategyLine;
      strategyLine.setName("Sharpe").fromSeries(strategySharpe);
      builder.addLine(strategyLine.build());

      if (m_benchmark.has_value()) {
        const auto benchmarkSharpe =
            RollingSharpe(*m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);
        epoch_tearsheet::LineBuilder benchmarkLine;
        benchmarkLine.setName("Benchmark Sharpe").fromSeries(benchmarkSharpe);
        builder.addLine(benchmarkLine.build());
      }

      builder.addStraightLine(MakeStraightLine("Average Sharpe", strategySharpe.mean(), false));
      builder.addStraightLine(kStraightLineAtZero);

      lines.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create rolling Sharpe chart: {}", e.what());
    }
  }

  void TearSheetFactory::MakeRollingVolatilityCharts(
      std::vector<Chart> &lines) const {
    try {
      auto strategyVol =
          RollingVolatility(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);

      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("rollingVol")
          .setTitle("Rolling volatility (6 Months)")
          .setCategory(epoch_folio::categories::RiskAnalysis);

      // Add strategy volatility line
      epoch_tearsheet::LineBuilder strategyLine;
      strategyLine.setName("Volatility").fromSeries(strategyVol);
      builder.addLine(strategyLine.build());

      if (m_benchmark.has_value()) {
        auto benchmarkVol =
            RollingVolatility(*m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);
        epoch_tearsheet::LineBuilder benchmarkLine;
        benchmarkLine.setName("Benchmark Volatility").fromSeries(benchmarkVol);
        builder.addLine(benchmarkLine.build());
      }

      builder.addStraightLine(MakeStraightLine("Average Volatility", strategyVol.mean(), false));
      builder.addStraightLine(kStraightLineAtZero);

      lines.push_back(builder.build());
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Failed to create rolling volatility chart: {}", e.what());
    }
  }

  void TearSheetFactory::MakeInterestingDateRangeLineCharts(
      std::vector<Chart> &lines) const {
    // Handle strategy date ranges
    for (auto const &strategy : m_strategyReturnsInteresting) {
      auto event = strategy.first;

      try {
        epoch_tearsheet::LinesChartBuilder builder;
        builder.setId(event)
            .setTitle(event)
            .setCategory(epoch_folio::categories::StrategyBenchmark);

        // Add strategy line
        epoch_tearsheet::LineBuilder strategyLine;
        strategyLine.setName(kStrategyColumnName).fromSeries(ep::CumReturns(strategy.second));
        builder.addLine(strategyLine.build());

        // Add benchmark line if available
        if (m_benchmark.has_value() && !m_benchmarkReturnsInteresting.empty()) {
          // Find matching benchmark date range
          auto benchmarkIt = std::find_if(
              m_benchmarkReturnsInteresting.begin(),
              m_benchmarkReturnsInteresting.end(),
              [&event](const auto& b) { return b.first == event; });

          if (benchmarkIt != m_benchmarkReturnsInteresting.end()) {
            epoch_tearsheet::LineBuilder benchmarkLine;
            benchmarkLine.setName(kBenchmarkColumnName).fromSeries(ep::CumReturns(benchmarkIt->second));
            builder.addLine(benchmarkLine.build());
          }
        }

        builder.addStraightLine(kStraightLineAtOne);
        lines.push_back(builder.build());
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed to create interesting date range chart for {}: {}", event, e.what());
      }
    }
  }

  std::vector<Chart> TearSheetFactory::MakeStrategyBenchmarkLineCharts() const {
    const DataFrame df = GetStrategyAndBenchmark();

    std::vector<Chart> lines = MakeReturnsLineCharts(df);
    MakeRollingBetaCharts(lines);
    MakeInterestingDateRangeLineCharts(lines);
    return lines;
  }

  epoch_proto::CardDef TearSheetFactory::MakePerformanceStats(
      epoch_core::TurnoverDenominator turnoverDenominator) const {
    try {
      (void)turnoverDenominator; // Mark as used

      epoch_tearsheet::CardBuilder cardBuilder;
      cardBuilder.setType(epoch_proto::WidgetCard)
          .setCategory(epoch_folio::categories::StrategyBenchmark)
          .setGroupSize(4);

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

        // Add date fields using new CardDataBuilder
        epoch_tearsheet::CardDataBuilder startDateBuilder;
        startDateBuilder.setTitle("Start date")
            .setValue(epoch_tearsheet::ScalarFactory::create(start))
            .setType(epoch_proto::TypeDate)
            .setGroup(kGroup0);
        cardBuilder.addCardData(startDateBuilder.build());

        epoch_tearsheet::CardDataBuilder endDateBuilder;
        endDateBuilder.setTitle("End date")
            .setValue(epoch_tearsheet::ScalarFactory::create(end))
            .setType(epoch_proto::TypeDate)
            .setGroup(kGroup0);
        cardBuilder.addCardData(endDateBuilder.build());

        auto months = start.dt().months_between(end).cast_int32();
        epoch_tearsheet::CardDataBuilder monthsBuilder;
        monthsBuilder.setTitle("Total months")
            .setValue(epoch_tearsheet::ScalarFactory::create(months))
            .setType(epoch_proto::TypeInteger)
            .setGroup(kGroup0);
        cardBuilder.addCardData(monthsBuilder.build());

        for (auto const &[stat, func] : ep::get_simple_stats()) {
          try {
            auto scalar = func(m_strategy);
            epoch_tearsheet::CardDataBuilder statBuilder;
            statBuilder.setTitle(ep::get_stat_name(stat))
                .setGroup(kGroup1);

            if (pct_fns.contains(ep::get_stat_name(stat))) {
              statBuilder
              .setType(epoch_proto::TypePercent)
              .setValue(epoch_tearsheet::ScalarFactory::fromPercentValue(scalar));
            } else {
              statBuilder.setType(epoch_proto::TypeDecimal).setValue(epoch_tearsheet::ScalarFactory::fromDecimal(scalar));
            }
            cardBuilder.addCardData(statBuilder.build());
          } catch (const std::exception &e) {
            // Skip this stat if it fails
            SPDLOG_WARN("Failed to compute stat {}: {}", ep::get_stat_name(stat),
                        e.what());
          }
        }
      }

      if (!positions.empty()) {
        try {
          epoch_tearsheet::CardDataBuilder leverageBuilder;
          leverageBuilder.setTitle("Gross Leverage")
              .setValue(epoch_tearsheet::ScalarFactory::create(GrossLeverage(positions).mean()))
              .setType(epoch_proto::TypeDecimal)
              .setGroup(kGroup2);
          cardBuilder.addCardData(leverageBuilder.build());
        } catch (const std::exception &e) {
          SPDLOG_WARN("Failed to compute gross leverage: {}", e.what());
        }

        if (!m_transactions.empty()) {
          try {
            auto turnover =
                GetTurnover(positions, m_transactions, turnoverDenominator).mean().as_double();
            epoch_tearsheet::CardDataBuilder turnoverBuilder;
            turnoverBuilder.setTitle("Daily Turnover")
                .setValue(epoch_tearsheet::ScalarFactory::fromPercentValue(turnover))
                .setType(epoch_proto::TypePercent)
                .setGroup(kGroup2);
            cardBuilder.addCardData(turnoverBuilder.build());
          } catch (const std::exception &e) {
            SPDLOG_WARN("Failed to compute daily turnover: {}", e.what());
          }
        }
      }

      if (m_benchmark.has_value()) {
        try {
          auto merged_returns = epoch_frame::make_dataframe(
              m_strategy.index(), {m_strategy.array(), m_benchmark->array()},
              {kStrategyColumnName, kBenchmarkColumnName});
          for (auto const &[stat, func] : ep::get_factor_stats()) {
            try {
              epoch_tearsheet::CardDataBuilder factorBuilder;
              factorBuilder.setTitle(ep::get_stat_name(stat))
                  .setValue(epoch_tearsheet::ScalarFactory::create(epoch_frame::Scalar{func(merged_returns)}))
                  .setType(epoch_proto::TypeDecimal)
                  .setGroup(kGroup3);
              cardBuilder.addCardData(factorBuilder.build());
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

      return cardBuilder.build();
    } catch (const std::exception &e) {
      SPDLOG_ERROR("Exception in MakePerformanceStats: {}", e.what());
      return epoch_proto::CardDef{};
    }
  }

  epoch_proto::Table TearSheetFactory::MakeStressEventTable() const {
    try {
      epoch_tearsheet::TableBuilder builder;
      builder.setType(epoch_proto::WidgetDataTable)
          .setCategory(epoch_folio::categories::StrategyBenchmark)
          .setTitle("Stress Events Analysis");

      // Add columns
      builder.addColumn("event", "Event", epoch_proto::TypeString)
          .addColumn("mean", "Mean", epoch_proto::TypePercent)
          .addColumn("min", "Min", epoch_proto::TypePercent)
          .addColumn("max", "Max", epoch_proto::TypePercent);

      // Build data incrementally using addRow
      if (!m_strategyReturnsInteresting.empty()) {
        for (auto const &[event, strategy] : m_strategyReturnsInteresting) {
          epoch_proto::TableRow row;
          *row.add_values() = epoch_tearsheet::ScalarFactory::create(Scalar{event});
          *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(strategy.mean().cast_double().as_double() * 100);
          *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(strategy.min().cast_double().as_double() * 100);
          *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(strategy.max().cast_double().as_double() * 100);
          builder.addRow(row);
        }
      }

      return builder.build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in MakeStressEventTable: {}", e.what());
      return epoch_proto::Table{};
    }
  }

  epoch_proto::Table TearSheetFactory::MakeWorstDrawdownTable(int64_t top,
                                                 DrawDownTable &data) const {
    try {
      data = GenerateDrawDownTable(m_strategy, top);

      epoch_tearsheet::TableBuilder builder;
      builder.setType(epoch_proto::WidgetDataTable)
          .setCategory(epoch_folio::categories::RiskAnalysis)
          .setTitle("Worst Drawdown Periods");

      // Add columns
      builder.addColumn("netDrawdown", "Net Drawdown", epoch_proto::TypePercent)
          .addColumn("peakDate", "Peak Date", epoch_proto::TypeDate)
          .addColumn("valleyDate", "Valley Date", epoch_proto::TypeDate)
          .addColumn("recoveryDate", "Recovery Date", epoch_proto::TypeDate)
          .addColumn("duration", "Duration", epoch_proto::TypeDayDuration);

      // Build data incrementally using addRow
      if (!data.empty()) {
        for (const auto &row : data) {
          epoch_proto::TableRow table_row;
          *table_row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(row.netDrawdown.cast_double().as_double());
          *table_row.add_values() = epoch_tearsheet::ScalarFactory::fromDate(row.peakDate);
          *table_row.add_values() = epoch_tearsheet::ScalarFactory::fromDate(row.valleyDate);
          *table_row.add_values() = row.recoveryDate.has_value() ?
            epoch_tearsheet::ScalarFactory::fromDate(*row.recoveryDate) :
            epoch_tearsheet::ScalarFactory::create(Scalar{});
          *table_row.add_values() = epoch_tearsheet::ScalarFactory::fromDayDuration(row.duration.value<size_t>().value());
          builder.addRow(table_row);
        }
      }

      return builder.build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in MakeWorstDrawdownTable: {}", e.what());
      return epoch_proto::Table{};
    }
  }

 void TearSheetFactory::MakeStrategyBenchmark(
      TurnoverDenominator turnoverDenominator, epoch_tearsheet::DashboardBuilder& ts) const {
    try {
      ts.addCard(MakePerformanceStats(turnoverDenominator));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create performance stats: {}", e.what());
    }

    try {
      auto charts = MakeStrategyBenchmarkLineCharts();
      for (auto &chart : charts) {
        ts.addChart(std::move(chart));
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create strategy benchmark charts: {}", e.what());
    }

    try {
      auto table = MakeStressEventTable();
      ts.addTable(std::move(table));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create stress event table: {}", e.what());
    }
  }

  void TearSheetFactory::MakeRollingMaxDrawdownCharts(
      std::vector<Chart> &lines, DrawDownTable &drawDownTable,
      int64_t topKDrawDowns) const {
    try {
      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("drawdowns")
          .setTitle(std::format("Top {} drawdown periods", topKDrawDowns))
          .setCategory(epoch_folio::categories::RiskAnalysis);

      // Add strategy line
      epoch_tearsheet::LineBuilder lineBuilder;
      lineBuilder.setName("Strategy").fromSeries(m_strategyCumReturns);
      builder.addLine(lineBuilder.build());

      // Add straight line at one
      builder.addStraightLine(kStraightLineAtOne);

      // Add bands for drawdown periods
      for (auto const &row : drawDownTable) {
        auto recovery =
            row.recoveryDate.value_or(m_strategy.index()->at(-1).to_date().date());

        epoch_proto::Band band;
        // Convert Date to timestamp in milliseconds
        band.mutable_from()->set_timestamp_ms(epoch_frame::DateTime{row.peakDate}.m_nanoseconds.count() / 1e6);
        band.mutable_to()->set_timestamp_ms(epoch_frame::DateTime{recovery}.m_nanoseconds.count() / 1e6);
        builder.addXPlotBand(band);
      }

      lines.push_back(builder.build());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in MakeRollingMaxDrawdownCharts: {}", e.what());
      epoch_tearsheet::LinesChartBuilder builder;
      builder.setId("drawdowns")
          .setTitle(std::format("Top {} drawdown periods", topKDrawDowns))
          .setCategory(epoch_folio::categories::RiskAnalysis);
      lines.push_back(builder.build());
    }
  }

  void TearSheetFactory::MakeUnderwaterCharts(std::vector<Chart> &lines) const {
    try {
      auto underwaterData =
          Scalar{100} * GetUnderwaterFromCumReturns(m_strategyCumReturns);

      epoch_tearsheet::AreaChartBuilder builder;
      builder.setId("underwater")
          .setTitle("Underwater plot")
          .setCategory(epoch_folio::categories::RiskAnalysis);

      epoch_tearsheet::LineBuilder lineBuilder;
      lineBuilder.setName("Underwater").fromSeries(underwaterData);
      builder.addArea(lineBuilder.build());

      lines.push_back(builder.build());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in MakeUnderwaterCharts: {}", e.what());
      epoch_tearsheet::AreaChartBuilder builder;
      builder.setId("underwater")
          .setTitle("Underwater plot")
          .setCategory(epoch_folio::categories::RiskAnalysis);
      lines.push_back(builder.build());
    }
  }

  void
  TearSheetFactory::MakeRiskAnalysis(int64_t topKDrawDowns, epoch_tearsheet::DashboardBuilder &output) const {
    try {
      DrawDownTable drawDownTable;
      auto table = MakeWorstDrawdownTable(topKDrawDowns, drawDownTable);

      std::vector<epoch_proto::Chart> lines;

      try {
        MakeRollingVolatilityCharts(lines);
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed in MakeRollingVolatilityCharts: {}", e.what());
        throw;
      }

      try {
        MakeRollingSharpeCharts(lines);
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed in MakeRollingSharpeCharts: {}", e.what());
        throw;
      }

      try {
        MakeRollingMaxDrawdownCharts(lines, drawDownTable, topKDrawDowns);
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed in MakeRollingMaxDrawdownCharts: {}", e.what());
        throw;
      }

      try {
        MakeUnderwaterCharts(lines);
      } catch (const std::exception &e) {
        SPDLOG_ERROR("Failed in MakeUnderwaterCharts: {}", e.what());
        throw;
      }

      for (auto &chart : lines) {
        output.addChart(std::move(chart));
      }
      output.addTable(std::move(table));
    } catch (const std::exception &e) {
      SPDLOG_ERROR("MakeRiskAnalysis failed: {}", e.what());
      throw;
    }
  }

  std::unordered_map<std::string, std::string> month_to_string{
      {"1", "Jan"}, {"2", "Feb"},  {"3", "Mar"},  {"4", "Apr"},
      {"5", "May"}, {"6", "Jun"},  {"7", "Jul"},  {"8", "Aug"},
      {"9", "Sep"}, {"10", "Oct"}, {"11", "Nov"}, {"12", "Dec"}};

  epoch_proto::Chart TearSheetFactory::BuildMonthlyReturnsHeatMap() const {
    try {
      Scalar hundred_percent{100.0};
      auto monthlyReturns =
          ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

      epoch_tearsheet::HeatMapChartBuilder builder;
      builder.setId("monthlyReturns")
          .setTitle("Monthly returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .setYAxisLabel("Year")
          .setXAxisLabel("Month")
          .setYAxisType(epoch_proto::AxisCategory)
          .setXAxisType(epoch_proto::AxisCategory);

      auto index = monthlyReturns.index();
      std::unordered_map<std::string, size_t> year_map;
      std::unordered_map<std::string, size_t> month_map;
      std::vector<std::string> years;
      std::vector<std::string> months;

      for (size_t i : std::views::iota(0UL, monthlyReturns.size())) {
        auto t =
            std::dynamic_pointer_cast<arrow::StructScalar>(index->at(i).value());
        if (t == nullptr) {
          SPDLOG_WARN("Expected a struct scalar at index {}", i);
          continue;
        }

        auto year = AssertScalarResultIsOk(t->field(0))->ToString();
        auto month = AssertScalarResultIsOk(t->field(1))->ToString();

        size_t x{};
        size_t y{};
        if (year_map.contains(year)) {
          y = year_map.at(year);
        } else {
          y = year_map.size();
          year_map.emplace(year, y);
          years.push_back(year);
        }

        if (month_map.contains(month)) {
          x = month_map.at(month);
        } else {
          x = month_map.size();
          month_map.emplace(month, x);
          months.push_back(month_to_string.at(month));
        }

        auto value = monthlyReturns.iloc(i);
        builder.addPoint(x, y, (value * hundred_percent).as_double());
      }

      builder.setYAxisCategories(years)
          .setXAxisCategories(months);

      return builder.build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in BuildMonthlyReturnsHeatMap: {}", e.what());
      epoch_tearsheet::HeatMapChartBuilder builder;
      builder.setId("monthlyReturns")
          .setTitle("Monthly returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution);
      return builder.build();
    }
  }

  epoch_proto::Chart TearSheetFactory::BuildAnnualReturnsBar() const {
    try {
      auto annualReturns =
          ep::AggregateReturns(m_strategy, EmpyricalPeriods::yearly) *
          Scalar{100.0};
      auto mean = annualReturns.mean();

      epoch_tearsheet::BarChartBuilder builder;
      builder.setId("annualReturns")
          .setTitle("Annual returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .setXAxisLabel("Year")
          .setYAxisLabel("Returns (%)")
          .setXAxisType(epoch_proto::AxisCategory);

      // Extract year categories from the annual returns index
      auto index = annualReturns.index();
      std::vector<std::string> years;

      auto t = index->array().to_view<uint64_t>();
      if (t != nullptr) {
        for (size_t i = 0; i < annualReturns.size(); ++i) {
          auto year = t->Value(i);
          years.push_back(std::to_string(year));
        }
      }
      else {
        throw std::runtime_error("invalid index type for BuildAnnualReturnsBar; expected uint64 got " + index->dtype()->ToString());
      }

      builder.setXAxisCategories(years);
      builder.fromSeries(annualReturns);
      builder.addStraightLine(MakeStraightLine("Mean", mean, false));

      return builder.build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in BuildAnnualReturnsBar: {}", e.what());
      return epoch_tearsheet::BarChartBuilder()
          .setId("annualReturns")
          .setTitle("Annual returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .build();
    }
  }

  epoch_proto::Chart TearSheetFactory::BuildMonthlyReturnsHistogram() const {
    try {
      auto monthlyReturnsTable =
          ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly) *
          Scalar{100.0};
      auto mean = monthlyReturnsTable.mean();

      return epoch_tearsheet::HistogramChartBuilder()
          .setId("monthlyReturns")
          .setTitle("Distribution of monthly returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .setYAxisLabel("Number of Months")
          .setXAxisLabel("Monthly Returns")
          .fromSeries(monthlyReturnsTable)
          .addStraightLine(MakeStraightLine("Mean", mean, false))
          .setBinsCount(12)
          .build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in BuildMonthlyReturnsHistogram: {}", e.what());
      return epoch_tearsheet::HistogramChartBuilder()
          .setId("monthlyReturns")
          .setTitle("Distribution of monthly returns")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .build();
    }
  }

  // Helper function to create box plot data point from a series
  std::pair<epoch_proto::BoxPlotDataPoint, std::vector<epoch_proto::BoxPlotOutlier>>
  CreateBoxPlotDataPoint(int x_index, const epoch_frame::Series &series) {
    epoch_proto::BoxPlotDataPoint point;
    std::vector<epoch_proto::BoxPlotOutlier> outliers;

    try {
      if (series.empty()) {
        return {point, outliers};
      }

      // Convert to percentage (multiply by 100)
      auto percentage_series = series * epoch_frame::Scalar{100.0};

      // Calculate quartiles
      auto q1 = percentage_series.quantile(arrow::compute::QuantileOptions{0.25});
      auto median = percentage_series.quantile(arrow::compute::QuantileOptions{0.5});
      auto q3 = percentage_series.quantile(arrow::compute::QuantileOptions{0.75});
      auto min_val = percentage_series.min();
      auto max_val = percentage_series.max();

      // Calculate IQR and outlier boundaries
      auto iqr = q3 - q1;
      auto lower_bound = q1 - iqr * epoch_frame::Scalar{1.5};
      auto upper_bound = q3 + iqr * epoch_frame::Scalar{1.5};

      // Find whiskers (min/max within bounds)
      auto lower_whisker = min_val;
      auto upper_whisker = max_val;

      for (size_t i = 0; i < percentage_series.size(); ++i) {
        auto value = percentage_series.iloc(i);
        if (value >= lower_bound && value < q1) {
          if (value > lower_whisker || lower_whisker < lower_bound) {
            lower_whisker = value;
          }
        }
        if (value <= upper_bound && value > q3) {
          if (value < upper_whisker || upper_whisker > upper_bound) {
            upper_whisker = value;
          }
        }

        // Collect outliers
        if (value < lower_bound || value > upper_bound) {
          epoch_proto::BoxPlotOutlier outlier;
          outlier.set_category_index(x_index);
          outlier.set_value(value.as_double());
          outliers.push_back(outlier);
        }
      }

      // Set box plot data point values
      point.set_low(lower_whisker.as_double());
      point.set_q1(q1.as_double());
      point.set_median(median.as_double());
      point.set_q3(q3.as_double());
      point.set_high(upper_whisker.as_double());

    } catch (const std::exception &e) {
      SPDLOG_WARN("Failed to calculate box plot statistics: {}", e.what());
    }

    return {point, outliers};
  }

  epoch_proto::Chart TearSheetFactory::BuildReturnQuantiles() const {
    try {
      auto is_weekly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::weekly);
      auto is_monthly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

      auto [returns_plot, returns_outliers] = CreateBoxPlotDataPoint(0, m_strategy);
      auto [weekly_plot, weekly_outliers] = CreateBoxPlotDataPoint(1, is_weekly);
      auto [monthly_plot, monthly_outliers] = CreateBoxPlotDataPoint(2, is_monthly);

      // Use the BoxPlotChartBuilder
      auto builder = epoch_tearsheet::BoxPlotChartBuilder()
          .setId("returnQuantiles")
          .setTitle("Return quantiles")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .setXAxisLabel("")
          .setYAxisLabel("Returns")
          .setXAxisType(epoch_proto::AxisCategory)
          .setXAxisCategories({"Daily", "Weekly", "Monthly"})
          .addDataPoint(returns_plot)
          .addDataPoint(weekly_plot)
          .addDataPoint(monthly_plot);

      // Add outliers one by one
      for (const auto &outlier : returns_outliers) {
        builder.addOutlier(outlier);
      }
      for (const auto &outlier : weekly_outliers) {
        builder.addOutlier(outlier);
      }
      for (const auto &outlier : monthly_outliers) {
        builder.addOutlier(outlier);
      }

      return builder.build();
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Exception in BuildReturnQuantiles: {}", e.what());
      return epoch_tearsheet::BoxPlotChartBuilder()
          .setId("returnQuantiles")
          .setTitle("Return quantiles")
          .setCategory(epoch_folio::categories::ReturnsDistribution)
          .build();
    }
  }

  void TearSheetFactory::MakeReturnsDistribution(epoch_tearsheet::DashboardBuilder &output) const {
    try {
      output.addChart(BuildMonthlyReturnsHeatMap());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create monthly returns heatmap: {}", e.what());
    }

    try {
      output.addChart(BuildAnnualReturnsBar());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create annual returns bar chart: {}", e.what());
    }

    try {
      output.addChart(BuildMonthlyReturnsHistogram());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create monthly returns histogram: {}", e.what());
    }

    try {
      output.addChart(BuildReturnQuantiles());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create return quantiles chart: {}", e.what());
    }
  }

  void TearSheetFactory::Make(TurnoverDenominator turnoverDenominator,
                              int64_t topKDrawDowns,
                              epoch_tearsheet::DashboardBuilder &output) const {
    try {
      MakeStrategyBenchmark(turnoverDenominator, output);
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create strategy benchmark tearsheet: {}", e.what());
    }

    try {
      MakeRiskAnalysis(topKDrawDowns, output);
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create risk analysis tearsheet: {}", e.what());
    }

    try {
      MakeReturnsDistribution(output);
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create returns distribution tearsheet: {}",
                   e.what());
    }
  }
} // namespace epoch_folio::returns
