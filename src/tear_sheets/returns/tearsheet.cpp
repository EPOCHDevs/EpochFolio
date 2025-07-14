//
// Created by adesola on 1/13/25.
//

#include "tearsheet.h"

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
    StraightLineDef kStraightLineAtOne{"", ONE, false};

    const Scalar ZERO{0.0};
    const StraightLineDef kStraightLineAtZero{"", ZERO, false};

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

        result.emplace_back(
            LinesDef{{"cumReturns", "Cumulative returns",
                      epoch_core::EpochFolioDashboardWidget::Lines,
                      EpochFolioCategory::StrategyBenchmark},
                     MakeSeriesLines(df),
                     StraightLines{kStraightLineAtOne}});

        result.emplace_back(
            LinesDef{{"cumReturnsVolMatched",
                      "Cumulative returns volatility matched to benchmark",
                      epoch_core::EpochFolioDashboardWidget::Lines,
                      epoch_core::EpochFolioCategory::StrategyBenchmark},
                     MakeSeriesLines(volatilityMatchedCumReturns, cumFactorReturns,
                                     kStrategyColumnName, kBenchmarkColumnName),
                     StraightLines{kStraightLineAtOne}});

        result.emplace_back(
            LinesDef{{"cumReturnsLogScale", "Cumulative returns on log scale",
                      epoch_core::EpochFolioDashboardWidget::Lines,
                      epoch_core::EpochFolioCategory::StrategyBenchmark,
                      AxisDef{.type = kLogAxisType}},
                     MakeSeriesLines(df),
                     StraightLines{kStraightLineAtOne}});

        result.emplace_back(LinesDef{
            {"returns", "Returns", epoch_core::EpochFolioDashboardWidget::Lines,
             epoch_core::EpochFolioCategory::StrategyBenchmark,
             MakePercentageAxis("Returns (%)")},
            {MakeSeriesLine(m_strategy * 100_scalar, kStrategyColumnName)},
            StraightLines{kStraightLineAtZero}});
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

        lines.emplace_back(
            LinesDef{{"rolling_beta", "Rolling portfolio beta",
                      epoch_core::EpochFolioDashboardWidget::Lines,
                      epoch_core::EpochFolioCategory::StrategyBenchmark},
                     MakeSeriesLines(rollingBeta),
                     StraightLines{
                         kStraightLineAtOne,
                         StraightLineDef{"6-mo Average", rolling6MonthMean, false}}});
    }

    void TearSheetFactory::MakeRollingSharpeCharts(
        std::vector<Chart> &lines) const {
        const auto strategySharpe =
            RollingSharpe(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);
        const auto benchmarkSharpe =
            RollingSharpe(m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);

        const LinesDef rollingSharpe{
          {"rollingSharpe", "Rolling Sharpe ratio (6 Months)",
           epoch_core::EpochFolioDashboardWidget::Lines,
           epoch_core::EpochFolioCategory::RiskAnalysis},
          MakeSeriesLines(strategySharpe, benchmarkSharpe, "Sharpe",
                          "Benchmark Sharpe"),
          StraightLines{
              StraightLineDef{"Average Sharpe", strategySharpe.mean(), false},
              kStraightLineAtZero}};
        lines.emplace_back(rollingSharpe);
    }

    void TearSheetFactory::MakeRollingVolatilityCharts(
        std::vector<Chart> &lines) const {
        auto strategyVol =
            RollingVolatility(m_strategy, 6 * ep::APPROX_BDAYS_PER_MONTH);
        auto benchmarkVol =
            RollingVolatility(m_benchmark, 6 * ep::APPROX_BDAYS_PER_MONTH);

        LinesDef rollingVol{{"rollingVol", "Rolling volatility (6 Months)",
                             epoch_core::EpochFolioDashboardWidget::Lines,
                             epoch_core::EpochFolioCategory::RiskAnalysis},
                            MakeSeriesLines(strategyVol, benchmarkVol, "Volatility",
                                            "Benchmark Volatility"),
                            StraightLines{StraightLineDef{"Average Volatility",
                                                          strategyVol.mean(), false},
                                          kStraightLineAtZero}};
        lines.emplace_back(rollingVol);
    }

    void TearSheetFactory::MakeInterestingDateRangeLineCharts(
        std::vector<Chart> &lines) const {
        for (auto const &[strategy, benchmark] : std::views::zip(
                 m_strategyReturnsInteresting, m_benchmarkReturnsInteresting)) {
            auto event = strategy.first;
            lines.emplace_back(
                LinesDef{{event, event, epoch_core::EpochFolioDashboardWidget::Lines,
                          epoch_core::EpochFolioCategory::StrategyBenchmark},
                         MakeSeriesLines(ep::CumReturns(strategy.second),
                                         ep::CumReturns(benchmark.second),
                                         kStrategyColumnName, kBenchmarkColumnName),
                         StraightLines{kStraightLineAtOne}});
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
        constexpr uint8_t kGroupSize = 4;
        std::vector<CardData> values;
        values.reserve(ep::get_simple_stats().size());

        if (!m_strategy.empty()) {
            auto start = m_strategy.index()->at(0);
            auto end = m_strategy.index()->at(-1);
            values.emplace_back("Start date", start, EpochFolioType::Date, kGroup0);
            values.emplace_back("End date", end, EpochFolioType::Date, kGroup0);

            auto months = start.dt().months_between(end).cast_int32();
            values.emplace_back("Total months", months, EpochFolioType::Integer,
                                kGroup0);

            for (auto const &[stat, func] : ep::get_simple_stats()) {
                auto scalar = func(m_strategy);
                auto type = EpochFolioType::Decimal;
                if (pct_fns.contains(ep::get_stat_name(stat))) {
                    scalar *= 100;
                    type = EpochFolioType::Percent;
                }
                values.emplace_back(ep::get_stat_name(stat), Scalar{std::move(scalar)},
                                    type, kGroup1);
            }
        }

        if (!positions.empty()) {
            values.emplace_back("Gross Leverage", GrossLeverage(positions).mean(),
                                EpochFolioType::Decimal, kGroup2);
            if (!m_transactions.empty()) {
                values.emplace_back(
                    "Daily Turnover",
                    GetTurnover(positions, m_transactions, turnoverDenominator).mean() *
                        100.0_scalar,
                    EpochFolioType::Percent, kGroup2);
            }
        }

        if (!m_benchmark.empty()) {
            auto merged_returns = epoch_frame::make_dataframe(
                m_strategy.index(), {m_strategy.array(), m_benchmark.array()},
                {kStrategyColumnName, kBenchmarkColumnName});
            for (auto const &[stat, func] : ep::get_factor_stats()) {
                values.emplace_back(ep::get_stat_name(stat),
                                    epoch_frame::Scalar{func(merged_returns)},
                                    EpochFolioType::Decimal, kGroup3);
            }
        }

        return {epoch_core::EpochFolioDashboardWidget::Card,
                epoch_core::EpochFolioCategory::StrategyBenchmark, values,
                kGroupSize};
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
            stressEvents[1].emplace_back(strategy.mean()*hundred);
            stressEvents[2].emplace_back(strategy.min()*hundred);
            stressEvents[3].emplace_back(strategy.max()*hundred);
        }

        auto data = factory::table::make_table(
            stressEvents, {string_field("event"), float64_field("mean"),
                           float64_field("min"), float64_field("max")});

        return {epoch_core::EpochFolioDashboardWidget::DataTable,
                epoch_core::EpochFolioCategory::StrategyBenchmark,
                "Stress Events Analysis",
                ColumnDefs{ColumnDef{"event", "Stress Events",
                                     epoch_core::EpochFolioType::String},
                           {"mean", "Mean", epoch_core::EpochFolioType::Percent},
                           {"min", "Min", epoch_core::EpochFolioType::Percent},
                           {"max", "Max", epoch_core::EpochFolioType::Percent}},
                data};
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

        return {
            EpochFolioDashboardWidget::DataTable, EpochFolioCategory::RiskAnalysis,
            "Worst Drawdown Periods",
            ColumnDefs{
              {"index", "Worst DrawDown Period",
               epoch_core::EpochFolioType::Integer},
              {"netDrawdown", "Net Drawdown", epoch_core::EpochFolioType::Percent},
              {"peakDate", "Peak date", epoch_core::EpochFolioType::Date},
              {"duration", "Duration", epoch_core::EpochFolioType::DayDuration},
              {"valleyDate", "Valley date", epoch_core::EpochFolioType::Date},
              {"recoveryDate", "Recovery date", epoch_core::EpochFolioType::Date}

            },
            factory::table::make_table(
                tableData,
                {int64_field("index"), datetime_field("peakDate"),
                 datetime_field("valleyDate"), datetime_field("recoveryDate"),
                 uint64_field("duration"), float64_field("netDrawdown")})};
    }

    TearSheet TearSheetFactory::MakeStrategyBenchmark(
        TurnoverDenominator turnoverDenominator) const {
        return {
          {MakePerformanceStats(turnoverDenominator)},
          MakeStrategyBenchmarkLineCharts(),
          std::vector<Table>{MakeStressEventTable()},

      };
    }

    void TearSheetFactory::MakeRollingMaxDrawdownCharts(
        std::vector<Chart> &lines, DrawDownTable &drawDownTable,
        int64_t topKDrawDowns) const {
        LinesDef drawdowns{{"drawdowns",
                            std::format("Top {} drawdown periods", topKDrawDowns),
                            epoch_core::EpochFolioDashboardWidget::Lines,
                            epoch_core::EpochFolioCategory::RiskAnalysis},
                           {MakeSeriesLine(m_strategyCumReturns)},
                           StraightLines{kStraightLineAtOne}};
        for (auto const &row : drawDownTable) {
            auto recovery =
                row.recoveryDate.value_or(m_strategy.index()->at(-1).to_date().date());
            drawdowns.xPlotBands.emplace_back(
                Band{Scalar{row.peakDate}, Scalar{recovery}});
        }
        lines.emplace_back(drawdowns);
    }

    void TearSheetFactory::MakeUnderwaterCharts(std::vector<Chart> &lines) const {
        auto underwaterData = epoch_frame::Scalar{100} *
                              GetUnderwaterFromCumReturns(m_strategyCumReturns);

        // Underwater plot is always negative or zero, so max is 0

        LinesDef underwater{
          {"underwater", "Underwater plot",
           epoch_core::EpochFolioDashboardWidget::Area,
           epoch_core::EpochFolioCategory::RiskAnalysis},
          {MakeSeriesLine(underwaterData)}};
        lines.emplace_back(underwater);
    }

    TearSheet TearSheetFactory::MakeRiskAnalysis(int64_t topKDrawDowns) const {
        DrawDownTable drawDownTable;
        auto table = MakeWorstDrawdownTable(topKDrawDowns, drawDownTable);

        std::vector<Chart> lines;
        MakeRollingVolatilityCharts(lines);
        MakeRollingSharpeCharts(lines);
        MakeRollingMaxDrawdownCharts(lines, drawDownTable, topKDrawDowns);
        MakeUnderwaterCharts(lines);

        return {{}, lines, {table}};
    }

    std::unordered_map<std::string, std::string> month_to_string{
        {"1", "Jan"}, {"2", "Feb"},  {"3", "Mar"},  {"4", "Apr"},
        {"5", "May"}, {"6", "Jun"},  {"7", "Jul"},  {"8", "Aug"},
        {"9", "Sep"}, {"10", "Oct"}, {"11", "Nov"}, {"12", "Dec"}};

    HeatMapDef TearSheetFactory::BuildMonthlyReturnsHeatMap() const {
        Scalar hundred_percent{100_scalar};
        auto monthlyReturns =
            ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

        HeatMapDef heatMap{
            .chartDef = {"monthlyReturns", "Monthly returns",
                         epoch_core::EpochFolioDashboardWidget::HeatMap,
                         epoch_core::EpochFolioCategory::ReturnsDistribution,
                         AxisDef{.type = kCategoryAxisType, .label = "Year"},
                         AxisDef{.type = kCategoryAxisType, .label = "Month"}}};

        heatMap.points.reserve(monthlyReturns.size());
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
                heatMap.chartDef.yAxis->categories.push_back(year);
            }

            if (month_map.contains(month)) {
                x = month_map.at(month);
            } else {
                x = month_map.size();
                month_map.emplace(month, x);
                heatMap.chartDef.xAxis->categories.push_back(month_to_string.at(month));
            }

            auto value = monthlyReturns.iloc(i);
            heatMap.points.emplace_back(std::array{
                Scalar{std::move(x)}, Scalar{std::move(y)}, value * hundred_percent});
        }

        return heatMap;
    }

    BarDef TearSheetFactory::BuildAnnualReturnsBar() const {
        auto annualReturns =
            ep::AggregateReturns(m_strategy, EmpyricalPeriods::yearly) * 100_scalar;
        ;
        auto mean = annualReturns.mean();
        auto data = annualReturns.contiguous_array();
        auto categories = annualReturns.index()
                              ->array()
                              .cast(arrow::utf8())
                              .to_vector<std::string>();
        return {.chartDef = {"annualReturns", "Annual returns",
                             EpochFolioDashboardWidget::Bar,
                             EpochFolioCategory::ReturnsDistribution,
                             AxisDef{.type = kLinearAxisType,
                                     .label = "Year",
                                     .categories = categories},
                             MakePercentageAxis("Returns")},
                .data = data,
                .straightLines = {StraightLineDef{"Mean", mean, false}}};
    }

    HistogramDef TearSheetFactory::BuildMonthlyReturnsHistogram() const {
        auto monthlyReturnsTable =
            ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly) * 100_scalar;
        auto mean = monthlyReturnsTable.mean();
        auto data = monthlyReturnsTable.contiguous_array();

        return {.chartDef = {"monthlyReturns", "Distribution of monthly returns",
                             EpochFolioDashboardWidget::Histogram,
                             EpochFolioCategory::ReturnsDistribution,
                             MakeLinearAxis("Number of Months"),
                             MakePercentageAxis("Monthly Returns")},
                .data = data,
                .straightLines = {StraightLineDef{"Mean", mean, false}},
                .binsCount = 12};
    }

    BoxPlotDef TearSheetFactory::BuildReturnQuantiles() const {
        BoxPlotDataPointDef data;
        auto is_weekly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::weekly);
        auto is_monthly = ep::AggregateReturns(m_strategy, EmpyricalPeriods::monthly);

        auto [returns_plot, returns_outliers] = BoxPlotDataPoint::Make(0, m_strategy);
        auto [weekly_plot, weekly_outliers] = BoxPlotDataPoint::Make(1, is_weekly);
        auto [monthly_plot, monthly_outliers] = BoxPlotDataPoint::Make(2, is_monthly);

        data.points = {returns_plot, weekly_plot, monthly_plot};
        data.outliers = chain(returns_outliers, weekly_outliers, monthly_outliers);

        return {.chartDef = {"returnQuantiles", "Return quantiles",
                             EpochFolioDashboardWidget::BoxPlot,
                             EpochFolioCategory::ReturnsDistribution,
                             MakePercentageAxis("Returns"),
                             AxisDef{.type = kCategoryAxisType,
                                     .label = "",
                                     .categories = {"Daily", "Weekly", "Monthly"}}},
                .data = data};
    }

    TearSheet TearSheetFactory::MakeReturnsDistribution() const {
        return {.charts = std::vector<Chart>{
            BuildMonthlyReturnsHeatMap(), BuildAnnualReturnsBar(),
            BuildMonthlyReturnsHistogram(), BuildReturnQuantiles()}};
    }

    void TearSheetFactory::Make(TurnoverDenominator turnoverDenominator,
                                int64_t topKDrawDowns,
                                FullTearSheet &output) const {
        output.strategy_benchmark = MakeStrategyBenchmark(turnoverDenominator);
        output.risk_analysis = MakeRiskAnalysis(topKDrawDowns);
        output.returns_distribution = MakeReturnsDistribution();
    }
} // namespace epoch_folio::returns
