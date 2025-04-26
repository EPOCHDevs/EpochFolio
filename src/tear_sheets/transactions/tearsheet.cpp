//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include "portfolio/txn.h"
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_frame;

namespace epoch_folio::txn {
TearSheetFactory::TearSheetFactory(epoch_frame::Series returns,
                                   epoch_frame::DataFrame positions,
                                   epoch_frame::DataFrame transactions)
    : m_returns(std::move(returns)), m_positions(std::move(positions)),
      m_transactions(std::move(transactions)) {}

LinesDef
TearSheetFactory::MakeTurnoverOverTimeChart(Series const &turnover) const {
  auto turnoverByMonth =
      turnover.resample_by_agg({factory::offset::month_end(1)}).mean();
  auto turnoverMean = turnover.mean();

  return LinesDef{
      .chartDef = ChartDef{"turnoverOverTime", "Daily turnover",
                           EpochFolioDashboardWidget::Lines,
                           EpochFolioCategory::Transactions},
      .lines = {MakeSeriesLine(turnover, "Daily turnover")},
      .straightLines = {{"Average daily turnover, net", turnoverMean, false}},
      .overlay =
          MakeSeriesLine(turnoverByMonth, "Average daily turnover, by month")};
}

LinesDef TearSheetFactory::MakeDailyVolumeChart() const {
  auto dailyTransactions = GetTransactionVolume(m_transactions)["txn_shares"];
  return LinesDef{
      .chartDef = ChartDef{"dailyVolume", "Daily transaction volume",
                           EpochFolioDashboardWidget::Lines,
                           EpochFolioCategory::Transactions,
                           AxisDef{kLinearAxisType, "Amount of shares traded"}},
      .lines = {MakeSeriesLine(dailyTransactions, "dailyVolume")}};
}

HistogramDef
TearSheetFactory::MakeDailyTurnoverHistogram(Series const &turnover) const {
  return HistogramDef{
      .chartDef = ChartDef{"dailyTurnoverHistogram", "Daily turnover histogram",
                           EpochFolioDashboardWidget::Histogram,
                           EpochFolioCategory::Transactions,
                           AxisDef{kLinearAxisType, "Proportion"},
                           AxisDef{kLinearAxisType}},
      .data = turnover.contiguous_array()};
}

BarDef TearSheetFactory::MakeTransactionTimeHistogram(
    size_t binSize, std::string const &timezone) const {
  auto timestamp = m_transactions.index()->tz_convert(timezone);
  auto trade_value = (m_transactions["amount"] * m_transactions["price"]).abs();

  auto current_index = timestamp->array().map([](Scalar const &s) {
    const auto dt = s.dt();
    return (dt.hour() * Scalar{60} + dt.minute()).cast_uint64();
  });
  auto new_index = factory::index::from_range(0, 1440, binSize);

  auto trade_value_df = trade_value.to_frame("txn_value")
                            .group_by_agg(current_index.as_chunked_array())
                            .sum()
                            .reindex(new_index);

  auto bin_group = trade_value_df.index()->array().map([](Scalar const &s) {
    const auto X = s.value<uint64_t>().value();
    auto [hr, min] = std::div(static_cast<int32_t>(X), 60);
    return Scalar{std::format("{:02d}:{:02d}", hr, min)};
  });

  trade_value = trade_value_df.group_by_agg(bin_group.as_chunked_array())
                    .sum()
                    .to_series();

  auto trade_value_sum = trade_value.sum();
  trade_value = trade_value.fillnull(Scalar{0}) / trade_value_sum;

  return BarDef{
      .chartDef =
          ChartDef{"transactionTimeHistogram", "Transaction time distribution",
                   EpochFolioDashboardWidget::Column,
                   EpochFolioCategory::Transactions,
                   AxisDef{kLinearAxisType, "Proportion"},
                   AxisDef{kLinearAxisType, "Proportion",
                           trade_value.index()->to_vector<std::string>()}},
      .data = trade_value.contiguous_array(),
      .barWidth = binSize};
}

void TearSheetFactory::Make(epoch_core::TurnoverDenominator turnoverDenominator,
                            size_t binSize, std::string const &timezone,
                            FullTearSheet &output) const {
  auto df_turnover =
      GetTurnover(m_positions, m_transactions, turnoverDenominator);

  output.transactions = TearSheet{
      .charts = std::vector<Chart>{
          MakeTurnoverOverTimeChart(df_turnover), MakeDailyVolumeChart(),
          MakeDailyTurnoverHistogram(df_turnover),
          MakeTransactionTimeHistogram(binSize, timezone)}};
}
} // namespace epoch_folio::txn