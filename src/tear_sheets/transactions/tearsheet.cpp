//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include "common/chart_def.h"
#include "common/table_helpers.h"
#include "epoch_folio/tearsheet.h"
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
  // TODO: Implement proper protobuf LinesDef construction

  auto turnoverByMonth =
      turnover.resample_by_agg({factory::offset::month_end(1)}).mean();
  auto turnoverMean = turnover.mean();

  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("turnoverOverTime");
  cd->set_title("Daily turnover");
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::Transactions);

  *out.add_lines() = MakeSeriesLine(turnover, "Daily turnover");
  *out.add_straight_lines() =
      MakeStraightLine("Average daily turnover, net", turnoverMean, false);
  *out.add_lines() =
      MakeSeriesLine(turnoverByMonth, "Average daily turnover, by month");
  return out;
}

LinesDef TearSheetFactory::MakeDailyVolumeChart() const {
  auto dailyTransactions = GetTransactionVolume(m_transactions)["txn_shares"];

  LinesDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("dailyVolume");
  cd->set_title("Daily transaction volume");
  cd->set_type(epoch_proto::WidgetLines);
  cd->set_category(epoch_folio::categories::Transactions);
  *cd->mutable_x_axis() = MakeLinearAxis("Amount of shares traded");
  *out.add_lines() = MakeSeriesLine(dailyTransactions, "dailyVolume");
  return out;
}

HistogramDef
TearSheetFactory::MakeDailyTurnoverHistogram(Series const &turnover) const {
  // Convert turnover to percentage for display
  auto turnoverPct = turnover * Scalar{100.0};

  HistogramDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("dailyTurnoverHistogram");
  cd->set_title("Daily turnover histogram");
  cd->set_type(epoch_proto::WidgetHistogram);
  cd->set_category(epoch_folio::categories::Transactions);
  *cd->mutable_y_axis() = MakeLinearAxis("Frequency");
  *cd->mutable_x_axis() = MakePercentageAxis("Daily Turnover");
  *out.mutable_data() = MakeArrayFromArrow(turnoverPct.array());
  return out;
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

  BarDef out;
  auto *cd = out.mutable_chart_def();
  cd->set_id("transactionTimeHistogram");
  cd->set_title("Transaction time distribution");
  cd->set_type(epoch_proto::WidgetColumn);
  cd->set_category(epoch_folio::categories::Transactions);
  *cd->mutable_y_axis() = MakePercentageAxis("Proportion");
  auto *x_axis = cd->mutable_x_axis();
  x_axis->set_type(epoch_proto::AxisCategory);
  x_axis->set_label("Time");
  auto categories = trade_value.index()->to_vector<std::string>();
  for (const auto &cat : categories) {
    x_axis->add_categories(cat);
  }
  *out.mutable_data() =
      MakeArrayFromArrow((trade_value * Scalar{100.0}).array());
  out.set_bar_width(binSize);
  return out;
}

void TearSheetFactory::Make(epoch_core::TurnoverDenominator turnoverDenominator,
                            size_t binSize, std::string const &timezone,
                            epoch_proto::FullTearSheet &output) const {
  try {
    auto df_turnover =
        GetTurnover(m_positions, m_transactions, turnoverDenominator);

    std::vector<Chart> charts;

    try {
      Chart c;
      *c.mutable_lines_def() = MakeTurnoverOverTimeChart(df_turnover);
      charts.push_back(std::move(c));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create turnover over time chart: {}", e.what());
    }

    try {
      Chart c;
      *c.mutable_lines_def() = MakeDailyVolumeChart();
      charts.push_back(std::move(c));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create daily volume chart: {}", e.what());
    }

    try {
      Chart c;
      *c.mutable_histogram_def() = MakeDailyTurnoverHistogram(df_turnover);
      charts.push_back(std::move(c));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create daily turnover histogram: {}", e.what());
    }

    try {
      Chart c;
      *c.mutable_bar_def() = MakeTransactionTimeHistogram(binSize, timezone);
      charts.push_back(std::move(c));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create transaction time histogram: {}", e.what());
    }

    epoch_proto::TearSheet ts;
    for (auto &chart : charts) {
      *ts.mutable_charts()->add_charts() = std::move(chart);
    }
    (*output.mutable_categories())[epoch_folio::categories::Transactions] =
        std::move(ts);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create transactions tearsheet: {}", e.what());
  }
}
} // namespace epoch_folio::txn