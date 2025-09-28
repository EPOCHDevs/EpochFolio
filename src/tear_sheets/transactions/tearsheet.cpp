//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include "epoch_dashboard/tearsheet/dashboard_builders.h"
#include "epoch_folio/tearsheet.h"
#include "portfolio/txn.h"
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <spdlog/spdlog.h>

using namespace epoch_core;
using namespace epoch_frame;

namespace epoch_folio::txn {
TearSheetFactory::TearSheetFactory(epoch_frame::Series returns,
                                   epoch_frame::DataFrame positions,
                                   epoch_frame::DataFrame transactions)
    : m_returns(std::move(returns)), m_positions(std::move(positions)),
      m_transactions(std::move(transactions)) {}

epoch_proto::Chart
TearSheetFactory::MakeTurnoverOverTimeChart(Series const &turnover) const {
  try {
    auto turnoverByMonth =
        turnover.resample_by_agg({factory::offset::month_end(1)}).mean();
    auto turnoverMean = turnover.mean();

    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("turnoverOverTime")
        .setTitle("Daily turnover")
        .setCategory(epoch_folio::categories::Transactions);

    // Add daily turnover line
    epoch_tearsheet::LineBuilder dailyLineBuilder;
    dailyLineBuilder.setName("Daily turnover").fromSeries(turnover);
    builder.addLine(dailyLineBuilder.build());

    // Add monthly average line
    epoch_tearsheet::LineBuilder monthlyLineBuilder;
    monthlyLineBuilder.setName("Average daily turnover, by month")
        .fromSeries(turnoverByMonth);
    builder.addLine(monthlyLineBuilder.build());

    // Add straight line for overall average
    epoch_proto::StraightLineDef straightLine;
    straightLine.set_title("Average daily turnover, net");
    straightLine.set_value(turnoverMean.cast_double().as_double());
    straightLine.set_vertical(false);
    builder.addStraightLine(straightLine);

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeTurnoverOverTimeChart: {}", e.what());
    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("turnoverOverTime")
        .setTitle("Daily turnover")
        .setCategory(epoch_folio::categories::Transactions);
    return builder.build();
  }
}

epoch_proto::Chart TearSheetFactory::MakeDailyVolumeChart() const {
  try {
    auto dailyTransactions = GetTransactionVolume(m_transactions)["txn_shares"];

    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("dailyVolume")
        .setTitle("Daily transaction volume")
        .setCategory(epoch_folio::categories::Transactions)
        .setXAxisLabel("Amount of shares traded");

    epoch_tearsheet::LineBuilder lineBuilder;
    lineBuilder.setName("dailyVolume").fromSeries(dailyTransactions.cast(arrow::float64()));
    builder.addLine(lineBuilder.build());

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeDailyVolumeChart: {}", e.what());
    epoch_tearsheet::LinesChartBuilder builder;
    builder.setId("dailyVolume")
        .setTitle("Daily transaction volume")
        .setCategory(epoch_folio::categories::Transactions);
    return builder.build();
  }
}

epoch_proto::Chart
TearSheetFactory::MakeDailyTurnoverHistogram(Series const &turnover) const {
  try {
    // Convert turnover to percentage for display
    auto turnoverPct = turnover * Scalar{100.0};

    return epoch_tearsheet::HistogramChartBuilder()
        .setId("dailyTurnoverHistogram")
        .setTitle("Daily turnover histogram")
        .setCategory(epoch_folio::categories::Transactions)
        .setYAxisLabel("Frequency")
        .setXAxisLabel("Daily Turnover")
        .fromSeries(turnoverPct)
        .build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeDailyTurnoverHistogram: {}", e.what());
    return epoch_tearsheet::HistogramChartBuilder()
        .setId("dailyTurnoverHistogram")
        .setTitle("Daily turnover histogram")
        .setCategory(epoch_folio::categories::Transactions)
        .build();
  }
}

epoch_proto::Chart TearSheetFactory::MakeTransactionTimeHistogram(
    size_t binSize, std::string const &timezone) const {
  try {
    auto timestamp = m_transactions.index()->tz_convert(timezone);
    auto trade_value =
        (m_transactions["amount"] * m_transactions["price"]).abs();

    // Convert timestamps to minutes from midnight (like Python)
    auto minutes_index = timestamp->array().map([](Scalar const &s) {
      const auto dt = s.dt();
      return (dt.hour() * Scalar{60} + dt.minute()).cast_uint64();
    });

    // Group by minutes and sum trade values
    auto trade_value_df = trade_value.to_frame("txn_value")
                              .group_by_agg(minutes_index.as_chunked_array())
                              .sum();

    // Reindex to full day (0-1440 minutes = 00:00 - 24:00)
    auto full_day_index = factory::index::from_range(0, 1440, 1);
    trade_value_df = trade_value_df.reindex(full_day_index);

    // Apply binning: (minutes / binSize) * binSize (like Python)
    auto binned_index = trade_value_df.index()->array().map([binSize](Scalar const &s) {
      auto minutes = s.value<uint64_t>().value();
      auto binned_minutes = (minutes / binSize) * binSize;
      return Scalar{binned_minutes};
    });

    // Group by binned minutes and sum
    trade_value_df = trade_value_df.group_by_agg(binned_index.as_chunked_array()).sum();

    // Create time string labels for categories
    auto time_labels = trade_value_df.index()->array().map([](Scalar const &s) {
      auto minutes = s.value<uint64_t>().value();
      auto [hr, min] = std::div(static_cast<int32_t>(minutes), 60);
      return Scalar{std::format("{:02d}:{:02d}", hr, min)};
    });

    auto trade_value_series = trade_value_df.to_series();
    auto trade_value_sum = trade_value_series.sum();

    // Normalize to proportions (not percentage)
    trade_value_series = trade_value_series.fillnull(Scalar{0}) / trade_value_sum;

    // Set up the chart with time string categories
    epoch_tearsheet::BarChartBuilder builder;
    builder.setId("transactionTimeHistogram")
        .setTitle("Transaction time distribution")
        .setCategory(epoch_folio::categories::Transactions)
        .setYAxisLabel("Proportion")
        .setXAxisLabel("Time")
        .setXAxisType(epoch_proto::AxisCategory)
        .setVertical(true); // Force vertical bars (columns)

    // Extract time labels for X-axis categories
    std::vector<std::string> categories;
    for (size_t i = 0; i < trade_value_series.size(); ++i) {
      auto minutes = trade_value_df.index()->at(i).value<uint64_t>().value();
      auto [hr, min] = std::div(static_cast<int32_t>(minutes), 60);
      categories.push_back(std::format("{:02d}:{:02d}", hr, min));
    }
    builder.setXAxisCategories(categories);

    // Use the proportion values (not percentage)
    builder.fromSeries(trade_value_series)
        .setBarWidth(static_cast<double>(binSize));

    return builder.build();
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Exception in MakeTransactionTimeHistogram: {}", e.what());
    epoch_tearsheet::BarChartBuilder builder;
    builder.setId("transactionTimeHistogram")
        .setTitle("Transaction time distribution")
        .setCategory(epoch_folio::categories::Transactions)
        .setVertical(true);
    return builder.build();
  }
}

void TearSheetFactory::Make(epoch_core::TurnoverDenominator turnoverDenominator,
                            size_t binSize, std::string const &timezone,
                            epoch_tearsheet::DashboardBuilder &output) const {
  try {
    auto df_turnover =
        GetTurnover(m_positions, m_transactions, turnoverDenominator);

    try {
      output.addChart(MakeTurnoverOverTimeChart(df_turnover));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create turnover over time chart: {}", e.what());
    }

    try {
      output.addChart(MakeDailyVolumeChart());
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create daily volume chart: {}", e.what());
    }

    try {
      output.addChart(MakeDailyTurnoverHistogram(df_turnover));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create daily turnover histogram: {}", e.what());
    }

    try {
      output.addChart(MakeTransactionTimeHistogram(binSize, timezone));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create transaction time histogram: {}", e.what());
    }

  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create transactions tearsheet: {}", e.what());
  }
}
} // namespace epoch_folio::txn