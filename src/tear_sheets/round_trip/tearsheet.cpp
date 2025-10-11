//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include "epoch_folio/tearsheet.h"
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <epoch_core/common_utils.h>
#include <oneapi/tbb/parallel_for.h>

#include "portfolio/round_trip.h"
#include <boost/math/distributions/beta.hpp>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_dashboard/tearsheet/numeric_lines_chart_builder.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_folio;
namespace ac = arrow::acero;

namespace epoch_folio::round_trip {
std::vector<double> linspace(double start, double end, int64_t num,
                             bool endPoint = true) {
  std::vector<double> result(num);
  double step;

  if (endPoint) {
    step = (end - start) / static_cast<double>(num - 1);
  } else {
    step = (end - start) / static_cast<double>(num);
  }

  for (int i = 0; i < num; ++i) {
    result[i] = start + i * step;
  }

  // If endpoint is false and there are at least 2 elements, make sure the last
  // element is not equal to end.
  if (!endPoint && num >= 2) {
    result[num - 1] = result[num - 2] + step;
  }

  return result;
}

TearSheetFactory::TearSheetFactory(epoch_frame::DataFrame round_trip,
                                   epoch_frame::Series returns,
                                   epoch_frame::DataFrame positions,
                                   SectorMapping sector_mapping)
    : m_round_trip(std::move(round_trip)), m_returns(std::move(returns)),
      m_positions(std::move(positions)),
      m_sector_mapping(std::move(sector_mapping)) {}

epoch_proto::Chart
TearSheetFactory::MakeXRangeDef(epoch_frame::DataFrame const &trades) const {
  // round_trip_lifetimes
  auto symbol_series = trades["symbol"];

  epoch_tearsheet::XRangeChartBuilder builder;
  builder.setId("xrange")
      .setTitle("Round trip lifetimes")
      .setCategory(epoch_folio::categories::RoundTripAnalysis)
      .setYAxisType(epoch_proto::AxisCategory)
      .setYAxisLabel("Asset");

  // Set categories and prepare for parallel processing
  auto categories = Array{symbol_series.unique()}.to_vector<std::string>();
  for (const auto &cat : categories) {
    builder.addYCategory(cat);
  }

  auto date_range =
      trades[std::vector<std::string>{"open_dt", "close_dt", "long"}];

  // Check that open_dt and close_dt are timestamps once before the loop
  auto open_dt_type = date_range["open_dt"].dtype();
  auto close_dt_type = date_range["close_dt"].dtype();
  if (open_dt_type->id() != arrow::Type::TIMESTAMP) {
    throw std::runtime_error("XRange open_dt must be timestamp type, got: " +
                             open_dt_type->ToString());
  }
  if (close_dt_type->id() != arrow::Type::TIMESTAMP) {
    throw std::runtime_error("XRange close_dt must be timestamp type, got: " +
                             close_dt_type->ToString());
  }

  // Process sequentially to avoid thread safety issues with builder
  for (size_t i = 0; i < categories.size(); ++i) {
    auto symbol = Scalar{categories[i]};
    auto trades_in_sector = date_range.loc(symbol_series == symbol);
    for (int64_t j = 0;
         j < static_cast<int64_t>(trades_in_sector.num_rows()); ++j) {
      auto open_dt = trades_in_sector["open_dt"].iloc(j);
      auto close_dt = trades_in_sector["close_dt"].iloc(j);
      auto long_ = trades_in_sector["long"].iloc(j);

      // Skip if open_dt is null (no valid trade start)
      if (open_dt.is_null()) {
        continue;
      }

      // Skip if close_dt is null (position still open, not a completed round trip)
      if (close_dt.is_null()) {
        continue;
      }

      // Convert timestamps to ms (we know they are timestamps from the
      // check above)
      auto open_ts = std::static_pointer_cast<arrow::TimestampScalar>(
          open_dt.value());
      auto close_ts = std::static_pointer_cast<arrow::TimestampScalar>(
          close_dt.value());

      auto open_ms = open_ts->value / 1000000;  // Convert ns to ms
      auto close_ms = close_ts->value / 1000000; // Convert ns to ms

      // Skip if the range is invalid (open >= close or close is 0)
      if (close_ms == 0 || open_ms >= close_ms) {
        continue;
      }

      builder.addPoint(open_ms, close_ms, i, long_.as_bool());
    }
  }

  return builder.build();
}

epoch_proto::Chart TearSheetFactory::MakeProbProfitChart(
    epoch_frame::DataFrame const &trades) const {
  constexpr double kMaxPoints = 500;
  auto x = linspace(0.0, 1.0, kMaxPoints);
  auto profitable = trades["pnl"] > Scalar{0.0};

  const auto alpha = profitable.sum().cast_double().as_double();
  const auto beta = (!profitable).sum().cast_double().as_double();
  if (alpha == 0.0 || beta == 0.0) {
    SPDLOG_WARN("No profitable trades found, skipping prob profit chart");
    epoch_tearsheet::NumericLinesChartBuilder builder;
    builder.setId("prob_profit_trade")
        .setTitle("Probability of making a profitable decision")
        .setCategory(epoch_folio::categories::RoundTripPerformance);
    return builder.build();
  }

  const boost::math::beta_distribution dist(alpha, beta);

  std::vector<double> y(x.size());
  std::transform(x.begin(), x.end(), x.begin(), y.begin(),
                 [&](double x_, double &x_out) {
                   auto y_ = pdf(dist, x_);
                   x_out *= 100.0;
                   return y_;
                 });

  epoch_tearsheet::NumericLineBuilder line_builder;
  for (size_t i = 0; i < x.size(); ++i) {
    line_builder.addPoint(x[i], y[i]);
  }
  line_builder.setName("Probability");

  epoch_proto::StraightLineDef straight_line1;
  straight_line1.set_title("2.5%");
  straight_line1.set_value(quantile(dist, 0.025) * 100.0);
  straight_line1.set_vertical(true);

  epoch_proto::StraightLineDef straight_line2;
  straight_line2.set_title("97.5%");
  straight_line2.set_value(quantile(dist, 0.975) * 100.0);
  straight_line2.set_vertical(true);

  epoch_tearsheet::NumericLinesChartBuilder builder;
  return builder.setId("prob_profit_trade")
      .setTitle("Probability of making a profitable decision")
      .setCategory(epoch_folio::categories::RoundTripPerformance)
      .setYAxisLabel("Belief")
      .setXAxisLabel("Probability")
      .setXAxisType(epoch_proto::AxisLinear)
      .setYAxisType(epoch_proto::AxisLinear)
      .addLine(line_builder.build())
      .addStraightLine(straight_line1)
      .addStraightLine(straight_line2)
      .build();
}

epoch_proto::Chart TearSheetFactory::MakeHoldingTimeChart(
    epoch_frame::DataFrame const &trades) const {
  // Convert duration from nanoseconds to days
  auto data_series = trades["duration"] / Scalar{86400000000000}; // nanoseconds per day

  return epoch_tearsheet::HistogramChartBuilder()
      .setId("holding_time")
      .setTitle("Holding time in days")
      .setCategory(epoch_folio::categories::RoundTripAnalysis)
      .fromSeries(data_series.cast(arrow::int64()))
      .build();
}

epoch_proto::Chart TearSheetFactory::MakePnlPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  return epoch_tearsheet::HistogramChartBuilder()
      .setId("pnl_per_round_trip")
      .setTitle("PnL per round trip in dollars")
      .setCategory(epoch_folio::categories::RoundTripAnalysis)
      .fromSeries(trades["pnl"])
      .build();
}

epoch_proto::Chart TearSheetFactory::MakeReturnsPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  return epoch_tearsheet::HistogramChartBuilder()
      .setId("returns_per_round_trip")
      .setTitle("Returns per round trip in dollars")
      .setCategory(epoch_folio::categories::RoundTripAnalysis)
      .fromSeries(trades["returns"] * Scalar{100.0})
      .build();
}

void TearSheetFactory::Make(epoch_tearsheet::DashboardBuilder &output) const {
  try {
    auto trades = ExtractRoundTrips();
    if (trades.num_rows() == 0) {
      SPDLOG_WARN("No trades found, skipping round trip tear sheet");
      return;
    }

    try {
      for (auto const &table : GetRoundTripStats(trades)) {
        output.addTable(table);
      }
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to get round trip stats: {}", e.what());
    }

    try {
      output.addChart(MakeProfitabilityPieChart(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create profitability pie chart: {}", e.what());
    }

    try {
      output.addChart(MakeXRangeDef(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create x-range chart: {}", e.what());
    }

    try {
      output.addChart(MakeProbProfitChart(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create probability profit chart: {}", e.what());
    }

    try {
      output.addChart(MakeHoldingTimeChart(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create holding time chart: {}", e.what());
    }

    try {
      output.addChart(MakePnlPerRoundTripDollarsChart(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create PnL per round trip chart: {}", e.what());
    }

    try {
      output.addChart(MakeReturnsPerRoundTripDollarsChart(trades));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create returns per round trip chart: {}",
                   e.what());
    }

  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create round trip tearsheet: {}", e.what());
  }
}

DataFrame TearSheetFactory::ExtractRoundTrips() const {

  Series open_dt = m_round_trip["open_datetime"];
  auto close_dt = m_round_trip["close_datetime"];
  auto is_long = m_round_trip["side"] == "Long"_scalar;
  auto symbol = m_round_trip["asset"];
  auto pnl = m_round_trip["net_return"];
  auto duration = open_dt.dt().nanoseconds_between(close_dt.contiguous_array());

  auto portfolio_value =
      m_positions.sum(AxisType::Column) / (1.0_scalar + m_returns);
  auto pv_date = portfolio_value.index()->as_chunked_array();
  auto pv_table = arrow::Table::Make(
      arrow::schema({
          arrow::field("portfolio_value", portfolio_value.array()->type()),
          arrow::field("date", pv_date->type()),
      }),
      {portfolio_value.array(), pv_date});

  auto rt_date = close_dt.dt().normalize().as_chunked_array();
  auto round_trip_table = arrow::Table::Make(
      arrow::schema({
          arrow::field("open_dt", open_dt.array()->type()),
          arrow::field("close_dt", close_dt.array()->type()),
          arrow::field("long", is_long.array()->type()),
          arrow::field("symbol", symbol.array()->type()),
          arrow::field("duration", duration->type()),
          arrow::field("pnl", pnl.array()->type()),
          arrow::field("date", rt_date->type()),
      }),
      {open_dt.array(), close_dt.array(), is_long.array(), symbol.array(),
       std::make_shared<arrow::ChunkedArray>(duration.value()), pnl.array(),
       rt_date});

  ac::Declaration left{"table_source",
                       ac::TableSourceNodeOptions(round_trip_table)};
  ac::Declaration right{"table_source", ac::TableSourceNodeOptions(pv_table)};

  ac::HashJoinNodeOptions join_opts{
      ac::JoinType::LEFT_OUTER,
      /*left_keys=*/{"date"},
      /*right_keys=*/{"date"},
      /*null_handling=*/arrow::compute::literal(true),
      /*left_suffix=*/"_",
      /*right_suffix=*/""};

  ac::Declaration join{"hashjoin", {left, right}, join_opts};
  auto tmp = AssertTableResultIsOk(ac::DeclarationToTable(join));

  auto returns = AssertArrayResultIsOk(arrow::compute::Divide(
      tmp->GetColumnByName("pnl"), tmp->GetColumnByName("portfolio_value")));
  tmp = AssertTableResultIsOk(tmp->AddColumn(
      tmp->num_columns(), arrow::field("returns", returns->type()), returns));
  tmp = AssertTableResultIsOk(
      tmp->RemoveColumn(tmp->schema()->GetFieldIndex("date")));

  return make_dataframe(tmp);
}

epoch_proto::Chart TearSheetFactory::MakeProfitabilityPieChart(
    epoch_frame::DataFrame const &trades) const {
  DataFrame profit_attribution = GetProfitAttribution(trades);

  auto sectors =
      profit_attribution.index()->array().map([this](Scalar const &symbol) {
        return Scalar{lookupDefault(m_sector_mapping, symbol.repr(),
                                    std::string("Others"))};
      });
  auto sector_profit_attr =
      profit_attribution.group_by_agg(sectors.as_chunked_array())
          .sum()
          .to_series();
  auto profit_attr = profit_attribution.to_series();

  // Use the builder pattern for cleaner construction
  epoch_tearsheet::PieChartBuilder builder;
  builder.setId("profitability_pie")
      .setTitle("Profitability (PnL / PnL total)")
      .setCategory(epoch_folio::categories::RoundTripPerformance);

  // Create pie data from the profit attribution series
  std::vector<epoch_proto::PieData> asset_data;
  for (int64_t i = 0; i < static_cast<int64_t>(profit_attr.size()); ++i) {
    epoch_proto::PieData data;
    data.set_name(profit_attr.index()->at(i).repr());
    data.set_y(profit_attr.iloc(i).cast_double().as_double() * 100.0);
    asset_data.push_back(std::move(data));
  }

  // Add asset series (outer ring)
  builder.addSeries("Asset", asset_data, epoch_tearsheet::PieSize{80},
                    epoch_tearsheet::PieInnerSize{60});

  // Create pie data from the sector attribution series
  std::vector<epoch_proto::PieData> sector_data;
  for (int64_t i = 0; i < static_cast<int64_t>(sector_profit_attr.size()); ++i) {
    epoch_proto::PieData data;
    data.set_name(sector_profit_attr.index()->at(i).repr());
    data.set_y(sector_profit_attr.iloc(i).cast_double().as_double() *
                   100.0);
    sector_data.push_back(std::move(data));
  }

  // Add sector series (inner ring)
  builder.addSeries("Sector", sector_data, epoch_tearsheet::PieSize{45},
                    epoch_tearsheet::PieInnerSize{0});

  return builder.build();
}
} // namespace epoch_folio::round_trip