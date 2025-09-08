//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <common/chart_def.h>
#include <common/table_helpers.h>
#include <epoch_core/common_utils.h>
#include <oneapi/tbb/parallel_for.h>

#include "portfolio/round_trip.h"
#include <boost/math/distributions/beta.hpp>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>

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

XRangeDef
TearSheetFactory::MakeXRangeDef(epoch_frame::DataFrame const &trades) const {
  // round_trip_lifetimes
  auto symbol_series = trades["symbol"];
  XRangeDef xrange;

  // Set up chart definition
  auto *chart_def = xrange.mutable_chart_def();
  chart_def->set_id("xrange");
  chart_def->set_title("Round trip lifetimes");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_XRANGE);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);

  // Set up y-axis
  auto *y_axis = chart_def->mutable_y_axis();
  y_axis->set_type(kCategoryAxisType);
  y_axis->set_label("Asset");

  // Set categories and prepare for parallel processing
  auto categories = Array{symbol_series.unique()}.to_vector<std::string>();
  for (const auto &cat : categories) {
    xrange.add_categories(cat);
  }

  // Pre-allocate points vector for the parallel processing
  std::vector<XRangePoint> points_vec(trades.num_rows());

  auto date_range =
      trades[std::vector<std::string>{"open_dt", "close_dt", "long"}];
  tbb::parallel_for(
      tbb::blocked_range<size_t>(0, categories.size()), [&](auto const &range) {
        for (auto i = range.begin(); i != range.end(); ++i) {
          auto symbol = Scalar{categories[i]};
          auto trades_in_sector = date_range.loc(symbol_series == symbol);
          for (int64_t j = 0;
               j < static_cast<int64_t>(trades_in_sector.num_rows()); ++j) {
            auto open_dt = trades_in_sector["open_dt"].iloc(j);
            auto close_dt = trades_in_sector["close_dt"].iloc(j);
            auto long_ = trades_in_sector["long"].iloc(j);
            auto index = trades_in_sector.index()->at(j);

            XRangePoint point;
            *point.mutable_x() = ToProtoScalar(open_dt);
            *point.mutable_x2() = ToProtoScalar(close_dt);
            point.set_y(i);
            point.set_is_long(long_.as_bool());
            points_vec[index.value<uint64_t>().value()] = std::move(point);
          }
        }
      });

  // Add points to the xrange
  for (auto &point : points_vec) {
    *xrange.add_points() = std::move(point);
  }

  return xrange;
}

LinesDef TearSheetFactory::MakeProbProfitChart(
    epoch_frame::DataFrame const &trades) const {
  LinesDef prob_profit_chart;

  // Set up chart definition
  auto *chart_def = prob_profit_chart.mutable_chart_def();
  chart_def->set_id("prob_profit_trade");
  chart_def->set_title("Probability of making a profitable decision");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);
  *chart_def->mutable_y_axis() = MakeLinearAxis("Belief");
  *chart_def->mutable_x_axis() = MakeLinearAxis("Probability");

  constexpr double kMaxPoints = 500;
  auto x = linspace(0.0, 1.0, kMaxPoints);
  auto profitable = trades["pnl"] > Scalar{0.0};

  const auto alpha = profitable.sum().cast_double().as_double();
  const auto beta = (!profitable).sum().cast_double().as_double();
  if (alpha == 0.0 || beta == 0.0) {
    SPDLOG_WARN("No profitable trades found, skipping prob profit chart");
    return prob_profit_chart;
  }

  const boost::math::beta_distribution dist(alpha, beta);

  std::vector<double> y(x.size());
  std::transform(x.begin(), x.end(), x.begin(), y.begin(),
                 [&](double x_, double &x_out) {
                   auto y_ = pdf(dist, x_);
                   x_out *= 100.0;
                   return y_;
                 });

  *prob_profit_chart.add_lines() = MakeSeriesLine(x, y, "Probability");

  auto *straight_line1 = prob_profit_chart.add_straight_lines();
  straight_line1->set_title("2.5%");
  *straight_line1->mutable_value() =
      ToProtoScalar(Scalar{quantile(dist, 0.025) * 100.0});
  straight_line1->set_vertical(true);

  auto *straight_line2 = prob_profit_chart.add_straight_lines();
  straight_line2->set_title("97.5%");
  *straight_line2->mutable_value() =
      ToProtoScalar(Scalar{quantile(dist, 0.975) * 100.0});
  straight_line2->set_vertical(true);

  return prob_profit_chart;
}

HistogramDef TearSheetFactory::MakeHoldingTimeChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef histogram_def;

  // Set up chart definition
  auto *chart_def = histogram_def.mutable_chart_def();
  chart_def->set_id("holding_time");
  chart_def->set_title("Holding time in days");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_HISTOGRAM);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);

  // Set data
  auto data_series = trades["duration"]
                         .cast(arrow::timestamp(arrow::TimeUnit::NANO))
                         .dt()
                         .floor(arrow::compute::RoundTemporalOptions{})
                         .cast(arrow::int64());
  *histogram_def.mutable_data() =
      MakeArrayFromArrow(data_series.as_chunked_array());

  return histogram_def;
}

HistogramDef TearSheetFactory::MakePnlPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef histogram_def;

  // Set up chart definition
  auto *chart_def = histogram_def.mutable_chart_def();
  chart_def->set_id("pnl_per_round_trip");
  chart_def->set_title("PnL per round trip in dollars");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_HISTOGRAM);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);

  // Set data
  *histogram_def.mutable_data() = MakeArrayFromArrow(trades["pnl"].array());

  return histogram_def;
}

HistogramDef TearSheetFactory::MakeReturnsPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef histogram_def;

  // Set up chart definition
  auto *chart_def = histogram_def.mutable_chart_def();
  chart_def->set_id("returns_per_round_trip");
  chart_def->set_title("Returns per round trip in dollars");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_HISTOGRAM);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);

  // Set data
  *histogram_def.mutable_data() =
      MakeArrayFromArrow((trades["returns"] * Scalar{100.0}).array());

  return histogram_def;
}

void TearSheetFactory::Make(FullTearSheet &output) const {
  try {
    auto trades = ExtractRoundTrips();

    if (trades.num_rows() == 0) {
      SPDLOG_WARN("No trades found, skipping round trip tear sheet");
      output.round_trip = TearSheet{};
      return;
    }

    std::vector<Table> tables;
    try {
      tables = GetRoundTripStats(trades);
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to get round trip stats: {}", e.what());
    }

    std::vector<Chart> charts;

    try {
      Chart chart;
      *chart.mutable_pie_def() = MakeProfitabilityPieChart(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create profitability pie chart: {}", e.what());
    }

    try {
      Chart chart;
      *chart.mutable_x_range_def() = MakeXRangeDef(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create x-range chart: {}", e.what());
    }

    try {
      Chart chart;
      *chart.mutable_lines_def() = MakeProbProfitChart(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create probability profit chart: {}", e.what());
    }

    try {
      Chart chart;
      *chart.mutable_histogram_def() = MakeHoldingTimeChart(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create holding time chart: {}", e.what());
    }

    try {
      Chart chart;
      *chart.mutable_histogram_def() = MakePnlPerRoundTripDollarsChart(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create PnL per round trip chart: {}", e.what());
    }

    try {
      Chart chart;
      *chart.mutable_histogram_def() =
          MakeReturnsPerRoundTripDollarsChart(trades);
      charts.push_back(std::move(chart));
    } catch (std::exception const &e) {
      SPDLOG_ERROR("Failed to create returns per round trip chart: {}",
                   e.what());
    }

    TearSheet tear_sheet;
    for (auto &chart : charts) {
      tear_sheet.charts.push_back(std::move(chart));
    }
    for (auto &table : tables) {
      tear_sheet.tables.push_back(std::move(table));
    }
    output.round_trip = std::move(tear_sheet);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create round trip tearsheet: {}", e.what());
    output.round_trip = TearSheet{};
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

PieDef TearSheetFactory::MakeProfitabilityPieChart(
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

  PieDataDef profit_attr_data;
  profit_attr_data.set_name("Asset");
  profit_attr_data.set_size("80%");
  profit_attr_data.set_inner_size("60%");

  for (int64_t i = 0; i < static_cast<int64_t>(profit_attr.size()); ++i) {
    auto asset = profit_attr.index()->at(i);
    auto profit = profit_attr.iloc(i) * 100_scalar;

    auto *point = profit_attr_data.add_points();
    point->set_name(asset.repr());
    *point->mutable_y() = ToProtoScalar(profit);
  }

  PieDataDef sector_profit_attr_data;
  sector_profit_attr_data.set_name("Sector");
  sector_profit_attr_data.set_size("45%");

  for (int64_t i = 0; i < static_cast<int64_t>(sector_profit_attr.size());
       ++i) {
    auto sector = sector_profit_attr.index()->at(i);
    auto profit = sector_profit_attr.iloc(i) * 100_scalar;

    auto *point = sector_profit_attr_data.add_points();
    point->set_name(sector.repr());
    *point->mutable_y() = ToProtoScalar(profit);
  }

  PieDef pie_def;

  // Set up chart definition
  auto *chart_def = pie_def.mutable_chart_def();
  chart_def->set_id("profitability_pie");
  chart_def->set_title("Profitability (PnL / PnL total)");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_PIE);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_ROUND_TRIP);

  // Add the pie data
  *pie_def.add_data() = std::move(profit_attr_data);
  *pie_def.add_data() = std::move(sector_profit_attr_data);

  return pie_def;
}
} // namespace epoch_folio::round_trip