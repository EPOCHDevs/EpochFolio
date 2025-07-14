//
// Created by adesola on 3/30/25.
//

#include "tearsheet.h"
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <epoch_core/common_utils.h>
#include <models/chart_def.h>
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
  xrange.chartDef =
      ChartDef{"xrange",
               "Round trip lifetimes",
               EpochFolioDashboardWidget::XRange,
               EpochFolioCategory::RoundTrip,
               std::nullopt, // Default x-axis (datetime)
               AxisDef{.type = kCategoryAxisType, .label = "Asset"}};
  xrange.categories = Array{symbol_series.unique()}.to_vector<std::string>();
  xrange.points.resize(trades.num_rows());

  auto date_range =
      trades[std::vector<std::string>{"open_dt", "close_dt", "long"}];
  tbb::parallel_for(
      tbb::blocked_range<size_t>(0, xrange.categories.size()),
      [&](auto const &range) {
        for (auto i = range.begin(); i != range.end(); ++i) {
          auto symbol = Scalar{xrange.categories[i]};
          auto trades_in_sector = date_range.loc(symbol_series == symbol);
          for (int64_t j = 0;
               j < static_cast<int64_t>(trades_in_sector.num_rows()); ++j) {
            auto open_dt = trades_in_sector["open_dt"].iloc(j);
            auto close_dt = trades_in_sector["close_dt"].iloc(j);
            auto long_ = trades_in_sector["long"].iloc(j);
            auto index = trades_in_sector.index()->at(j);
            xrange.points[index.value<uint64_t>().value()] =
                XRangePoint{open_dt, close_dt, i, long_.as_bool()};
          }
        }
      });
  return xrange;
}

LinesDef TearSheetFactory::MakeProbProfitChart(
    epoch_frame::DataFrame const &trades) const {
  LinesDef prob_profit_chart{
      .chartDef = ChartDef{
          "prob_profit_trade", "Probability of making a profitable decision",
          EpochFolioDashboardWidget::Lines, EpochFolioCategory::RoundTrip,
          MakeLinearAxis("Probability Density")}};

  constexpr double kMaxPoints = 500;
  auto x = linspace(0.0, 1.0, kMaxPoints);
  auto profitable = trades["pnl"] > 0_scalar;

  const auto alpha = profitable.sum().cast_double().as_double();
  const auto beta = (!profitable).sum().cast_double().as_double();
  if (alpha == 0.0 || beta == 0.0) {
    SPDLOG_WARN("No profitable trades found, skipping prob profit chart");
    return prob_profit_chart;
  }

  const boost::math::beta_distribution dist(alpha, beta);

  std::vector<double> y(x.size());
  std::transform(x.begin(), x.end(), y.begin(),
                 [&](double x_) { return pdf(dist, x_); });

  auto points =
      std::views::iota(0, kMaxPoints) | epoch_core::ranges::to_vector_v;
  prob_profit_chart.lines.emplace_back(MakeSeriesLine(points, y));

  prob_profit_chart.straightLines.emplace_back(
      "2.5%", Scalar{quantile(dist, 0.025)}, true);
  prob_profit_chart.straightLines.emplace_back(
      "97.5%", Scalar{quantile(dist, 0.975)}, true);

  return prob_profit_chart;
}

HistogramDef TearSheetFactory::MakeHoldingTimeChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef holding_time_chart{
      .chartDef = ChartDef{"holding_time", "Holding time in days",
                           EpochFolioDashboardWidget::Histogram,
                           EpochFolioCategory::RoundTrip},
      .data = trades["duration"]
                  .cast(arrow::timestamp(arrow::TimeUnit::NANO))
                  .dt()
                  .floor(arrow::compute::RoundTemporalOptions{})
                  .cast(arrow::int64())};
  return holding_time_chart;
}

HistogramDef TearSheetFactory::MakePnlPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef pnl_per_round_trip_dollars_chart{
      .chartDef = ChartDef{"", "PnL per round trip in dollars",
                           EpochFolioDashboardWidget::Histogram,
                           EpochFolioCategory::RoundTrip},
      .data = trades["pnl"].contiguous_array()};
  return pnl_per_round_trip_dollars_chart;
}

HistogramDef TearSheetFactory::MakeReturnsPerRoundTripDollarsChart(
    epoch_frame::DataFrame const &trades) const {
  HistogramDef returns_per_round_trip_dollars_chart{
      .chartDef = ChartDef{"", "Returns per round trip in dollars",
                           EpochFolioDashboardWidget::Histogram,
                           EpochFolioCategory::RoundTrip},
      .data = trades["returns"].contiguous_array() * 100_scalar};
  return returns_per_round_trip_dollars_chart;
}

void TearSheetFactory::Make(FullTearSheet &output) const {
  auto trades = ExtractRoundTrips();

  if (trades.num_rows() == 0) {
    SPDLOG_WARN("No trades found, skipping round trip tear sheet");
    return;
  }

  std::vector<Table> tables = GetRoundTripStats(trades);
  auto pie_chart = MakeProfitabilityPieChart(trades);
  auto xrange = MakeXRangeDef(trades);
  auto prob_profit_trade = MakeProbProfitChart(trades);
  auto holding_time = MakeHoldingTimeChart(trades);
  auto pnl_per_round_trip_dollars = MakePnlPerRoundTripDollarsChart(trades);
  auto returns_per_round_trip_dollars =
      MakeReturnsPerRoundTripDollarsChart(trades);

  output.round_trip = TearSheet{
      .cards = {},
      .charts = std::vector<Chart>{pie_chart, xrange, prob_profit_trade,
                                   holding_time, pnl_per_round_trip_dollars,
                                   returns_per_round_trip_dollars},
      .tables = tables};
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
                                    std::string{"Others"})};
      });
  auto sector_profit_attr =
      profit_attribution.group_by_agg(sectors.as_chunked_array())
          .sum()
          .to_series();
  auto profit_attr = profit_attribution.to_series();

  PieDataDef profit_attr_data{"Asset", {}, "80%", "60%"};
  profit_attr_data.points.reserve(profit_attr.size());
  for (int64_t i = 0; i < static_cast<int64_t>(profit_attr.size()); ++i) {
    auto asset = profit_attr.index()->at(i);
    auto profit = profit_attr.iloc(i) * 100_scalar;
    profit_attr_data.points.emplace_back(asset.repr(), profit);
  }

  PieDataDef sector_profit_attr_data{"Sector", {}, "45%", std::nullopt};
  sector_profit_attr_data.points.reserve(sector_profit_attr.size());
  for (int64_t i = 0; i < static_cast<int64_t>(sector_profit_attr.size());
       ++i) {
    auto sector = sector_profit_attr.index()->at(i);
    auto profit = sector_profit_attr.iloc(i) * 100_scalar;
    sector_profit_attr_data.points.emplace_back(sector.repr(), profit);
  }

  return PieDef{ChartDef{"profitability_pie", "Profitability (PnL / PnL total)",
                         EpochFolioDashboardWidget::Pie,
                         EpochFolioCategory::RoundTrip},
                {profit_attr_data, sector_profit_attr_data}};
}
} // namespace epoch_folio::round_trip