//
// Created by adesola on 3/31/25.
//

#include "round_trip.h"
#include "common/chart_def.h"
#include "common/type_helper.h"
#include "epoch_protos/common.pb.h"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <oneapi/tbb/parallel_for.h>

using namespace epoch_frame;

#include "common/table_helpers.h"

namespace epoch_folio {
using AggList = std::vector<
    std::pair<std::string, std::variant<std::function<Scalar(Series const &)>,
                                        std::string>>>;

template <bool is_duration = false>
arrow::TablePtr AggAllLongShort(epoch_frame::DataFrame const &round_trip,
                                std::string const &col,
                                AggList const &stats_dict,
                                bool is_percentage = false) {
  std::vector<Scalar> index(stats_dict.size());
  std::vector<Scalar> all_trades(stats_dict.size());
  std::vector<Scalar> long_trades(stats_dict.size());
  std::vector<Scalar> short_trades(stats_dict.size());

  auto multiplier = is_percentage ? 100.0_scalar : 1.0_scalar;
  tbb::parallel_for(tbb::blocked_range<size_t>(0, stats_dict.size()),
                    [&](tbb::blocked_range<size_t> const &r) {
                      for (size_t i = r.begin(); i != r.end(); ++i) {
                        auto [key, fn] = stats_dict[i];
                        index[i] = Scalar{key};

                        auto all = round_trip[col];
                        auto _long = all.loc(round_trip["long"]);
                        auto _short = all.loc(!round_trip["long"]);

                        std::visit(
                            [&]<typename T>(T const &fn) {
                              if constexpr (std::is_same_v<T, std::string>) {
                                auto all_agg = all.agg(AxisType::Row, fn);
                                auto long_agg = _long.agg(AxisType::Row, fn);
                                auto short_agg = _short.agg(AxisType::Row, fn);

                                if constexpr (is_duration) {
                                  all_trades[i] = all_agg.cast_int64();
                                  long_trades[i] = long_agg.cast_int64();
                                  short_trades[i] = short_agg.cast_int64();
                                } else {
                                  all_trades[i] = all_agg * multiplier;
                                  long_trades[i] = long_agg * multiplier;
                                  short_trades[i] = short_agg * multiplier;
                                }
                              } else {
                                all_trades[i] = fn(all) * multiplier;
                                long_trades[i] = fn(_long) * multiplier;
                                short_trades[i] = fn(_short) * multiplier;
                              }
                            },
                            fn);
                      }
                    });

  arrow::FieldVector fields{string_field("key")};
  fields.reserve(4);
  for (auto const &field : {"all_trades", "long_trades", "short_trades"}) {
    if constexpr (is_duration) {
      fields.emplace_back(int64_field(field));
    } else {
      fields.emplace_back(float64_field(field));
    }
  }
  return factory::table::make_table(
      std::vector{index, all_trades, long_trades, short_trades}, fields);
}

epoch_proto::Table GetSymbolsTable(epoch_frame::DataFrame const &round_trip,
                                   AggList const &stats_dict) {
  auto apply_symbol = [&](std::string const &symbol,
                          epoch_frame::Series const &returns) {
    std::vector<std::string> index(stats_dict.size());
    std::vector<Scalar> all_trades(stats_dict.size());

    std::transform(stats_dict.begin(), stats_dict.end(), index.begin(),
                   all_trades.begin(), [&](auto const &stat, std::string &row) {
                     row = stat.first;
                     return std::visit(
                         [&]<typename T>(T const &fn) {
                           if constexpr (std::is_same_v<T, std::string>) {
                             return returns.agg(AxisType::Row, fn) * 100_scalar;
                           } else {
                             return fn(returns) * 100_scalar;
                           }
                         },
                         stat.second);
                   });

    return make_dataframe(factory::index::make_object_index(index),
                          {all_trades},
                          {arrow::field(symbol, arrow::float64())});
  };

  auto groups = round_trip[std::vector<std::string>{"returns", "symbol"}]
                    .group_by_apply("symbol")
                    .groups();
  epoch_proto::Table result_table;

  std::vector<FrameOrSeries> frames;

  epoch_proto::ColumnDef key_col;
  key_col.set_id("key");
  key_col.set_name("Stats");
  key_col.set_type(epoch_proto::TypeString);
  result_table.add_columns()->CopyFrom(std::move(key_col));

  frames.reserve(groups.size());
  result_table.mutable_columns()->Reserve(groups.size() + 1);

  auto returns = round_trip["returns"];
  for (auto const &[symbol, indexes] : groups) {
    auto symbol_name = symbol.repr();
    frames.emplace_back(
        apply_symbol(symbol_name, returns.iloc(Array{indexes})));
    epoch_proto::ColumnDef symbol_col;
    symbol_col.set_id(symbol_name);
    symbol_col.set_name(symbol_name);
    symbol_col.set_type(epoch_proto::TypePercent);
    result_table.add_columns()->CopyFrom(std::move(symbol_col));
  }
  arrow::TablePtr table;
  if (!frames.empty()) {
    table = concat({.frames = frames, .axis = AxisType::Column})
                .reset_index("key")
                .table();
  }

  result_table.set_type(epoch_proto::WidgetDataTable);
  result_table.set_category("RoundTrip");
  result_table.set_title("Returns by Symbol");

  // Set data
  if (table) {
    auto table_data = MakeTableDataFromArrow(table);
    *result_table.mutable_data() = std::move(table_data);
  }

  return result_table;
}

std::vector<epoch_proto::Table>
GetRoundTripStats(epoch_frame::DataFrame const &round_trip) {
  using epoch_frame::Series;
  static const Scalar ZERO{0.0};

  const AggList PNL_STATS{
      {"Total profit", std::string{"sum"}},
      {"Gross profit",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.loc(x > ZERO).sum(); })},
      {"Gross loss", std::function<Scalar(Series const &)>([](Series const &x) {
         return x.loc(x < ZERO).sum();
       })},
      {"Profit factor",
       std::function<Scalar(Series const &)>([](Series const &x) {
         auto neg_sum = x.loc(x < ZERO).abs().sum();
         if (neg_sum != ZERO)
           return x.loc(x > ZERO).sum() / neg_sum;
         return Scalar();
       })},
      {"Avg. trade net profit", std::string{"mean"}},
      {"Avg. winning trade",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.loc(x > ZERO).mean(); })},
      {"Avg. losing trade",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.loc(x < ZERO).mean(); })},
      {"Ratio Avg. Win:Avg. Loss",
       std::function<Scalar(Series const &)>([](Series const &x) {
         auto neg_mean = x.loc(x < ZERO).abs().mean();
         if (neg_mean != ZERO)
           return x.loc(x > ZERO).mean() / neg_mean;
         return Scalar();
       })},
      {"Largest winning trade", std::string{"max"}},
      {"Largest losing trade", std::string{"min"}}};

  const AggList SUMMARY_STATS{
      {"Total number of round_trips",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.count_valid(); })},
      {"Percent profitable",
       std::function<Scalar(Series const &)>([](Series const &x) {
         return Scalar{static_cast<double>(x.loc(x > ZERO).size()) /
                       static_cast<double>(x.size())};
       })},
      {"Winning round_trips",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return Scalar{x.loc(x > ZERO).size()}; })},
      {"Losing round_trips",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return Scalar{x.loc(x < ZERO).size()}; })},
      {"Even round_trips",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return Scalar{x.loc(x == ZERO).size()}; })}};

  const AggList RETURNS_STATS{
      {"Avg returns all round_trips", std::string{"mean"}},
      {"Avg returns winning",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.loc(x > ZERO).mean(); })},
      {"Avg returns losing",
       std::function<Scalar(Series const &)>(
           [](Series const &x) { return x.loc(x < ZERO).mean(); })},
      {"Median returns all round_trips",
       std::function<Scalar(Series const &)>([](Series const &x) {
         return x.quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       })},
      {"Median returns winning",
       std::function<Scalar(Series const &)>([](Series const &x) {
         return x.loc(x > ZERO).quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       })},
      {"Median returns losing",
       std::function<Scalar(Series const &)>([](Series const &x) {
         return x.loc(x < ZERO).quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       })},
      {"Largest winning trade", std::string{"max"}},
      {"Largest losing trade", std::string{"min"}}};

  const AggList DURATION_STATS{
      {"Avg duration", std::string{"mean"}},
      {"Median duration", std::string{"approximate_median"}},
      {"Longest duration", std::string{"max"}},
      {"Shortest duration", std::string{"min"}}};

  auto pnl = AggAllLongShort(round_trip, "pnl", PNL_STATS);
  auto summary = AggAllLongShort(round_trip, "pnl", SUMMARY_STATS);
  auto duration = AggAllLongShort<true>(round_trip, "duration", DURATION_STATS);
  auto returns = AggAllLongShort(round_trip, "returns", RETURNS_STATS, true);

  auto make_table =
      [&](const std::string &title,
          const std::vector<std::pair<std::string, epoch_proto::EpochFolioType>>
              &cols,
          const std::shared_ptr<arrow::Table> &data) {
        epoch_proto::Table t;
        t.set_type(epoch_proto::WidgetDataTable);
        t.set_category("RoundTrip");
        t.set_title(title);
        for (auto const &c : cols) {
          epoch_proto::ColumnDef cd;
          cd.set_name(c.first);
          cd.set_type(c.second);
          *t.add_columns() = std::move(cd);
        }
        *t.mutable_data() = MakeTableDataFromArrow(data);
        return t;
      };

  std::vector<epoch_proto::Table> out;
  out.reserve(5);

  out.emplace_back(make_table("PnL Statistics",
                              {{"key", epoch_proto::TypeString},
                               {"all_trades", epoch_proto::TypeDecimal},
                               {"long_trades", epoch_proto::TypeDecimal},
                               {"short_trades", epoch_proto::TypeDecimal}},
                              pnl));

  out.emplace_back(make_table("Trade Summary",
                              {{"key", epoch_proto::TypeString},
                               {"all_trades", epoch_proto::TypeDecimal},
                               {"long_trades", epoch_proto::TypeDecimal},
                               {"short_trades", epoch_proto::TypeDecimal}},
                              summary));

  out.emplace_back(make_table("Duration Analysis",
                              {{"key", epoch_proto::TypeString},
                               {"all_trades", epoch_proto::TypeDayDuration},
                               {"long_trades", epoch_proto::TypeDayDuration},
                               {"short_trades", epoch_proto::TypeDayDuration}},
                              duration));

  out.emplace_back(make_table("Return Analysis",
                              {{"key", epoch_proto::TypeString},
                               {"all_trades", epoch_proto::TypePercent},
                               {"long_trades", epoch_proto::TypePercent},
                               {"short_trades", epoch_proto::TypePercent}},
                              returns));

  out.emplace_back(GetSymbolsTable(round_trip, RETURNS_STATS));
  return out;
}

DataFrame GetProfitAttribution(epoch_frame::DataFrame const &round_trip,
                               std::string const &col) {
  const auto total_pnl = round_trip["pnl"].sum();
  return (
      round_trip[std::vector<std::string>{"pnl", col}].group_by_agg(col).sum() /
      total_pnl);
}

} // namespace epoch_folio