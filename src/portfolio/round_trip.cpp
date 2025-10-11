//
// Created by adesola on 3/31/25.
//

#include "round_trip.h"
#include "common/type_helper.h"
#include "epoch_protos/common.pb.h"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <oneapi/tbb/parallel_for.h>
#include "epoch_dashboard/tearsheet/table_builder.h"
#include "epoch_folio/tearsheet.h"

using namespace epoch_frame;


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

  epoch_tearsheet::TableBuilder builder;
  builder.setType(epoch_proto::WidgetDataTable)
      .setCategory(categories::RoundTripPerformance)
      .setTitle("Returns by Symbol");

  // Add key column
  builder.addColumn("key", "Stats", epoch_proto::TypeString);

  std::vector<FrameOrSeries> frames;
  frames.reserve(groups.size());

  auto returns = round_trip["returns"];
  for (auto const &[symbol, indexes] : groups) {
    auto symbol_name = symbol.repr();
    frames.emplace_back(
        apply_symbol(symbol_name, returns.iloc(Array{indexes})));
    builder.addColumn(symbol_name, symbol_name, epoch_proto::TypePercent);
  }

  if (!frames.empty()) {
    auto table = concat({.frames = frames, .axis = AxisType::Column})
                     .reset_index("key");

    // Build data incrementally using addRow
    // Process columns in the same order they were added to the builder
    std::vector<std::string> ordered_columns;
    ordered_columns.reserve(table.column_names().size());
    ordered_columns.push_back("key"); // First column added

    // Add symbol columns in the same order they were added to the builder
    for (auto const &[symbol, indexes] : groups) {
      ordered_columns.push_back(symbol.repr());
    }

    for (int64_t i = 0; i < static_cast<int64_t>(table.num_rows()); ++i) {
      epoch_proto::TableRow row;
      for (const auto& col_name : ordered_columns) {
        auto scalar = table[col_name].iloc(i);
        if (col_name == "key") {
          *row.add_values() = epoch_tearsheet::ScalarFactory::create(scalar);
        } else {
          // Percentage columns are already scaled by apply_symbol (lines 97,99 multiply by 100_scalar)
          *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(scalar.cast_double().as_double());
        }
      }
      builder.addRow(row);
    }
  }

  return builder.build();
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
        epoch_tearsheet::TableBuilder builder;
        builder.setType(epoch_proto::WidgetDataTable)
            .setCategory(categories::RoundTripPerformance)
            .setTitle(title);

        for (auto const &c : cols) {
          builder.addColumn(c.first, c.first, c.second);
        }

        if (data) {
          auto df = make_dataframe(data);

          // Build data incrementally using addRow
          for (int64_t i = 0; i < static_cast<int64_t>(df.num_rows()); ++i) {
            epoch_proto::TableRow row;
            for (size_t j = 0; j < cols.size(); ++j) {
              auto scalar = df[cols[j].first].iloc(i);

              switch (cols[j].second) {
                case epoch_proto::TypeString:
                  *row.add_values() = epoch_tearsheet::ScalarFactory::create(scalar);
                  break;
                case epoch_proto::TypePercent:
                  // Check if data is already scaled by looking at the calling context
                  *row.add_values() = epoch_tearsheet::ScalarFactory::fromPercentValue(scalar.cast_double().as_double());
                  break;
                case epoch_proto::TypeDuration:
                  // Convert from nanoseconds to milliseconds
                  *row.add_values() = epoch_tearsheet::ScalarFactory::fromDurationMs(static_cast<int64_t>(scalar.cast_double().as_double() / 1000000.0));
                  break;
                case epoch_proto::TypeDecimal:
                default:
                  *row.add_values() = epoch_tearsheet::ScalarFactory::fromDecimal(scalar.cast_double().as_double());
                  break;
              }
            }
            builder.addRow(row);
          }
        }

        return builder.build();
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
                               {"all_trades", epoch_proto::TypeDuration},
                               {"long_trades", epoch_proto::TypeDuration},
                               {"short_trades", epoch_proto::TypeDuration}},
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