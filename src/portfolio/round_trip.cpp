//
// Created by adesola on 3/31/25.
//

#include "round_trip.h"

#include "models/table_def.h"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/table_factory.h>
#include <oneapi/tbb/parallel_for.h>

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

Table GetSymbolsTable(epoch_frame::DataFrame const &round_trip,
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
  std::vector<FrameOrSeries> frames;
  ColumnDefs column_defs{{"key", "Stats", epoch_core::EpochFolioType::String}};

  frames.reserve(groups.size());
  column_defs.reserve(groups.size() + 1);

  auto returns = round_trip["returns"];
  for (auto const &[symbol, indexes] : groups) {
    auto symbol_name = symbol.repr();
    frames.emplace_back(
        apply_symbol(symbol_name, returns.iloc(Array{indexes})));
    column_defs.emplace_back(symbol_name, symbol_name,
                             epoch_core::EpochFolioType::Percent);
  }

  auto table = concat({.frames = frames, .axis = AxisType::Column})
                   .reset_index("key")
                   .table();
  return Table{epoch_core::EpochFolioDashboardWidget::DataTable,
               epoch_core::EpochFolioCategory::RoundTrip, "Returns by Symbol",
               column_defs, table};
}

std::vector<Table> GetRoundTripStats(epoch_frame::DataFrame const &round_trip) {
  static const Scalar ZERO{0.0};

  static const AggList PNL_STATS{
      {"Total profit", "sum"},
      {"Gross profit", [](Series const &x) { return x.loc(x > ZERO).sum(); }},
      {"Gross loss", [](Series const &x) { return x.loc(x < ZERO).sum(); }},
      {"Profit factor",
       [](Series const &x) {
         auto neg_sum = x.loc(x < ZERO).abs().sum();
         if (neg_sum != ZERO) {
           return x.loc(x > ZERO).sum() / neg_sum;
         }
         return Scalar();
       }},
      {"Avg. trade net profit", "mean"},
      {"Avg. winning trade",
       [](Series const &x) { return x.loc(x > ZERO).mean(); }},
      {"Avg. losing trade",
       [](Series const &x) { return x.loc(x < ZERO).mean(); }},
      {"Ratio Avg. Win:Avg. Loss",
       [](Series const &x) {
         auto neg_mean = x.loc(x < ZERO).abs().mean();
         if (neg_mean != ZERO) {
           return x.loc(x > ZERO).mean() / neg_mean;
         }
         return Scalar();
       }},
      {"Largest winning trade", "max"},
      {"Largest losing trade", "min"},
  };

  static const AggList SUMMARY_STATS{
      {"Total number of round_trips",
       [](Series const &x) { return x.count_valid(); }},
      {"Percent profitable",
       [](Series const &x) {
         return Scalar{static_cast<double>(x.loc(x > ZERO).size()) /
                       static_cast<double>(x.size())};
       }},
      {"Winning round_trips",
       [](Series const &x) { return Scalar{x.loc(x > ZERO).size()}; }},
      {"Losing round_trips",
       [](Series const &x) { return Scalar{x.loc(x < ZERO).size()}; }},
      {"Even round_trips",
       [](Series const &x) { return Scalar{x.loc(x == ZERO).size()}; }},
  };

  static const AggList RETURNS_STATS{
      {"Avg returns all round_trips", "mean"},
      {"Avg returns winning",
       [](Series const &x) { return x.loc(x > ZERO).mean(); }},
      {"Avg returns losing",
       [](Series const &x) { return x.loc(x < ZERO).mean(); }},
      {"Median returns all round_trips",
       [](Series const &x) {
         return x.quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       }},
      {"Median returns winning",
       [](Series const &x) {
         return x.loc(x > ZERO).quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       }},
      {"Median returns losing",
       [](Series const &x) {
         return x.loc(x < ZERO).quantile(arrow::compute::QuantileOptions{
             0.5, arrow::compute::QuantileOptions::Interpolation::LINEAR});
       }},
      {"Largest winning trade", "max"},
      {"Largest losing trade", "min"},
  };

  static const AggList DURATION_STATS{
      {"Avg duration", "mean"},
      {"Median duration", "approximate_median"},
      {"Longest duration", "max"},
      {"Shortest duration", "min"},
      // FIXME: For the commented out statistics below, we would need access to
      // both open_dt and close_dt fields to calculate the total trading period.
      // A potential solution would be to modify AggAllLongShort to accept
      // multiple fields or pre-calculate these metrics before passing to
      // AggAllLongShort.
      //
      // {"Avg # round_trips per day", [](DataFrame const& rt) {
      //     auto trading_period_days = (rt["closeDateTime"].dt().max() -
      //     rt["openDateTime"].dt().min()).days(); return
      //     static_cast<double>(rt.num_rows()) / trading_period_days;
      // }},
      // {"Avg # round_trips per month", [](DataFrame const& rt) {
      //     auto trading_period_days = (rt["closeDateTime"].dt().max() -
      //     rt["openDateTime"].dt().min()).days(); return
      //     static_cast<double>(rt.num_rows()) / (trading_period_days / 21.0);
      //     // ~21 trading days per month
      // }}
  };

  auto pnl = AggAllLongShort(round_trip, "pnl", PNL_STATS);
  auto summary = AggAllLongShort(round_trip, "pnl", SUMMARY_STATS);
  auto duration = AggAllLongShort<true>(round_trip, "duration", DURATION_STATS);
  auto returns = AggAllLongShort(round_trip, "returns", RETURNS_STATS, true);

  return {
      Table{epoch_core::EpochFolioDashboardWidget::DataTable,
            epoch_core::EpochFolioCategory::RoundTrip, "PnL Statistics",
            ColumnDefs{
                {"key", "PnL Stats", epoch_core::EpochFolioType::String},
                {"all_trades", "All Trades",
                 epoch_core::EpochFolioType::Monetary},
                {"long_trades", "Long Trades",
                 epoch_core::EpochFolioType::Monetary},
                {"short_trades", "Short Trades",
                 epoch_core::EpochFolioType::Monetary},
            },
            pnl},
      Table{
          epoch_core::EpochFolioDashboardWidget::DataTable,
          epoch_core::EpochFolioCategory::RoundTrip, "Trade Summary",
          ColumnDefs{
              {"key", "Summary Stats", epoch_core::EpochFolioType::String},
              {"all_trades", "All Trades", epoch_core::EpochFolioType::Decimal},
              {"long_trades", "Long Trades",
               epoch_core::EpochFolioType::Decimal},
              {"short_trades", "Short Trades",
               epoch_core::EpochFolioType::Decimal},
          },
          summary},
      Table{epoch_core::EpochFolioDashboardWidget::DataTable,
            epoch_core::EpochFolioCategory::RoundTrip, "Duration Analysis",
            ColumnDefs{
                {"key", "Duration Stats", epoch_core::EpochFolioType::String},
                {"all_trades", "All Trades",
                 epoch_core::EpochFolioType::Duration},
                {"long_trades", "Long Trades",
                 epoch_core::EpochFolioType::Duration},
                {"short_trades", "Short Trades",
                 epoch_core::EpochFolioType::Duration},
            },
            duration},
      Table{
          epoch_core::EpochFolioDashboardWidget::DataTable,
          epoch_core::EpochFolioCategory::RoundTrip, "Return Analysis",
          ColumnDefs{
              {"key", "Return Stats", epoch_core::EpochFolioType::String},
              {"all_trades", "All Trades", epoch_core::EpochFolioType::Percent},
              {"long_trades", "Long Trades",
               epoch_core::EpochFolioType::Percent},
              {"short_trades", "Short Trades",
               epoch_core::EpochFolioType::Percent},
          },
          returns},
      GetSymbolsTable(round_trip, RETURNS_STATS)};
}

DataFrame GetProfitAttribution(epoch_frame::DataFrame const &round_trip,
                               std::string const &col) {
  const auto total_pnl = round_trip["pnl"].sum();
  return (
      round_trip[std::vector<std::string>{"pnl", col}].group_by_agg(col).sum() /
      total_pnl);
}

} // namespace epoch_folio