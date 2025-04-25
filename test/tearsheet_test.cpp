//
// Created by adesola on 3/29/25.
//
#include "common_utils.h"
#include "epoch_folio/tearsheet.h"
#include <catch.hpp>
#include <epoch_frame/serialization.h>

TEST_CASE("Tearsheet") {
  using namespace epoch_folio;

  auto test_returns_path = get_s3_test_path("test_returns.csv");
  auto factor_returns_path =
      ("s3://epoch-db/DailyBars/Stocks/SPY.parquet.gzip");
  auto test_txn_path = get_s3_test_path("test_txn_randomized.csv");
  auto test_pos_path = get_s3_test_path("test_pos.csv");
  auto test_round_trip = get_s3_test_path("round_trips.csv");

  auto utc = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
  auto test_returns =
      epoch_frame::read_csv_file(test_returns_path, {.has_header = false})
          .set_index("f0")
          .to_series();
  auto index = test_returns.index();

  test_returns =
      test_returns.set_index(index->Make(index->array().cast(utc).value()));

  auto test_factor =
      epoch_frame::read_parquet(factor_returns_path)
          .set_index("t")["c"]
          .pct_change()
          .loc({test_returns.index()->at(0), test_returns.index()->at(-1)});
  test_factor = test_factor.set_index(
      index->Make(test_factor.index()->array().cast(utc).value()));

  auto test_txn = epoch_frame::read_csv_file(test_txn_path).rename({{"", "x"}});
  test_txn = test_txn.assign("timestamp", test_txn["x"].cast(utc)).drop("x");

  auto test_pos = epoch_frame::read_csv_file(test_pos_path).set_index("index");
  test_pos = test_pos.set_index(
      index->Make(test_pos.index()->array().cast(utc).value()));

  auto cash = test_pos["cash"];
  test_pos = test_pos.drop("cash");

  const SectorMapping sector{
      {"AMD", "Technology"},      {"CERN", "Health Care"},
      {"COST", "Consumer Goods"}, {"DELL", "Technology"},
      {"GPS", "Technology"},      {"INTC", "Technology"},
      {"MMM", "Construction"},
  };

  auto round_trip = epoch_frame::read_csv_file(test_round_trip).set_index("");
  round_trip =
      round_trip.assign("openDateTime", round_trip["open_dt"].cast(utc))
          .drop("open_dt");
  round_trip =
      round_trip.assign("closeDateTime", round_trip["close_dt"].cast(utc))
          .drop("close_dt");

  round_trip = round_trip.rename({{"pnl", "netReturn"}, {"symbol", "asset"}});

  round_trip =
      round_trip.assign("side", round_trip["long"].map([](auto const &v) {
        return epoch_frame::Scalar{v.as_bool() ? std::string{"Long"} : "Short"};
      }));
  round_trip = round_trip.drop("long");

  auto test_result = PortfolioTearSheetFactory{
      TearSheetDataOption{
          test_returns, test_factor, cash, test_pos, test_txn, round_trip,
          sector, false}}.MakeTearSheet(TearSheetOption{});

  write_json(test_result, "full_test_result.json");
  write_json(test_result.positions, "positions_test_result.json");
  write_json(test_result.strategy_benchmark,
             "strategy_benchmark_test_result.json");
  write_json(test_result.transactions, "transactions_test_result.json");
  write_json(test_result.round_trip, "round_trip_test_result.json");
  write_json(test_result.risk_analysis, "risk_analysis_test_result.json");
  write_json(test_result.returns_distribution,
             "returns_distribution_test_result.json");
}