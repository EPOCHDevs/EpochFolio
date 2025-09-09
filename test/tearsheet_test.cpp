//
// Created by adesola on 3/29/25.
//
#include "common_utils.h"
#include "epoch_folio/tearsheet.h"
#include <catch.hpp>
#include <epoch_frame/serialization.h>
#include <filesystem>

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
          .ValueOrDie()
          .set_index("f0")
          .to_series();
  auto index = test_returns.index();

  test_returns =
      test_returns.set_index(index->Make(index->array().cast(utc).value()));

  auto test_factor =
      epoch_frame::read_parquet(factor_returns_path)
          .ValueOrDie()
          .set_index("t")["c"]
          .pct_change()
          .loc({test_returns.index()->at(0), test_returns.index()->at(-1)});
  test_factor = test_factor.set_index(
      index->Make(test_factor.index()->array().cast(utc).value()));

  auto test_txn = epoch_frame::read_csv_file(test_txn_path)
                      .ValueOrDie()
                      .rename({{"", "x"}});
  test_txn = test_txn.assign("timestamp", test_txn["x"].cast(utc))
                 .drop("x")
                 .set_index("timestamp");

  auto test_pos =
      epoch_frame::read_csv_file(test_pos_path).ValueOrDie().set_index("index");
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

  auto round_trip =
      epoch_frame::read_csv_file(test_round_trip).ValueOrDie().set_index("");
  round_trip =
      round_trip.assign("open_datetime", round_trip["open_dt"].cast(utc))
          .drop("open_dt");
  round_trip =
      round_trip.assign("close_datetime", round_trip["close_dt"].cast(utc))
          .drop("close_dt");

  round_trip = round_trip.rename({{"pnl", "net_return"}, {"symbol", "asset"}});

  round_trip =
      round_trip.assign("side", round_trip["long"].map([](auto const &v) {
        return epoch_frame::Scalar{v.as_bool() ? std::string{"Long"} : "Short"};
      }));
  round_trip = round_trip.drop("long");

  auto test_result = PortfolioTearSheetFactory{
      TearSheetDataOption{
          test_returns, test_factor, cash, test_pos, test_txn, round_trip,
          sector, false}}.MakeTearSheet(TearSheetOption{});

  // Create output directory if it doesn't exist
  std::string output_dir = "test_output";
  std::filesystem::create_directories(output_dir);

  write_protobuf(test_result, output_dir + "/full_test_result.pb");
  auto &categories = test_result.categories();
  if (categories.contains(epoch_folio::categories::Positions)) {
    write_protobuf(categories.at(epoch_folio::categories::Positions),
                   output_dir + "/positions_test_result.pb");
  }
  if (categories.contains(epoch_folio::categories::StrategyBenchmark)) {
    write_protobuf(categories.at(epoch_folio::categories::StrategyBenchmark),
                   output_dir + "/strategy_benchmark_test_result.pb");
  }
  if (categories.contains(epoch_folio::categories::Transactions)) {
    write_protobuf(categories.at(epoch_folio::categories::Transactions),
                   output_dir + "/transactions_test_result.pb");
  }
  if (categories.contains(epoch_folio::categories::RoundTrip)) {
    write_protobuf(categories.at(epoch_folio::categories::RoundTrip),
                   output_dir + "/round_trip_test_result.pb");
  }
  if (categories.contains(epoch_folio::categories::RiskAnalysis)) {
    write_protobuf(categories.at(epoch_folio::categories::RiskAnalysis),
                   output_dir + "/risk_analysis_test_result.pb");
  }
  if (categories.contains(epoch_folio::categories::ReturnsDistribution)) {
    write_protobuf(categories.at(epoch_folio::categories::ReturnsDistribution),
                   output_dir + "/returns_distribution_test_result.pb");
  }
}