//
// Created by adesola on 1/13/25.
//
#include "portfolio/txn.h"
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/scalar_factory.h>
#include <epoch_frame/factory/series_factory.h>

using namespace epoch_folio;
using namespace epoch_frame::factory::index;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

TEST_CASE("Transaction") {
  auto dates = date_range({.start = "2015-01-01"_date,
                           .periods = 20,
                           .offset = factory::offset::days(1)});
  auto positions = make_dataframe(
      dates,
      std::vector{std::vector<double>{40, 10, 40, 10, 40, 10, 40, 10, 40, 10,
                                      40, 10, 40, 10, 40, 10, 40, 10, 40, 10},
                  std::vector(dates->size(), 10.0)},
      std::vector<std::string>{"0", "cash"});

  SECTION("No Transactions") {
    auto transactions =
        make_dataframe(dates, std::vector<std::vector<Scalar>>{},
                       {arrow::field("sid", arrow::int32()),
                        arrow::field("amount", arrow::float64()),
                        arrow::field("price", arrow::float64()),
                        arrow::field("symbol", arrow::utf8())});

    auto expected = make_series(dates, std::vector(dates->size(), 0.0));
    auto result = GetTurnover(positions, transactions);

    INFO(result << "\n!=\n" << expected);
    REQUIRE(result.equals(expected));
  }

  SECTION("Multiple Transactions") {
    auto index = factory::index::from_range(dates->size());

    auto transactions1 = make_dataframe(
        index,
        std::vector{
            dates->array().as_chunked_array(),
            factory::array::make_array(std::vector<int32_t>(dates->size(), 1)),
            factory::array::make_array(std::vector<double>(dates->size(), 1)),
            factory::array::make_array(std::vector<double>(dates->size(), 10)),
            factory::array::make_array(
                std::vector<std::string>(dates->size(), "0"))},
        {"timestamp", "sid", "amount", "price", "symbol"});

    auto transactions2 = make_dataframe(
        index,
        std::vector{
            dates->array().as_chunked_array(),
            factory::array::make_array(std::vector<int32_t>(dates->size(), 2)),
            factory::array::make_array(std::vector<double>(dates->size(), -1)),
            factory::array::make_array(std::vector<double>(dates->size(), 10)),
            factory::array::make_array(
                std::vector<std::string>(dates->size(), "0"))},
        {"timestamp", "sid", "amount", "price", "symbol"});

    auto transactions = concat(
        {.frames = {transactions1, transactions2}, .ignore_index = true});
    transactions =
        transactions.sort_values({"timestamp"}).set_index("timestamp");

    auto result = GetTurnover(positions, transactions);

    auto expected_v = std::vector(dates->size(), 0.8);
    expected_v[0] = 1.0;
    auto expected = make_series(dates, expected_v);

    INFO(result << "\n!=\n" << expected);
    REQUIRE(result.equals(expected));
  }
}
