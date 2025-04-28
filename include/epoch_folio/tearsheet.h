//
// Created by adesola on 11/23/24.
//

#pragma once
#include <string>

#include "portfolio/model.h"
#include <unordered_map>

#include "tear_sheets/positions/tearsheet.h"
#include "tear_sheets/returns/tearsheet.h"
#include "tear_sheets/round_trip/tearsheet.h"
#include "tear_sheets/transactions/tearsheet.h"

namespace epoch_folio {

class PortfolioTearSheetFactory {

public:
  explicit PortfolioTearSheetFactory(TearSheetDataOption const &options);

  FullTearSheet MakeTearSheet(TearSheetOption const &) const;

private:
  epoch_frame::Series m_returns;
  epoch_frame::DataFrame m_positions;
  returns::TearSheetFactory m_returnsFactory;
  positions::TearSheetFactory m_positionsFactory;
  txn::TearSheetFactory m_transactionsFactory;
  round_trip::TearSheetFactory m_roundTripFactory;
};

std::string write_json(FullTearSheet const &output);
void write_json(FullTearSheet const &output, std::string const &file_path);
std::string write_json(TearSheet const &output);
void write_json(TearSheet const &output, std::string const &file_path);
} // namespace epoch_folio

namespace glz {
json_t to_json(const epoch_frame::Scalar &array);

template <> struct to<JSON, arrow::TablePtr> {
  template <auto Opts>
  static void op(const arrow::TablePtr &table, auto &&...args) noexcept {
    std::vector<json_t> json_obj;
    if (!table) {
      serialize<JSON>::op<Opts>(json_obj, args...);
      return;
    }

    json_obj.reserve(table->num_rows());
    const auto column_names = table->ColumnNames();
    const auto columns = table->columns();

    auto col_range = std::views::iota(0, static_cast<int>(columns.size()));
    auto row_range = std::views::iota(0, static_cast<int>(table->num_rows()));

    for (size_t row : row_range) {
      json_t row_obj;
      for (size_t col : col_range) {
        const auto col_name = column_names[col];
        const auto scalar = columns[col]->GetScalar(row).MoveValueUnsafe();
        row_obj[col_name] = to_json(epoch_frame::Scalar{scalar});
      }
      json_obj.emplace_back(std::move(row_obj));
    }
    serialize<JSON>::op<Opts>(json_obj, args...);
  }
};

template <> struct to<JSON, epoch_frame::Array> {
  template <auto Opts>
  static void op(const epoch_frame::Array &array, auto &&...args) noexcept {
    std::vector<json_t> arr;
    arr.reserve(array.length());

    for (int64_t i = 0; i < array.length(); ++i) {
      arr.emplace_back(to_json(array[i]));
    }
    serialize<JSON>::op<Opts>(arr, args...);
  }
};

template <> struct to<JSON, epoch_frame::Scalar> {
  template <auto Opts>
  static void op(const epoch_frame::Scalar &scalar, auto &&...args) noexcept {
    serialize<JSON>::op<Opts>(to_json(scalar), args...);
  }
};
} // namespace glz