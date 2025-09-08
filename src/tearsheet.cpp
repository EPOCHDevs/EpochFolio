//
// Created by adesola on 11/23/24.
//

#include "epoch_folio/tearsheet.h"
#include "portfolio/pos.h"
#include "portfolio/timeseries.h"
#include "portfolio/txn.h"
#include <fstream>
#include <google/protobuf/util/json_util.h>

namespace glz {
json_t to_json(const epoch_frame::Scalar &array) {
  if (array.is_null()) {
    return {};
  }

  const auto type = array.type()->id();
  if (is_numeric(type)) {
    return array.cast_double().as_double();
  }

  if (type == arrow::Type::BOOL) {
    return array.as_bool();
  }
  return array.repr();
}
}; // namespace glz

namespace epoch_folio {
PortfolioTearSheetFactory::PortfolioTearSheetFactory(
    TearSheetDataOption const &options)
    : m_returns(options.isEquity ? options.equity.pct_change()
                                       .iloc({.start = 1})
                                       .ffill()
                                       .drop_null()
                                 : options.equity),
      m_positions(options.positions.assign("cash", options.cash)),
      m_returnsFactory(options.positions, options.transactions, options.cash,
                       m_returns, options.benchmark),
      m_positionsFactory(options.cash, options.positions, m_returns,
                         options.sectorMapping),
      m_transactionsFactory(m_returns, m_positions, options.transactions),
      m_roundTripFactory(options.roundTrip, m_returns, m_positions,
                         options.sectorMapping) {}

FullTearSheet
PortfolioTearSheetFactory::MakeTearSheet(TearSheetOption const &options) const {
  FullTearSheet tearSheet;

  try {
    m_returnsFactory.Make(options.turnoverDenominator, options.topKDrawDowns,
                          tearSheet);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create returns tearsheet: {}", e.what());
  }

  try {
    m_positionsFactory.Make(options.topKPositions, tearSheet);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create positions tearsheet: {}", e.what());
  }

  try {
    m_transactionsFactory.Make(options.turnoverDenominator,
                               options.transactionBinMinutes,
                               options.transactionTimezone, tearSheet);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create transactions tearsheet: {}", e.what());
  }

  try {
    m_roundTripFactory.Make(tearSheet);
  } catch (std::exception const &e) {
    SPDLOG_ERROR("Failed to create round trip tearsheet: {}", e.what());
  }

  return tearSheet;
}

template <typename T> std::string write_json_(T /*output*/) {
  // TODO: Implement proper JSON serialization for structs containing protobuf
  // messages
  SPDLOG_WARN("JSON serialization temporarily disabled - needs Glaze "
              "definitions for protobuf types");
  return "{}";
}

template <typename T>
void write_json_(T /*output*/, std::string const & /*file_path*/) {
  // TODO: Implement proper JSON file writing for structs containing protobuf
  // messages
  SPDLOG_WARN("JSON file writing temporarily disabled - needs Glaze "
              "definitions for protobuf types");
}

std::string write_json(TearSheet const &output) { return write_json_(output); }

void write_json(TearSheet const &output, std::string const &file_path) {
  write_json_(output, file_path);
}

std::string write_json(FullTearSheet const &output) {
  return write_json_(output);
}

void write_json(FullTearSheet const &output, std::string const &file_path) {
  write_json_(output, file_path);
}
} // namespace epoch_folio
