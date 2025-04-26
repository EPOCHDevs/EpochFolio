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