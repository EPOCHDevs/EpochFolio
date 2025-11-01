//
// Created by adesola on 11/23/24.
//

#include "epoch_folio/tearsheet.h"
#include <epoch_protos/tearsheet.pb.h>
#include <fstream>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <spdlog/spdlog.h>

namespace glz
{
  generic to_json(const epoch_frame::Scalar &array)
  {
    if (array.is_null())
    {
      return {};
    }

    const auto type = array.type()->id();
    if (is_numeric(type))
    {
      return array.cast_double().as_double();
    }

    if (type == arrow::Type::BOOL)
    {
      return array.as_bool();
    }
    return array.repr();
  }
}; // namespace glz

namespace epoch_folio
{
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

  epoch_proto::TearSheet
  PortfolioTearSheetFactory::MakeTearSheet(TearSheetOption const &options) const
  {
    epoch_tearsheet::DashboardBuilder builder;

    try
    {
      m_returnsFactory.Make(options.turnoverDenominator, options.topKDrawDowns,
                            builder);
    }
    catch (std::exception const &e)
    {
      SPDLOG_ERROR("Failed to create returns tearsheet: {}", e.what());
    }

    try
    {
      m_positionsFactory.Make(options.topKPositions, builder);
    }
    catch (std::exception const &e)
    {
      SPDLOG_ERROR("Failed to create positions tearsheet: {}", e.what());
    }

    try
    {
      m_transactionsFactory.Make(options.turnoverDenominator,
                                 options.transactionBinMinutes,
                                 options.transactionTimezone, builder);
    }
    catch (std::exception const &e)
    {
      SPDLOG_ERROR("Failed to create transactions tearsheet: {}", e.what());
    }

    try
    {
      m_roundTripFactory.Make(builder);
    }
    catch (std::exception const &e)
    {
      SPDLOG_ERROR("Failed to create round trip tearsheet: {}", e.what());
    }

    return builder.build();
  }

  template <typename T>
  std::string write_protobuf_(const T &output)
  {
    std::string binary_data;
    if (!output.SerializeToString(&binary_data))
    {
      SPDLOG_ERROR("Failed to serialize protobuf message to binary");
      return "";
    }
    return binary_data;
  }

  template <typename T>
  void write_protobuf_(const T &output, std::string const &file_path)
  {
    std::string binary_data;
    if (!output.SerializeToString(&binary_data))
    {
      SPDLOG_ERROR("Failed to serialize protobuf message to binary");
      return;
    }

    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open())
    {
      SPDLOG_ERROR("Failed to open file for writing: {}", file_path);
      return;
    }

    file.write(binary_data.data(), binary_data.size());
    file.close();
    SPDLOG_INFO("Successfully wrote protobuf data to: {}", file_path);
  }

  std::string write_protobuf(epoch_proto::TearSheet const &output)
  {
    return write_protobuf_(output);
  }

  void write_protobuf(epoch_proto::TearSheet const &output,
                      std::string const &file_path)
  {
    write_protobuf_(output, file_path);
  }
} // namespace epoch_folio
