//
// Created by adesola on 11/23/24.
//

#include "tearsheet.h"
#include "portfolio/timeseries.h"
#include "portfolio/pos.h"
#include "portfolio/txn.h"
#include <glaze/glaze.hpp>


namespace glz {
    json_t to_json(const epoch_frame::Scalar& array) {
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

    template <>
    struct to<JSON, arrow::TablePtr> {
        template <auto Opts>
        static void op(const arrow::TablePtr& table, auto&&... args) noexcept
        {
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

    template <>
    struct to<JSON, epoch_frame::Array> {
        template <auto Opts>
        static void op(const epoch_frame::Array& array, auto&&... args) noexcept
        {
            std::vector<json_t> arr;
            arr.reserve(array.length());

            for (int64_t i = 0; i < array.length(); ++i) {
                arr.emplace_back(to_json(array[i]));
            }
            serialize<JSON>::op<Opts>(arr, args...);
        }
    };

    template <>
    struct to<JSON, epoch_frame::Scalar> {
        template <auto Opts>
        static void op(const epoch_frame::Scalar& scalar, auto&&... args) noexcept
        {
            serialize<JSON>::op<Opts>(to_json(scalar), args...);
        }
    };
}

namespace epoch_folio {
    PortfolioTearSheetFactory::PortfolioTearSheetFactory(TearSheetDataOption const &options) :
            m_returns(options.isEquity ? options.equity.pct_change().iloc({.start=1}).ffill().drop_null() : options.equity),
            m_positions(options.positions.assign("cash", options.cash)),
            m_returnsFactory(options.positions, options.transactions, options.cash, m_returns, options.benchmark),
            m_positionsFactory(options.cash, options.positions, m_returns, options.sectorMapping),
            m_transactionsFactory(m_returns, m_positions, options.transactions),
            m_roundTripFactory(options.roundTrip, m_returns, m_positions, options.sectorMapping){}

    FullTearSheet PortfolioTearSheetFactory::MakeTearSheet(TearSheetOption const &options) const {
        FullTearSheet tearSheet;
        m_returnsFactory.Make(options.turnoverDenominator, options.topKDrawDowns, tearSheet);
        m_positionsFactory.Make(options.topKPositions, tearSheet);
        m_transactionsFactory.Make(options.turnoverDenominator, options.transactionBinMinutes, options.transactionTimezone, tearSheet);
        m_roundTripFactory.Make(tearSheet);
        return tearSheet;
    }

    template <typename T>
    std::string write_json_(T output) {
            auto result = glz::write_json(std::move(output));
            if (result.error())
            {
                SPDLOG_ERROR("Failed to write tear sheet to json: {}", glz::format_error(result.error()));
                return "";
            }
            return result.value();
        }

    template <typename T>
    void write_json_(T output, std::string const &file_path) {
        auto error = glz::write_file_json(std::move(output), file_path, std::string());
        if (!error)
        {
            return;
        }

        SPDLOG_ERROR("Failed to write full tear sheet to json: {}", glz::format_error(error));
    }

    std::string write_json(TearSheet const &output) {
        return write_json_(output);
    }

    void write_json(TearSheet const &output, std::string const &file_path) {
        write_json_(output, file_path);
    }

    std::string write_json(FullTearSheet const &output) {
        return write_json_(output);
    }

    void write_json(FullTearSheet const &output, std::string const &file_path) {
        write_json_(output, file_path);
    }
}

