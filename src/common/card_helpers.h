#pragma once

#include "chart_def.h"
#include <epoch_frame/scalar.h>
#include <epoch_protos/common.pb.h>
#include <epoch_protos/table_def.pb.h>

namespace epoch_folio {

// Helper class to safely add data to cards with correct type handling
class CardDataHelper {
public:
  // Add date field to card (handles conversion to seconds for TypeDate)
  static epoch_proto::CardData* AddDateField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& date_value,
      int group = 0);

  // Add timestamp field to card (uses milliseconds for TypeDateTime)
  static epoch_proto::CardData* AddTimestampField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& timestamp_value,
      int group = 0);

  // Add integer field to card
  static epoch_proto::CardData* AddIntegerField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& int_value,
      int group = 0);

  // Add decimal field to card
  static epoch_proto::CardData* AddDecimalField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& decimal_value,
      int group = 0);

  // Add percentage field to card (handles conversion if needed)
  static epoch_proto::CardData* AddPercentField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& percent_value,
      int group = 0);

  // Add monetary field to card
  static epoch_proto::CardData* AddMonetaryField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const epoch_frame::Scalar& monetary_value,
      int group = 0);

  // Add string field to card
  static epoch_proto::CardData* AddStringField(
      epoch_proto::CardDef& card,
      const std::string& title,
      const std::string& str_value,
      int group = 0);

  // Add day duration field to card
  static epoch_proto::CardData* AddDayDurationField(
      epoch_proto::CardDef& card,
      const std::string& title,
      int32_t days,
      int group = 0);
};

// Helper class for table column setup
class TableColumnHelper {
public:
  // Set up date column (if id is empty, uses name for both)
  static epoch_proto::ColumnDef* AddDateColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up timestamp/datetime column
  static epoch_proto::ColumnDef* AddTimestampColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up integer column
  static epoch_proto::ColumnDef* AddIntegerColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up decimal column
  static epoch_proto::ColumnDef* AddDecimalColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up percentage column
  static epoch_proto::ColumnDef* AddPercentColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up monetary column
  static epoch_proto::ColumnDef* AddMonetaryColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up string column
  static epoch_proto::ColumnDef* AddStringColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");

  // Set up day duration column
  static epoch_proto::ColumnDef* AddDayDurationColumn(
      epoch_proto::Table& table,
      const std::string& name,
      const std::string& id = "");
};

// Helper class for adding values to table rows with correct type conversion
class TableRowHelper {
public:
  // Add date value to row (handles conversion to seconds)
  static epoch_proto::Scalar AddDateValue(const epoch_frame::Scalar& date_value);

  // Add timestamp value to row (uses milliseconds)
  static epoch_proto::Scalar AddTimestampValue(const epoch_frame::Scalar& timestamp_value);

  // Add properly typed scalar based on the column type
  static epoch_proto::Scalar AddTypedValue(
      const epoch_frame::Scalar& value,
      epoch_proto::EpochFolioType column_type);
};

} // namespace epoch_folio