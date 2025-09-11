#include "card_helpers.h"
#include <arrow/type.h>
#include <spdlog/spdlog.h>

namespace epoch_folio {

// CardDataHelper implementations

epoch_proto::CardData* CardDataHelper::AddDateField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& date_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  
  // For TypeDate, we need to set the date in seconds (not milliseconds)
  // The incoming scalar is a timestamp index value
  try {
    // Use ToProtoScalar which handles the conversion properly
    auto proto_scalar = ToProtoScalar(date_value);
    
    // Check what type of value we got and convert appropriately for TypeDate
    if (proto_scalar.has_timestamp_ms()) {
      // Convert milliseconds to seconds for TypeDate
      int64_t seconds = proto_scalar.timestamp_ms() / 1000;
      proto_scalar.Clear();
      proto_scalar.set_date_value(seconds);
    } else if (proto_scalar.has_integer_value()) {
      // If we have an integer, interpret it as timestamp
      int64_t value = proto_scalar.integer_value();
      proto_scalar.Clear();
      // Assume nanoseconds if the value is large enough
      if (value > 1000000000000L) {
        proto_scalar.set_date_value(value / 1000000000); // ns to seconds
      } else {
        proto_scalar.set_date_value(value); // Already in seconds
      }
    } else if (!proto_scalar.has_date_value()) {
      // If we still don't have a date value, set to null
      proto_scalar.Clear();
      proto_scalar.set_null_value(google::protobuf::NULL_VALUE);
    }
    
    *data->mutable_value() = proto_scalar;
  } catch (const std::exception& e) {
    // Fallback: set to null
    SPDLOG_WARN("Error converting date value: {}", e.what());
    auto proto_scalar = epoch_proto::Scalar();
    proto_scalar.set_null_value(google::protobuf::NULL_VALUE);
    *data->mutable_value() = proto_scalar;
  }
  
  data->set_type(epoch_proto::TypeDate);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddTimestampField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& timestamp_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  *data->mutable_value() = ToProtoScalar(timestamp_value);
  data->set_type(epoch_proto::TypeDateTime);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddIntegerField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& int_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  *data->mutable_value() = ToProtoScalar(int_value);
  data->set_type(epoch_proto::TypeInteger);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddDecimalField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& decimal_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  *data->mutable_value() = ToProtoScalar(decimal_value);
  data->set_type(epoch_proto::TypeDecimal);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddPercentField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& percent_value,
    int group,
    bool multiply_by_100) {
  auto* data = card.add_data();
  data->set_title(title);
  
  if (multiply_by_100) {
    auto scaled_value = percent_value * epoch_frame::Scalar{100.0};
    *data->mutable_value() = ToProtoScalar(scaled_value);
  } else {
    *data->mutable_value() = ToProtoScalar(percent_value);
  }
  
  data->set_type(epoch_proto::TypePercent);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddMonetaryField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const epoch_frame::Scalar& monetary_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  *data->mutable_value() = ToProtoScalar(monetary_value);
  data->set_type(epoch_proto::TypeMonetary);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddStringField(
    epoch_proto::CardDef& card,
    const std::string& title,
    const std::string& str_value,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  data->mutable_value()->set_string_value(str_value);
  data->set_type(epoch_proto::TypeString);
  data->set_group(group);
  return data;
}

epoch_proto::CardData* CardDataHelper::AddDayDurationField(
    epoch_proto::CardDef& card,
    const std::string& title,
    int32_t days,
    int group) {
  auto* data = card.add_data();
  data->set_title(title);
  data->mutable_value()->set_day_duration(days);
  data->set_type(epoch_proto::TypeDayDuration);
  data->set_group(group);
  return data;
}

// TableColumnHelper implementations

epoch_proto::ColumnDef* TableColumnHelper::AddDateColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeDate);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddTimestampColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeDateTime);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddIntegerColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeInteger);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddDecimalColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeDecimal);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddPercentColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypePercent);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddMonetaryColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeMonetary);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddStringColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeString);
  return col;
}

epoch_proto::ColumnDef* TableColumnHelper::AddDayDurationColumn(
    epoch_proto::Table& table,
    const std::string& name,
    const std::string& id) {
  auto* col = table.add_columns();
  col->set_id(id.empty() ? name : id);
  col->set_name(name);
  col->set_type(epoch_proto::TypeDayDuration);
  return col;
}

// TableRowHelper implementations

epoch_proto::Scalar TableRowHelper::AddDateValue(const epoch_frame::Scalar& date_value) {
  auto proto_scalar = ToProtoScalar(date_value);
  
  // If it's a timestamp, convert milliseconds to seconds for TypeDate
  if (proto_scalar.has_timestamp_ms()) {
    proto_scalar.set_date_value(proto_scalar.timestamp_ms() / 1000);
    proto_scalar.clear_timestamp_ms();
  }
  
  return proto_scalar;
}

epoch_proto::Scalar TableRowHelper::AddTimestampValue(const epoch_frame::Scalar& timestamp_value) {
  return ToProtoScalar(timestamp_value);
}

epoch_proto::Scalar TableRowHelper::AddTypedValue(
    const epoch_frame::Scalar& value,
    epoch_proto::EpochFolioType column_type) {
  
  switch (column_type) {
    case epoch_proto::TypeDate:
      return AddDateValue(value);
      
    case epoch_proto::TypeDateTime:
      return AddTimestampValue(value);
      
    case epoch_proto::TypePercent: {
      // Percentages need to be multiplied by 100 for display
      auto scaled_value = value * epoch_frame::Scalar{100.0};
      return ToProtoScalar(scaled_value);
    }
      
    case epoch_proto::TypeMonetary:
      // Monetary values are decimals with currency formatting
      return ToProtoScalar(value);
      
    case epoch_proto::TypeDayDuration: {
      // Handle duration conversion properly
      if (!value.is_valid()) {
        epoch_proto::Scalar null_scalar;
        null_scalar.set_null_value(google::protobuf::NULL_VALUE);
        return null_scalar;
      }
      
      try {
        // Cast to int64 if needed (handles uint64 -> int64)
        auto casted = value.cast_int64();
        auto days = casted.as_int64();
        
        epoch_proto::Scalar proto_scalar;
        proto_scalar.set_day_duration(static_cast<int32_t>(days));
        return proto_scalar;
      } catch (const std::exception& e) {
        // If conversion fails, try the default ToProtoScalar
        auto proto_scalar = ToProtoScalar(value);
        if (proto_scalar.has_duration_ms()) {
          // Convert milliseconds to days
          int32_t days = proto_scalar.duration_ms() / (24 * 60 * 60 * 1000);
          proto_scalar.set_day_duration(days);
          proto_scalar.clear_duration_ms();
        }
        return proto_scalar;
      }
    }
      
    default:
      // For all other types, use standard conversion
      return ToProtoScalar(value);
  }
}

} // namespace epoch_folio