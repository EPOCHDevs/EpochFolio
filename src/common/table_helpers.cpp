//
// Created by assistant on 9/8/25.
//

#include "table_helpers.h"

#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/type.h>

#include "chart_def.h"

namespace epoch_folio {

static epoch_proto::EpochFolioType
MapArrowTypeToFolioType(const std::shared_ptr<arrow::DataType> &type) {
  switch (type->id()) {
  case arrow::Type::STRING:
  case arrow::Type::LARGE_STRING:
    return epoch_proto::EPOCH_FOLIO_TYPE_STRING;
  case arrow::Type::INT32:
  case arrow::Type::INT64:
  case arrow::Type::UINT32:
  case arrow::Type::UINT64:
    return epoch_proto::EPOCH_FOLIO_TYPE_INTEGER;
  case arrow::Type::DOUBLE:
  case arrow::Type::FLOAT:
    return epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL;
  case arrow::Type::BOOL:
    return epoch_proto::EPOCH_FOLIO_TYPE_BOOLEAN;
  default:
    return epoch_proto::EPOCH_FOLIO_TYPE_STRING;
  }
}

template <typename T> epoch_proto::Scalar ToProtoScalarValue(T const &scalar) {
  epoch_proto::Scalar s;

  if constexpr (std::is_integral_v<T>) {
    s.set_int64_value(scalar);
  } else if constexpr (std::is_floating_point_v<T>) {
    s.set_double_value(scalar);
  } else if constexpr (std::is_same_v<T, bool>) {
    s.set_bool_value(scalar);
  } else {
    s.set_string_value(scalar);
  }

  return s;
}

static epoch_proto::Scalar
MakeScalarFromArrow(const std::shared_ptr<arrow::ChunkedArray> &array,
                    int64_t row) {
  // Handle nulls
  if (!array || row < 0 || row >= array->length()) {
    epoch_proto::Scalar s;
    return s;
  }

  switch (array->type()->id()) {
  case arrow::Type::STRING: {
    auto str = std::static_pointer_cast<arrow::StringArray>(array);
    return ToProtoScalarValue(str->GetString(row));
  }
  case arrow::Type::LARGE_STRING: {
    auto str = std::static_pointer_cast<arrow::LargeStringArray>(array);
    return ToProtoScalarValue(str->GetString(row));
  }
  case arrow::Type::INT32: {
    auto a = std::static_pointer_cast<arrow::Int32Array>(array);
    return ToProtoScalarValue(static_cast<int64_t>(a->Value(row)));
  }
  case arrow::Type::INT64: {
    auto a = std::static_pointer_cast<arrow::Int64Array>(array);
    return ToProtoScalarValue(static_cast<int64_t>(a->Value(row)));
  }
  case arrow::Type::UINT32: {
    auto a = std::static_pointer_cast<arrow::UInt32Array>(array);
    return ToProtoScalarValue(static_cast<uint64_t>(a->Value(row)));
  }
  case arrow::Type::UINT64: {
    auto a = std::static_pointer_cast<arrow::UInt64Array>(array);
    return ToProtoScalarValue(static_cast<uint64_t>(a->Value(row)));
  }
  case arrow::Type::DOUBLE: {
    auto a = std::static_pointer_cast<arrow::DoubleArray>(array);
    return ToProtoScalarValue(a->Value(row));
  }
  case arrow::Type::FLOAT: {
    auto a = std::static_pointer_cast<arrow::FloatArray>(array);
    return ToProtoScalarValue(static_cast<double>(a->Value(row)));
  }
  case arrow::Type::BOOL: {
    auto a = std::static_pointer_cast<arrow::BooleanArray>(array);
    return ToProtoScalarValue(a->Value(row));
  }
  default: {
    // Fallback: use ToString
    return ToProtoScalarValue(array->ToString());
  }
  }
}

epoch_proto::Array
MakeArrayFromArrow(const std::shared_ptr<arrow::ChunkedArray> &array) {
  epoch_proto::Array out;
  if (!array) {
    return out;
  }

  auto n = array->length();
  out.mutable_values()->Reserve(n);
  for (int64_t i = 0; i < n; ++i) {
    *out.add_values() = MakeScalarFromArrow(array, i);
  }
  return out;
}

epoch_proto::TableData
MakeTableDataFromArrow(const std::shared_ptr<arrow::Table> &table) {
  epoch_proto::TableData out;
  if (!table) {
    return out;
  }

  auto cols = table->columns();

  const int64_t nrows = table->num_rows();
  for (int64_t r = 0; r < nrows; ++r) {
    auto *row = out.add_rows();
    for (int c = 0; c < static_cast<int>(cols.size()); ++c) {
      auto *cell = row->add_values();
      *cell = MakeScalarFromArrow(cols[c], r);
    }
  }

  return out;
}

} // namespace epoch_folio
