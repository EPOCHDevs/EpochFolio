//
// Created by assistant on 9/8/25.
//

#include "table_helpers.h"

#include <arrow/array.h>
#include <arrow/chunked_array.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "chart_def.h"

namespace epoch_folio {

static epoch_proto::Scalar
MakeScalarFromArrow(const std::shared_ptr<arrow::Array> &array, int64_t row) {
  // Check for null values first
  if (array->IsNull(row)) {
    return ToProtoScalarValue(nullptr);
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
  case arrow::Type::TIMESTAMP: {
    auto a = std::static_pointer_cast<arrow::TimestampArray>(array);
    return ToProtoScalarValue(a->Value(row));
  }
  default: {
    // Fallback: use ToString
    return ToProtoScalarValue(array->ToString());
  }
  }
}

epoch_proto::Array
MakeArrayFromArrow(const std::shared_ptr<arrow::ChunkedArray> &chunked_array) {
  epoch_proto::Array out;
  if (!chunked_array) {
    return out;
  }

  auto n = chunked_array->length();
  out.mutable_values()->Reserve(n);

  auto array = arrow::Concatenate(chunked_array->chunks()).MoveValueUnsafe();
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
  auto rb = table->CombineChunks().MoveValueUnsafe();
  auto cols = table->columns();

  const int64_t nrows = table->num_rows();
  for (int64_t r = 0; r < nrows; ++r) {
    auto *row = out.add_rows();
    for (int c = 0; c < static_cast<int>(cols.size()); ++c) {
      auto *cell = row->add_values();
      *cell = MakeScalarFromArrow(rb->column(c)->chunk(0), r);
    }
  }

  return out;
}

} // namespace epoch_folio
