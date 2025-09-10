#include "chart_def.h"
#include <arrow/scalar.h>
#include <memory>
#include <tbb/tbb.h>
namespace epoch_folio {
constexpr size_t kParallelThreshold = 10;

epoch_proto::Scalar ToProtoScalar(const epoch_frame::Scalar &s) {
  epoch_proto::Scalar out;

  // Handle null values first
  if (s.is_null()) {
    out.set_null_value(google::protobuf::NULL_VALUE);
    return out;
  }

  try {
    auto t = s.type();
    // Order by most common usage first
    switch (t->id()) {
    case arrow::Type::DOUBLE:
      out.set_decimal_value(s.as_double());
      return out;
    case arrow::Type::FLOAT:
      out.set_decimal_value(static_cast<double>(s.cast_float().as_double()));
      return out;
    case arrow::Type::INT64:
      out.set_integer_value(s.as_int64());
      return out;
    case arrow::Type::INT32:
      out.set_integer_value(static_cast<int64_t>(s.cast_int32().as_int32()));
      return out;
    case arrow::Type::UINT64:
      out.set_integer_value(static_cast<int64_t>(s.cast_uint64().as_int64()));
      return out;
    case arrow::Type::UINT32:
      out.set_integer_value(static_cast<int64_t>(s.cast_uint32().as_int32()));
      return out;
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
    case arrow::Type::LARGE_STRING:
      out.set_string_value(s.repr());
      return out;
    case arrow::Type::BOOL:
      out.set_boolean_value(s.as_bool());
      return out;
    case arrow::Type::TIMESTAMP: {
      // epoch_frame::Scalar repr holds ns timestamp; convert to ms
      auto ts = std::static_pointer_cast<arrow::TimestampScalar>(s.value());
      if (ts) {
        out.set_timestamp_ms(ts->value / 1000000); // Convert ns to ms
      } else {
        throw std::runtime_error("Invalid timestamp scalar");
      }
      return out;
    }
    default:
      // Fallback to repr as string
      out.set_string_value(s.repr());
      return out;
    }
  } catch (const std::exception &e) {
    // If any scalar conversion fails, set as null
    out.set_null_value(google::protobuf::NULL_VALUE);
    return out;
  }
}

SeriesLines MakeSeriesLines(const epoch_frame::DataFrame &df) {
  auto timestamp_index = df.index()->array().to_timestamp_view();
  SeriesLines result(df.num_cols());

  // Check that index is timestamp once before the loop
  auto index_type = timestamp_index->type();
  if (index_type->id() != arrow::Type::TIMESTAMP) {
    throw std::runtime_error("MakeSeriesLines expected timestamp index, got: " +
                             index_type->ToString());
  }

  auto inner_loop = [](auto start, auto end, auto &result, auto &df,
                       auto &timestamp_index) {
    for (size_t i = start; i < end; ++i) {
      const auto &col = df.table()->field(i)->name();
      result[i].mutable_data()->Reserve(df.num_rows());
      result[i].set_name(col);
      epoch_frame::Series column = df[col];

      // Cast column to DoubleArray once
      auto double_column = column.contiguous_array().to_view<double>();

      for (size_t row = 0; row < df.num_rows(); ++row) {
        auto *p = result[i].add_data();
        // Convert timestamp to ms (we know it's timestamp from the check above)
        p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
        // Get y value directly from double array
        p->set_y(double_column->Value(row));
      }
    }
  };

  if (df.num_cols() < kParallelThreshold) {
    inner_loop(0, df.num_cols(), result, df, timestamp_index);
  } else {
    tbb::parallel_for(tbb::blocked_range<size_t>(0, df.num_cols()),
                      [&](const tbb::blocked_range<size_t> &range) {
                        inner_loop(range.begin(), range.end(), result, df,
                                   timestamp_index);
                      });
  }

  return result;
}

Line MakeSeriesLine(const epoch_frame::Series &series,
                    std::optional<std::string> const &name) {
  Line line;
  if (name && !name->empty())
    line.set_name(*name);
  auto df = series.to_frame(name);
  auto timestamp_index = df.index()->array().to_timestamp_view();

  // Check that index is timestamp once before the loop
  auto index_type = timestamp_index->type();
  if (index_type->id() != arrow::Type::TIMESTAMP) {
    throw std::runtime_error("MakeSeriesLine expected timestamp index, got: " +
                             index_type->ToString());
  }

  epoch_frame::Series column = df[df.table()->field(0)->name()];
  // Cast column to DoubleArray once
  auto double_column = column.contiguous_array().to_view<double>();

  line.mutable_data()->Reserve(df.num_rows());
  for (size_t row = 0; row < df.num_rows(); ++row) {
    auto *p = line.add_data();
    // Convert timestamp to ms (we know it's timestamp from the check above)
    p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
    // Get y value directly from double array
    p->set_y(double_column->Value(row));
  }
  return line;
}

SeriesLines MakeSeriesLines(const epoch_frame::Series &seriesA,
                            const epoch_frame::Series &seriesB,
                            std::optional<std::string> const &nameA,
                            std::optional<std::string> const &nameB) {
  AssertFromFormat(seriesA.index()->equals(seriesB.index()),
                   "Series A and B must have the same index");

  auto timestamp_index = seriesA.index()->array().to_timestamp_view();

  // Check that index is timestamp once before the loop
  auto index_type = timestamp_index->type();
  if (index_type->id() != arrow::Type::TIMESTAMP) {
    throw std::runtime_error("MakeSeriesLines expected timestamp index, got: " +
                             index_type->ToString());
  }

  // Cast series to DoubleArray once
  auto double_seriesA = seriesA.contiguous_array().to_view<double>();
  auto double_seriesB = seriesB.contiguous_array().to_view<double>();

  auto columnA = nameA.value_or(seriesA.name().value_or(""));
  auto columnB = nameB.value_or(seriesB.name().value_or(""));

  SeriesLines result(2);
  result[0].set_name(columnA);
  result[1].set_name(columnB);

  result[0].mutable_data()->Reserve(timestamp_index->length());
  result[1].mutable_data()->Reserve(timestamp_index->length());
  for (size_t i = 0; i < static_cast<size_t>(timestamp_index->length()); ++i) {
    auto *p0 = result[0].add_data();
    // Convert timestamp to ms (we know it's timestamp from the check above)
    p0->set_x(timestamp_index->Value(i) / 1000000); // Convert ns to ms
    p0->set_y(double_seriesA->Value(i));

    auto *p1 = result[1].add_data();
    // Use same x value for both points
    p1->set_x(p0->x());
    p1->set_y(double_seriesB->Value(i));
  }

  return result;
}

} // namespace epoch_folio
