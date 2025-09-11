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
        auto unit = std::static_pointer_cast<arrow::TimestampType>(ts->type)->unit();
        int64_t ms_value;
        switch (unit) {
          case arrow::TimeUnit::NANO:
            ms_value = ts->value / 1000000; // Convert ns to ms
            break;
          case arrow::TimeUnit::MICRO:
            ms_value = ts->value / 1000; // Convert us to ms
            break;
          case arrow::TimeUnit::MILLI:
            ms_value = ts->value; // Already in ms
            break;
          case arrow::TimeUnit::SECOND:
            ms_value = ts->value * 1000; // Convert s to ms
            break;
          default:
            throw std::runtime_error("Unsupported timestamp unit");
        }
        out.set_timestamp_ms(ms_value);
      } else {
        throw std::runtime_error("Invalid timestamp scalar");
      }
      return out;
    }
    case arrow::Type::DATE32: {
      // Date32 stores days since epoch
      auto date = std::static_pointer_cast<arrow::Date32Scalar>(s.value());
      if (date) {
        // Convert days to milliseconds (86400000 ms per day)
        out.set_date_value(static_cast<int64_t>(date->value) * 86400000);
      } else {
        throw std::runtime_error("Invalid date32 scalar");
      }
      return out;
    }
    case arrow::Type::DATE64: {
      // Date64 stores milliseconds since epoch
      auto date = std::static_pointer_cast<arrow::Date64Scalar>(s.value());
      if (date) {
        out.set_date_value(date->value);
      } else {
        throw std::runtime_error("Invalid date64 scalar");
      }
      return out;
    }
    case arrow::Type::DURATION: {
      // Duration type - convert to milliseconds
      auto dur = std::static_pointer_cast<arrow::DurationScalar>(s.value());
      if (dur) {
        auto unit = std::static_pointer_cast<arrow::DurationType>(dur->type)->unit();
        int64_t ms_value;
        switch (unit) {
          case arrow::TimeUnit::NANO:
            ms_value = dur->value / 1000000;
            break;
          case arrow::TimeUnit::MICRO:
            ms_value = dur->value / 1000;
            break;
          case arrow::TimeUnit::MILLI:
            ms_value = dur->value;
            break;
          case arrow::TimeUnit::SECOND:
            ms_value = dur->value * 1000;
            break;
          default:
            throw std::runtime_error("Unsupported duration unit");
        }
        out.set_duration_ms(ms_value);
      } else {
        throw std::runtime_error("Invalid duration scalar");
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
      auto contiguous_array = column.contiguous_array();
      auto array_type = contiguous_array.type()->id();

      // Handle different numeric types
      if (array_type == arrow::Type::INT64) {
        auto int_column = contiguous_array.to_view<int64_t>();
        for (size_t row = 0; row < df.num_rows(); ++row) {
          auto *p = result[i].add_data();
          p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
          p->set_y(static_cast<double>(int_column->Value(row)));
        }
      } else if (array_type == arrow::Type::INT32) {
        auto int_column = contiguous_array.to_view<int32_t>();
        for (size_t row = 0; row < df.num_rows(); ++row) {
          auto *p = result[i].add_data();
          p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
          p->set_y(static_cast<double>(int_column->Value(row)));
        }
      } else if (array_type == arrow::Type::DOUBLE) {
        auto double_column = contiguous_array.to_view<double>();
        for (size_t row = 0; row < df.num_rows(); ++row) {
          auto *p = result[i].add_data();
          p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
          p->set_y(double_column->Value(row));
        }
      } else if (array_type == arrow::Type::FLOAT) {
        auto float_column = contiguous_array.to_view<float>();
        for (size_t row = 0; row < df.num_rows(); ++row) {
          auto *p = result[i].add_data();
          p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
          p->set_y(static_cast<double>(float_column->Value(row)));
        }
      } else {
        // Fallback: convert column to double first
        auto double_series = column.cast(arrow::float64());
        auto double_column = double_series.contiguous_array().to_view<double>();
        for (size_t row = 0; row < df.num_rows(); ++row) {
          auto *p = result[i].add_data();
          p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
          p->set_y(double_column->Value(row));
        }
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
  auto contiguous_array = column.contiguous_array();
  
  line.mutable_data()->Reserve(df.num_rows());
  
  // Handle both integer and double types
  auto array_type = contiguous_array.type()->id();
  if (array_type == arrow::Type::INT64) {
    auto int_column = contiguous_array.to_view<int64_t>();
    for (size_t row = 0; row < df.num_rows(); ++row) {
      auto *p = line.add_data();
      p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
      p->set_y(static_cast<double>(int_column->Value(row)));
    }
  } else if (array_type == arrow::Type::INT32) {
    auto int_column = contiguous_array.to_view<int32_t>();
    for (size_t row = 0; row < df.num_rows(); ++row) {
      auto *p = line.add_data();
      p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
      p->set_y(static_cast<double>(int_column->Value(row)));
    }
  } else if (array_type == arrow::Type::DOUBLE) {
    auto double_column = contiguous_array.to_view<double>();
    for (size_t row = 0; row < df.num_rows(); ++row) {
      auto *p = line.add_data();
      p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
      p->set_y(double_column->Value(row));
    }
  } else if (array_type == arrow::Type::FLOAT) {
    auto float_column = contiguous_array.to_view<float>();
    for (size_t row = 0; row < df.num_rows(); ++row) {
      auto *p = line.add_data();
      p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
      p->set_y(static_cast<double>(float_column->Value(row)));
    }
  } else {
    // Fallback: convert series to double first
    auto double_series = column.cast(arrow::float64());
    auto double_column = double_series.contiguous_array().to_view<double>();
    for (size_t row = 0; row < df.num_rows(); ++row) {
      auto *p = line.add_data();
      p->set_x(timestamp_index->Value(row) / 1000000); // Convert ns to ms
      p->set_y(double_column->Value(row));
    }
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

  auto columnA = nameA.value_or(seriesA.name().value_or(""));
  auto columnB = nameB.value_or(seriesB.name().value_or(""));

  SeriesLines result(2);
  result[0].set_name(columnA);
  result[1].set_name(columnB);

  result[0].mutable_data()->Reserve(timestamp_index->length());
  result[1].mutable_data()->Reserve(timestamp_index->length());
  
  // Helper lambda to get double value from series at index
  auto get_double_value = [](const epoch_frame::Series& series, size_t idx) -> double {
    auto array = series.contiguous_array();
    auto type_id = array.type()->id();
    
    switch(type_id) {
      case arrow::Type::INT64:
        return static_cast<double>(array.to_view<int64_t>()->Value(idx));
      case arrow::Type::INT32:
        return static_cast<double>(array.to_view<int32_t>()->Value(idx));
      case arrow::Type::DOUBLE:
        return array.to_view<double>()->Value(idx);
      case arrow::Type::FLOAT:
        return static_cast<double>(array.to_view<float>()->Value(idx));
      default:
        // Fallback: get scalar and convert
        return series.iloc(idx).as_double();
    }
  };
  
  for (size_t i = 0; i < static_cast<size_t>(timestamp_index->length()); ++i) {
    auto *p0 = result[0].add_data();
    // Convert timestamp to ms (we know it's timestamp from the check above)
    p0->set_x(timestamp_index->Value(i) / 1000000); // Convert ns to ms
    p0->set_y(get_double_value(seriesA, i));

    auto *p1 = result[1].add_data();
    // Use same x value for both points
    p1->set_x(p0->x());
    p1->set_y(get_double_value(seriesB, i));
  }

  return result;
}

} // namespace epoch_folio
