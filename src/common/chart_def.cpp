#include "chart_def.h"
#include <tbb/tbb.h>
namespace epoch_folio {
constexpr size_t kParallelThreshold = 10;

epoch_proto::Scalar ToProtoScalar(const epoch_frame::Scalar &s) {
  epoch_proto::Scalar out;
  auto t = s.type();
  // Order by most common usage first
  if (t->id() == arrow::Type::DOUBLE) {
    out.set_double_value(s.as_double());
    return out;
  }
  if (t->id() == arrow::Type::FLOAT) {
    out.set_double_value(static_cast<double>(s.cast_float().as_double()));
    return out;
  }
  if (t->id() == arrow::Type::INT64) {
    out.set_int64_value(s.as_int64());
    return out;
  }
  if (t->id() == arrow::Type::INT32) {
    out.set_int64_value(static_cast<int64_t>(s.cast_int32().as_int32()));
    return out;
  }
  if (t->id() == arrow::Type::UINT64) {
    out.set_uint64_value(static_cast<uint64_t>(s.cast_uint64().as_int64()));
    return out;
  }
  if (t->id() == arrow::Type::UINT32) {
    out.set_uint64_value(static_cast<uint64_t>(s.cast_uint32().as_int32()));
    return out;
  }
  if (t->id() == arrow::Type::STRING || t->id() == arrow::Type::BINARY ||
      t->id() == arrow::Type::LARGE_STRING) {
    out.set_string_value(s.repr());
    return out;
  }
  if (t->id() == arrow::Type::BOOL) {
    out.set_bool_value(s.as_bool());
    return out;
  }
  if (t->id() == arrow::Type::TIMESTAMP) {
    // epoch_frame::Scalar repr holds ns timestamp; prefer direct if possible
    auto ts = s.cast(arrow::timestamp(arrow::TimeUnit::NANO)).as_int64();
    out.set_timestamp_nanos(static_cast<uint64_t>(ts));
    return out;
  }
  // Fallback to repr as string
  out.set_string_value(s.repr());
  return out;
}

SeriesLines MakeSeriesLines(const epoch_frame::DataFrame &df) {
  auto index = df.index()->array();
  SeriesLines result(df.num_cols());

  auto inner_loop = [](auto start, auto end, auto &result, auto &df,
                       auto &index) {
    for (size_t i = start; i < end; ++i) {
      const auto &col = df.table()->field(i)->name();
      result[i].mutable_data()->Reserve(df.num_rows());
      result[i].set_name(col);
      epoch_frame::Series column = df[col];
      for (size_t row = 0; row < df.num_rows(); ++row) {
        auto *p = result[i].add_data();
        *p->mutable_x() = ToProtoScalar(index[row]);
        *p->mutable_y() = ToProtoScalar(column.iloc(row));
      }
    }
  };

  if (df.num_cols() < kParallelThreshold) {
    inner_loop(0, df.num_cols(), result, df, index);
  } else {
    tbb::parallel_for(tbb::blocked_range<size_t>(0, df.num_cols()),
                      [&](const tbb::blocked_range<size_t> &range) {
                        inner_loop(range.begin(), range.end(), result, df,
                                   index);
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
  auto index = df.index()->array();
  epoch_frame::Series column = df[df.table()->field(0)->name()];
  line.mutable_data()->Reserve(df.num_rows());
  for (size_t row = 0; row < df.num_rows(); ++row) {
    auto *p = line.add_data();
    *p->mutable_x() = ToProtoScalar(index[row]);
    *p->mutable_y() = ToProtoScalar(column.iloc(row));
  }
  return line;
}

SeriesLines MakeSeriesLines(const epoch_frame::Series &seriesA,
                            const epoch_frame::Series &seriesB,
                            std::optional<std::string> const &nameA,
                            std::optional<std::string> const &nameB) {
  AssertFromFormat(seriesA.index()->equals(seriesB.index()),
                   "Series A and B must have the same index");

  auto index = seriesA.index()->array();

  auto columnA = nameA.value_or(seriesA.name().value_or(""));
  auto columnB = nameB.value_or(seriesB.name().value_or(""));

  SeriesLines result(2);
  result[0].set_name(columnA);
  result[1].set_name(columnB);

  result[0].mutable_data()->Reserve(index.length());
  result[1].mutable_data()->Reserve(index.length());
  for (size_t i = 0; i < static_cast<size_t>(index.length()); ++i) {
    auto *p0 = result[0].add_data();
    *p0->mutable_x() = ToProtoScalar(index[i]);
    *p0->mutable_y() = ToProtoScalar(seriesA.iloc(i));
    auto *p1 = result[1].add_data();
    *p1->mutable_x() = ToProtoScalar(index[i]);
    *p1->mutable_y() = ToProtoScalar(seriesB.iloc(i));
  }

  return result;
}

} // namespace epoch_folio
