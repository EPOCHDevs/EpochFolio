//
// Created by adesola on 1/12/25.
//

#pragma once
#include "epoch_frame/datetime.h"
#include <cmath>
#include <epoch_frame/frame_or_series.h>
#include <epoch_protos/chart_def.pb.h>
#include <epoch_protos/common.pb.h>
#include <epoch_protos/table_def.pb.h>
#include <ranges>
#include <string>
#include <string_view>

namespace epoch_folio {
constexpr epoch_proto::AxisType kLinearAxisType{epoch_proto::AxisLinear};
constexpr epoch_proto::AxisType kLogAxisType{epoch_proto::AxisLogarithmic};
constexpr epoch_proto::AxisType kDateTimeAxisType{epoch_proto::AxisDateTime};
constexpr epoch_proto::AxisType kCategoryAxisType{epoch_proto::AxisCategory};

// Now in proto includes above

using AxisDef = epoch_proto::AxisDef;
using ChartDef = epoch_proto::ChartDef;
using StraightLineDef = epoch_proto::StraightLineDef;
using Band = epoch_proto::Band;
using Point = epoch_proto::Point;
using Line = epoch_proto::Line;
using LinesDef = epoch_proto::LinesDef;
using HeatMapPoint = epoch_proto::HeatMapPoint;
using HeatMapDef = epoch_proto::HeatMapDef;
using BarDef = epoch_proto::BarDef;
using HistogramDef = epoch_proto::HistogramDef;
using BoxPlotDataPoint = epoch_proto::BoxPlotDataPoint;
using BoxPlotDataPointDef = epoch_proto::BoxPlotDataPointDef;
using BoxPlotDef = epoch_proto::BoxPlotDef;
using XRangePoint = epoch_proto::XRangePoint;
using XRangeDef = epoch_proto::XRangeDef;
using PieData = epoch_proto::PieData;
using PieDataDef = epoch_proto::PieDataDef;
using PieDef = epoch_proto::PieDef;
using Chart = epoch_proto::Chart;
using Table = epoch_proto::Table;
using CardDef = epoch_proto::CardDef;

using SeriesLines = std::vector<Line>;

using StraightLines = std::vector<StraightLineDef>;

// Converters and helpers
epoch_proto::Scalar ToProtoScalar(const epoch_frame::Scalar &s);

// Fast path: build proto Scalar directly from primitives without
// constructing epoch_frame::Scalar first
inline epoch_proto::Scalar ToProtoScalarValue(double v) {
  epoch_proto::Scalar s;
  s.set_decimal_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(float v) {
  epoch_proto::Scalar s;
  s.set_decimal_value(static_cast<double>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(int64_t v) {
  epoch_proto::Scalar s;
  s.set_integer_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(int32_t v) {
  epoch_proto::Scalar s;
  s.set_integer_value(static_cast<int64_t>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(uint64_t v) {
  epoch_proto::Scalar s;
  s.set_integer_value(static_cast<int64_t>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(uint32_t v) {
  epoch_proto::Scalar s;
  s.set_integer_value(static_cast<int64_t>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(bool v) {
  epoch_proto::Scalar s;
  s.set_boolean_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(const std::string &v) {
  epoch_proto::Scalar s;
  s.set_string_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(std::string_view v) {
  epoch_proto::Scalar s;
  s.set_string_value(std::string(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(std::nullptr_t) {
  epoch_proto::Scalar s;
  s.set_null_value(google::protobuf::NULL_VALUE);
  return s;
}

// Date types - use date_value field (stores as SECONDS since epoch for TypeDate)
inline epoch_proto::Scalar ToProtoScalarDate(epoch_frame::Date const &v) {
  epoch_proto::Scalar s;
  // Convert date to SECONDS since epoch for TypeDate fields
  // (ordinal is days since epoch)
  s.set_date_value(static_cast<int64_t>(v.toordinal()) * 86400);
  return s;
}

// DateTime/Timestamp types - use timestamp_ms field
inline epoch_proto::Scalar
ToProtoScalarTimestamp(epoch_frame::DateTime const &v) {
  epoch_proto::Scalar s;
  // Convert nanoseconds to milliseconds
  s.set_timestamp_ms(v.m_nanoseconds.count() / 1000000);
  return s;
}

// Convert scalar (which may be a timestamp) to proper timestamp format
inline epoch_proto::Scalar ToProtoScalarTimestampFromScalar(
    const epoch_frame::Scalar &scalar) {
  epoch_proto::Scalar s;
  // If it's already a timestamp type, convert properly
  if (scalar.type()->id() == arrow::Type::TIMESTAMP) {
    // The scalar holds nanoseconds, convert to milliseconds
    s.set_timestamp_ms(scalar.as_int64() / 1000000);
  } else {
    // Try to interpret as timestamp value
    s.set_timestamp_ms(scalar.as_int64());
  }
  return s;
}

// Convert Date to timestamp in milliseconds (for use in charts/bands)
inline int64_t DateToTimestampMs(const epoch_frame::Date &date) {
  // Convert days since epoch to milliseconds since epoch
  return static_cast<int64_t>(date.toordinal()) * 86400000;
}

// Duration in milliseconds - use duration_ms field
inline epoch_proto::Scalar ToProtoScalarDurationMs(int64_t milliseconds) {
  epoch_proto::Scalar s;
  s.set_duration_ms(milliseconds);
  return s;
}

// Day duration - use day_duration field (for durations measured in days)
inline epoch_proto::Scalar ToProtoScalarDayDuration(int32_t days) {
  epoch_proto::Scalar s;
  s.set_day_duration(days);
  return s;
}

// Percentage - use percent_value field
inline epoch_proto::Scalar ToProtoScalarPercent(double percent) {
  epoch_proto::Scalar s;
  s.set_percent_value(percent);
  return s;
}

// Monetary value - use monetary_value field
inline epoch_proto::Scalar ToProtoScalarMonetary(double amount) {
  epoch_proto::Scalar s;
  s.set_monetary_value(amount);
  return s;
}

inline StraightLineDef MakeStraightLine(const std::string &title,
                                        const epoch_frame::Scalar &value,
                                        bool vertical) {
  StraightLineDef out;
  out.set_title(title);

  // Handle both integer and double types
  auto type_id = value.type()->id();
  if (type_id == arrow::Type::INT64) {
    out.set_value(static_cast<double>(value.as_int64()));
  } else if (type_id == arrow::Type::INT32) {
    out.set_value(static_cast<double>(value.cast_int32().as_int32()));
  } else {
    out.set_value(value.as_double());
  }

  out.set_vertical(vertical);
  return out;
}

// Builders for axes
inline AxisDef MakeLinearAxis(std::optional<std::string> label = std::nullopt) {
  AxisDef axis;
  axis.set_type(kLinearAxisType);
  if (label)
    axis.set_label(*label);
  return axis;
}

inline AxisDef
MakeDateTimeAxis(std::optional<std::string> label = std::nullopt) {
  AxisDef axis;
  axis.set_type(kDateTimeAxisType);
  if (label)
    axis.set_label(*label);
  return axis;
}

inline AxisDef
MakePercentageAxis(std::optional<std::string> label = std::nullopt) {
  return MakeLinearAxis(label);
}

// Series/Line helpers
SeriesLines MakeSeriesLines(const epoch_frame::DataFrame &df);
Line MakeSeriesLine(const epoch_frame::Series &series,
                    std::optional<std::string> const &name);
SeriesLines MakeSeriesLines(const epoch_frame::Series &seriesA,
                            const epoch_frame::Series &seriesB,
                            std::optional<std::string> const &nameA,
                            std::optional<std::string> const &nameB);

template <typename X, typename Y>
Line MakeSeriesLine(std::vector<X> x, std::vector<Y> y,
                    std::optional<std::string> const &name) {
  Line line;
  if (name && !name->empty())
    line.set_name(*name);
  auto n = x.size();
  line.mutable_data()->Reserve(static_cast<int>(n));
  for (auto const &[x_val, y_val] : std::views::zip(x, y)) {
    auto *p = line.add_data();
    p->set_x(static_cast<int64_t>(x_val));
    p->set_y(static_cast<double>(y_val));
  }
  return line;
}

// Chart-specific data helpers for proper timestamp formatting
// XRange chart helpers
inline XRangePoint MakeXRangePoint(const epoch_frame::Scalar &timestamp_scalar,
                                   size_t category_y, bool is_long = false) {
  XRangePoint point;
  int64_t timestamp_ms = timestamp_scalar.timestamp().value / 1000000;  // nanoseconds to milliseconds
  point.set_x(timestamp_ms);
  point.set_x2(timestamp_ms);  // For point-like ranges
  point.set_y(category_y);
  point.set_is_long(is_long);
  return point;
}

inline XRangePoint MakeXRangePoint(const epoch_frame::DateTime &datetime,
                                   size_t category_y, bool is_long = false) {
  XRangePoint point;
  int64_t timestamp_ms = datetime.m_nanoseconds.count() / 1000000;  // nanoseconds to milliseconds
  point.set_x(timestamp_ms);
  point.set_x2(timestamp_ms);  // For point-like ranges
  point.set_y(category_y);
  point.set_is_long(is_long);
  return point;
}

// Line chart data helpers
inline Point MakeLinePoint(const epoch_frame::Scalar &timestamp_scalar, double y_value) {
  Point point;
  int64_t timestamp_ms = timestamp_scalar.timestamp().value / 1000000;  // nanoseconds to milliseconds
  point.set_x(timestamp_ms);
  point.set_y(y_value);
  return point;
}

inline Point MakeLinePoint(const epoch_frame::DateTime &datetime, double y_value) {
  Point point;
  int64_t timestamp_ms = datetime.m_nanoseconds.count() / 1000000;  // nanoseconds to milliseconds
  point.set_x(timestamp_ms);
  point.set_y(y_value);
  return point;
}

inline Point MakeLinePoint(int64_t timestamp_ms, double y_value) {
  Point point;
  point.set_x(timestamp_ms);
  point.set_y(y_value);
  return point;
}

// Helper to convert month string (YYYY-MM) to mid-month timestamp in milliseconds
inline int64_t MonthStringToTimestampMs(const std::string &month_str) {
  std::string date_str = month_str + "-15T12:00:00";  // Mid-month timestamp
  auto datetime = epoch_frame::DateTime::from_str(date_str, "UTC", "%Y-%m-%dT%H:%M:%S");
  return datetime.m_nanoseconds.count() / 1000000;  // Convert to milliseconds
}

} // namespace epoch_folio
