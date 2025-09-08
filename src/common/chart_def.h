//
// Created by adesola on 1/12/25.
//

#pragma once
#include <cmath>
#include <epoch_frame/frame_or_series.h>
#include <epoch_protos/chart_def.pb.h>
#include <epoch_protos/common.pb.h>
#include <epoch_protos/table_def.pb.h>
#include <string>
#include <string_view>

namespace epoch_folio {
constexpr epoch_proto::AxisType kLinearAxisType{epoch_proto::AXIS_TYPE_LINEAR};
constexpr epoch_proto::AxisType kLogAxisType{
    epoch_proto::AXIS_TYPE_LOGARITHMIC};
constexpr epoch_proto::AxisType kDateTimeAxisType{
    epoch_proto::AXIS_TYPE_DATETIME};
constexpr epoch_proto::AxisType kCategoryAxisType{
    epoch_proto::AXIS_TYPE_CATEGORY};

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
  s.set_double_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(float v) {
  epoch_proto::Scalar s;
  s.set_double_value(static_cast<double>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(int64_t v) {
  epoch_proto::Scalar s;
  s.set_int64_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(int32_t v) {
  epoch_proto::Scalar s;
  s.set_int64_value(static_cast<int64_t>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(uint64_t v) {
  epoch_proto::Scalar s;
  s.set_uint64_value(v);
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(uint32_t v) {
  epoch_proto::Scalar s;
  s.set_uint64_value(static_cast<uint64_t>(v));
  return s;
}
inline epoch_proto::Scalar ToProtoScalarValue(bool v) {
  epoch_proto::Scalar s;
  s.set_bool_value(v);
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

inline StraightLineDef MakeStraightLine(const std::string &title,
                                        const epoch_frame::Scalar &value,
                                        bool vertical) {
  StraightLineDef out;
  out.set_title(title);
  *out.mutable_value() = ToProtoScalar(value);
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
  for (size_t i = 0; i < n; ++i) {
    auto *p = line.add_data();
    *p->mutable_x() = ToProtoScalarValue(x[i]);
    *p->mutable_y() = ToProtoScalarValue(y[i]);
  }
  return line;
}

} // namespace epoch_folio
