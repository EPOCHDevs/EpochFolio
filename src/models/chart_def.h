//
// Created by adesola on 1/12/25.
//

#pragma once
#include "table_def.h"
#include <cmath>
#include <epoch_frame/frame_or_series.h>
#include <limits>

namespace epoch_folio {
constexpr const char *kLinearAxisType{"linear"};
constexpr const char *kLogAxisType{"logarithmic"};
constexpr const char *kDateTimeAxisType{"datetime"};
constexpr const char *kCategoryAxisType{"category"};
struct AxisDef {
  std::optional<std::string> type{};
  std::optional<std::string> label{}; // Axis title
  std::vector<std::string> categories{};

  // Chart rendering optimization fields
  std::optional<epoch_frame::Scalar> min{};          // Minimum value for axis
  std::optional<epoch_frame::Scalar> max{};          // Maximum value for axis
  std::optional<epoch_frame::Scalar> tickInterval{}; // Interval between ticks
  std::optional<std::string>
      labelFormat{}; // Format string for axis labels (e.g., "{value}%")
};

struct ChartDef {
  std::string id;
  std::string title;
  epoch_core::EpochFolioDashboardWidget type;
  epoch_core::EpochFolioCategory category;
  std::optional<AxisDef> yAxis{AxisDef{.type = kLinearAxisType}};
  std::optional<AxisDef> xAxis{AxisDef{.type = kDateTimeAxisType}};
};

struct StraightLineDef {
  std::string title;
  epoch_frame::Scalar value;
  bool vertical{false};
};
using StraightLines = std::vector<StraightLineDef>;

struct Band {
  epoch_frame::Scalar from, to;
};

struct Point {
  epoch_frame::Scalar x{};
  epoch_frame::Scalar y{};
};
using Points = std::vector<Point>;
struct Line {
  Points data;
  std::string name;
  std::optional<std::string> dashStyle{};
  std::optional<uint32_t> lineWidth{};
};
using SeriesLines = std::vector<Line>;

struct LinesDef {
  ChartDef chartDef;
  SeriesLines lines{};
  StraightLines straightLines{};
  std::vector<Band> yPlotBands{};
  std::vector<Band> xPlotBands{};
  std::optional<Line> overlay{};
  bool stacked{false};
};

using HeatMapPoint = std::array<epoch_frame::Scalar, 3>;
using HeatMapPoints = std::vector<HeatMapPoint>;
struct HeatMapDef {
  ChartDef chartDef;
  HeatMapPoints points{};
};

struct BarDef {
  ChartDef chartDef;
  epoch_frame::Array data;
  StraightLines straightLines{};
  std::optional<uint32_t> barWidth{};
};

struct HistogramDef {
  ChartDef chartDef;
  epoch_frame::Array data;
  StraightLines straightLines{};
  std::optional<uint32_t> binsCount{};
};

using BoxPlotOutliers = std::vector<std::tuple<uint64_t, double>>;
struct BoxPlotDataPoint {
  double low{};
  double q1{};
  double median{};
  double q3{};
  double high{};

  static std::pair<BoxPlotDataPoint, BoxPlotOutliers>
  Make(int64_t category_index, const epoch_frame::Series &x);
};
using BoxPlotDataPoints = std::vector<BoxPlotDataPoint>;

struct BoxPlotDataPointDef {
  BoxPlotOutliers outliers;
  BoxPlotDataPoints points;
};

struct BoxPlotDef {
  ChartDef chartDef;
  BoxPlotDataPointDef data;
};

struct XRangePoint {
  epoch_frame::Scalar x{};
  epoch_frame::Scalar x2{};
  size_t y{};
  bool is_long{true};
};

struct XRangeDef {
  ChartDef chartDef;
  std::vector<std::string> categories;
  std::vector<XRangePoint> points;
};

struct PieData {
  std::string name;
  epoch_frame::Scalar y;
};
using PieDataPoints = std::vector<PieData>;

struct PieDataDef {
  std::string name;
  PieDataPoints points{};
  std::string size{};
  std::optional<std::string> innerSize{};
};

struct PieDef {
  ChartDef chartDef;
  std::vector<PieDataDef> data;
};

using Chart = std::variant<LinesDef, HeatMapDef, BarDef, HistogramDef,
                           BoxPlotDef, XRangeDef, PieDef>;

template <typename X, typename Y>
Line MakeSeriesLine(std::vector<X> x, std::vector<Y> y,
                    std::optional<std::string> const &name = std::nullopt) {
  Line line;
  line.data = std::vector<Point>(x.size());
  for (size_t i = 0; i < x.size(); ++i) {
    line.data[i] = Point{epoch_frame::Scalar{std::move(x[i])},
                         epoch_frame::Scalar{std::move(y[i])}};
  }
  line.name = name.value_or("");
  return line;
}

SeriesLines MakeSeriesLines(const epoch_frame::DataFrame &df);
Line MakeSeriesLine(const epoch_frame::Series &series,
                    std::optional<std::string> const &name = std::nullopt);
SeriesLines
MakeSeriesLines(const epoch_frame::Series &seriesA,
                const epoch_frame::Series &seriesB,
                std::optional<std::string> const &nameA = std::nullopt,
                std::optional<std::string> const &nameB = std::nullopt);

// Helper functions to create common axis configurations
inline AxisDef
MakeLinearAxis(std::optional<std::string> label = std::nullopt,
               std::optional<double> min = std::nullopt,
               std::optional<double> max = std::nullopt,
               std::optional<double> tickInterval = std::nullopt,
               std::optional<std::string> labelFormat = std::nullopt) {
  return AxisDef{
      .type = kLinearAxisType,
      .label = label,
      .categories = {},
      .min = min.has_value() ? std::optional<epoch_frame::Scalar>{*min}
                             : std::nullopt,
      .max = max.has_value() ? std::optional<epoch_frame::Scalar>{*max}
                             : std::nullopt,
      .tickInterval = tickInterval.has_value()
                          ? std::optional<epoch_frame::Scalar>{*tickInterval}
                          : std::nullopt,
      .labelFormat = labelFormat};
}

inline AxisDef
MakeDateTimeAxis(std::optional<std::string> label = std::nullopt,
                 std::optional<int64_t> minTimestamp = std::nullopt,
                 std::optional<int64_t> maxTimestamp = std::nullopt,
                 std::optional<int64_t> tickIntervalMs = std::nullopt,
                 std::optional<std::string> labelFormat = std::nullopt) {
  return AxisDef{.type = kDateTimeAxisType,
                 .label = label,
                 .categories = {},
                 .min = minTimestamp.has_value()
                            ? std::optional<epoch_frame::Scalar>{*minTimestamp}
                            : std::nullopt,
                 .max = maxTimestamp.has_value()
                            ? std::optional<epoch_frame::Scalar>{*maxTimestamp}
                            : std::nullopt,
                 .tickInterval =
                     tickIntervalMs.has_value()
                         ? std::optional<epoch_frame::Scalar>{*tickIntervalMs}
                         : std::nullopt,
                 .labelFormat = labelFormat};
}

inline AxisDef
MakePercentageAxis(std::optional<std::string> label = std::nullopt,
                   std::optional<double> min = std::nullopt,
                   std::optional<double> max = std::nullopt,
                   std::optional<double> tickInterval = std::nullopt) {
  return MakeLinearAxis(label, min, max, tickInterval, "{value}%");
}

// Helper functions to compute axis bounds from data
struct AxisBounds {
  double min;
  double max;
  double tickInterval;
};

inline AxisBounds ComputeAxisBounds(const epoch_frame::Series &series,
                                    double paddingRatio = 0.05) {
  double dataMin = series.min().cast_double().as_double();
  double dataMax = series.max().cast_double().as_double();
  double range = dataMax - dataMin;
  double padding = range * paddingRatio;

  // Calculate nice tick interval based on range
  double tickInterval = std::pow(10, std::floor(std::log10(range))) / 2.0;

  return {dataMin - padding, dataMax + padding, tickInterval};
}

inline AxisBounds
ComputeAxisBounds(const std::vector<epoch_frame::Series> &seriesList,
                  double paddingRatio = 0.05) {
  double dataMin = std::numeric_limits<double>::max();
  double dataMax = std::numeric_limits<double>::lowest();

  for (const auto &series : seriesList) {
    dataMin = std::min(dataMin, series.min().cast_double().as_double());
    dataMax = std::max(dataMax, series.max().cast_double().as_double());
  }

  double range = dataMax - dataMin;
  double padding = range * paddingRatio;

  // Calculate nice tick interval based on range
  double tickInterval = std::pow(10, std::floor(std::log10(range))) / 2.0;

  return {dataMin - padding, dataMax + padding, tickInterval};
}

inline AxisBounds ComputePercentageAxisBounds(const epoch_frame::Series &series,
                                              double paddingRatio = 0.05) {
  double dataMin = series.min().cast_double().as_double();
  double dataMax = series.max().cast_double().as_double();
  double range = dataMax - dataMin;
  double padding = range * paddingRatio;

  // Nice tick interval for percentages
  double tickInterval = std::abs(range) > 50 ? 10.0 : 5.0;

  return {dataMin - padding, dataMax + padding, tickInterval};
}

inline AxisDef
MakeAxisFromBounds(const AxisBounds &bounds,
                   std::optional<std::string> label = std::nullopt,
                   std::optional<std::string> labelFormat = std::nullopt) {
  return MakeLinearAxis(label, bounds.min, bounds.max, bounds.tickInterval,
                        labelFormat);
}
} // namespace epoch_folio
