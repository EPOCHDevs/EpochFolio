#include "gap_report.h"
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/table.h>
#include <ctime>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_metadata/metadata_options.h>
#include <spdlog/spdlog.h>

#include "common/chart_def.h"
#include "common/table_helpers.h"

namespace epoch_folio {

// Static metadata definition
ReportMetadata GapReport::s_metadata = {
    .id = "gap_report",
    .displayName = "Price Gap Analysis",
    .summary =
        "Analyzes opening price gaps, their fills, and patterns over time",
    .category = epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS,
    .tags = {"gaps", "overnight", "price-action", "fill-analysis",
             "market-microstructure"},
    .requiredColumns = {},
    .typicalOutputs = {},
    .defaultOptions = {},
    .version = "0.1.0",
    .owner = "epoch"};

// Original detailed requiredColumns preserved for later implementation:
/*
    .requiredColumns =
        {{"gap_up", "Gap Up", epoch_proto::EPOCH_FOLIO_TYPE_BOOLEAN},
         {"gap_down", "Gap Down", epoch_proto::EPOCH_FOLIO_TYPE_BOOLEAN},
         {"gap_up_filled", "Gap Up Filled",
          epoch_proto::EPOCH_FOLIO_TYPE_BOOLEAN},
         {"gap_down_filled", "Gap Down Filled",
          epoch_proto::EPOCH_FOLIO_TYPE_BOOLEAN},
         {"gap_up_size", "Gap Up Size", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"gap_down_size", "Gap Down Size",
          epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"gap_up_fraction", "Gap Up Fraction",
          epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"gap_down_fraction", "Gap Down Fraction",
          epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"o", "Open", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"h", "High", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"l", "Low", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"c", "Close", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL},
         {"v", "Volume", epoch_proto::EPOCH_FOLIO_TYPE_DECIMAL}},
    .typicalOutputs = {epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_CARD,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_BAR,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_DATA_TABLE,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_XRANGE,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_HISTOGRAM,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_PIE,
                       epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES},
*/

const ReportMetadata &GapReport::metadata() const { return s_metadata; }

epoch_proto::TearSheet
GapReport::generate(const epoch_frame::DataFrame &df) const {
  return generate_impl(df);
}

epoch_proto::TearSheet
GapReport::generate_impl(const epoch_frame::DataFrame &df) const {
  epoch_proto::TearSheet result;

  auto filtered_gaps = filter_gaps(df);
  if (filtered_gaps.num_rows() == 0) {
    SPDLOG_WARN("No gaps found after filtering");
    return result;
  }

  // 1. Summary cards
  result.cards = compute_summary_cards(filtered_gaps);

  // Helper to read boolean options from metadata
  auto getBoolOpt = [&](std::string const &key, bool defVal) -> bool {
    if (!m_config)
      return defVal;
    try {
      return m_config->GetOptionValue(key).GetBoolean();
    } catch (...) {
      return defVal;
    }
  };
  auto getIntOpt = [&](std::string const &key, int defVal) -> int {
    if (!m_config)
      return defVal;
    try {
      return static_cast<int>(m_config->GetOptionValue(key).GetInteger());
    } catch (...) {
      return defVal;
    }
  };

  bool show_fill_analysis = getBoolOpt("show_fill_analysis", true);
  bool show_day_of_week_analysis =
      getBoolOpt("show_day_of_week_analysis", true);
  bool show_fill_time_analysis = getBoolOpt("show_fill_time_analysis", true);
  bool show_performance_analysis =
      getBoolOpt("show_performance_analysis", true);
  bool show_streak_analysis = getBoolOpt("show_streak_analysis", true);
  bool show_distribution_histogram =
      getBoolOpt("show_distribution_histogram", true);
  int histogram_bins = getIntOpt("histogram_bins", 20);
  int max_table_rows = getIntOpt("max_table_rows", 100);
  int max_streaks = getIntOpt("max_streaks", 5);

  // 2. Fill rate analysis bar chart
  if (show_fill_analysis) {
    Chart chart;
    *chart.mutable_bar_def() =
        create_fill_rate_chart(filtered_gaps, "Gap Fill Analysis");
    result.charts.push_back(std::move(chart));
  }

  // 3. Day of week frequency table
  if (show_day_of_week_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "day_of_week", "Gap Frequency by Day of Week"));
  }

  // 4. Time bucket analysis table
  if (show_fill_time_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "fill_time", "Gap Frequency by Time"));
  }

  // 5. Streak visualization
  if (show_streak_analysis) {
    Chart chart;
    *chart.mutable_x_range_def() =
        create_streak_chart(filtered_gaps, static_cast<uint32_t>(max_streaks));
    result.charts.push_back(std::move(chart));
  }

  // 6. Gap size distribution histogram
  if (show_distribution_histogram) {
    Chart chart;
    *chart.mutable_histogram_def() = create_gap_distribution(
        filtered_gaps, static_cast<uint32_t>(histogram_bins));
    result.charts.push_back(std::move(chart));
  }

  // 7. Performance analysis
  if (show_performance_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "close_performance", "Gap Fill vs Close Performance"));
  }

  // 8. Time distribution pie chart
  Chart chart;
  *chart.mutable_pie_def() = create_time_distribution(filtered_gaps);
  result.charts.push_back(std::move(chart));

  // 9. Gap details table
  result.tables.emplace_back(create_gap_details_table(
      filtered_gaps, static_cast<uint32_t>(max_table_rows)));

  // 10. Trend analysis
  Chart chart2;
  *chart2.mutable_lines_def() = create_gap_trend_chart(filtered_gaps);
  result.charts.push_back(std::move(chart2));

  return result;
}

epoch_frame::DataFrame
GapReport::filter_gaps(const epoch_frame::DataFrame &df) const {
  using namespace epoch_frame;
  using epoch_frame::make_series;
  using epoch_frame::Scalar;

  // Start with full mask (all true)
  auto mask =
      make_series(df.index(), std::vector<bool>(df.num_rows(), true), "mask");

  // Derive common series
  auto is_up = df["gap_up"];
  auto is_down = df["gap_down"];
  // Combine filled flags - if either up or down gap was filled
  auto is_filled_up = df["gap_up_filled"];
  auto is_filled_down = df["gap_down_filled"];
  // Create is_filled manually since we need boolean OR
  auto is_filled = is_filled_up || is_filled_down;
  auto gap_up_pct = df["gap_up_fraction"] * epoch_frame::Scalar{100.0};
  auto gap_down_pct = df["gap_down_fraction"] * epoch_frame::Scalar{100.0};
  // Combine gap percentages - use the non-zero one
  auto gap_pct = gap_up_pct.where(gap_up_pct != Scalar{0.0}, gap_down_pct);
  auto pct_abs = gap_pct.abs();

  // Gap type filter
  bool include_gap_up = true;
  bool include_gap_down = true;
  if (!include_gap_up || !include_gap_down) {
    Series type_mask;
    if (include_gap_up && !include_gap_down) {
      type_mask = is_up;
    } else if (!include_gap_up && include_gap_down) {
      type_mask = is_down;
    } else { // both false => nothing
      type_mask =
          make_series(df.index(), std::vector<bool>(df.num_rows(), false));
    }
    // Apply type mask using element-wise AND
    mask = mask && type_mask;
  }

  // Gap percentage bounds (absolute value)
  // Apply percentage bounds filter
  double min_gap_pct = 0.0;
  double max_gap_pct = 100.0;
  mask = mask && (pct_abs >= Scalar{min_gap_pct}) &&
         (pct_abs <= Scalar{max_gap_pct});

  // Filled / unfilled filter
  bool only_filled = false;
  bool only_unfilled = false;
  if (only_filled && !only_unfilled) {
    mask = mask && is_filled;
  } else if (only_unfilled && !only_filled) {
    mask = mask && !is_filled;
  }

  // Time range filter (index is date-sorted)
  std::optional<int64_t> start_timestamp_ns{};
  std::optional<int64_t> end_timestamp_ns{};
  if (start_timestamp_ns) {
    auto start_date = epoch_frame::Scalar{*start_timestamp_ns};
    // Create a date mask by converting index to Series and comparing
    auto index_series = make_series(
        df.index(), df.index()->array().to_vector<int64_t>(), "index");
    auto date_mask = index_series >= start_date;
    mask = mask && date_mask;
  }
  if (end_timestamp_ns) {
    auto end_date = epoch_frame::Scalar{*end_timestamp_ns};
    // Create a date mask by converting index to Series and comparing
    auto index_series = make_series(
        df.index(), df.index()->array().to_vector<int64_t>(), "index");
    auto date_mask = index_series <= end_date;
    mask = mask && date_mask;
  }

  // Apply the mask
  auto filtered = df.loc(mask);
  auto index_dt = filtered.index()->array().dt();
  auto gap_up = filtered["gap_up"];
  auto gap_down = filtered["gap_down"];

  // Add derived columns to the filtered DataFrame
  SPDLOG_INFO("Adding derived columns to {} rows", filtered.num_rows());
  // Derive day_of_week from index
  std::vector<std::string> day_of_week_data;
  std::vector<std::string> time_bucket_data;
  std::vector<std::string> close_performance_data;

  SPDLOG_INFO("Starting derived column computation loop...");
  double lastSessionEnd = 0;
  for (size_t i = 0; i < static_cast<size_t>(filtered.num_rows()); ++i) {
    bool hasGap = gap_up.iloc(i).as_bool() || gap_down.iloc(i).as_bool();
    if (!hasGap) {
      continue;
    }

    // Extract day of week from timestamp
    auto timestamp_ns = filtered.index()->at(i).as_int64();
    time_t time_seconds = timestamp_ns / 1000000000LL;
    std::tm *tm_info = std::gmtime(&time_seconds);

    // Day of week (0=Sunday, 1=Monday, etc.)
    const char *days[] = {"Sunday",   "Monday", "Tuesday", "Wednesday",
                          "Thursday", "Friday", "Saturday"};
    day_of_week_data.push_back(days[tm_info->tm_wday]);

    // Time bucket (simplified - based on hour)
    time_bucket_data.push_back(
        std::format("{}:{}", tm_info->tm_hour, tm_info->tm_min));

    // Close performance (green if close > open, red otherwise)
    auto close_val = filtered["c"].iloc(i).as_double();
    close_performance_data.push_back(close_val > lastSessionEnd ? "green"
                                                                : "red");
    lastSessionEnd = close_val;
  }

  // Add the derived columns to the DataFrame
  auto day_of_week_series =
      make_series(filtered.index(), day_of_week_data, "day_of_week");
  auto fill_time_series =
      make_series(filtered.index(), time_bucket_data, "fill_time");
  auto close_performance_series = make_series(
      filtered.index(), close_performance_data, "close_performance");

  // Create new DataFrame with additional columns
  std::vector<Series> all_series;
  std::vector<std::string> all_columns;

  // Copy existing columns
  for (size_t i = 0; i < static_cast<size_t>(filtered.num_cols()); ++i) {
    auto col_name = filtered.table()->field(i)->name();
    all_series.push_back(filtered[col_name]);
    all_columns.push_back(col_name);
  }

  // Add derived columns
  all_series.push_back(day_of_week_series);
  all_columns.push_back("day_of_week");
  all_series.push_back(fill_time_series);
  all_columns.push_back("fill_time");
  all_series.push_back(close_performance_series);
  all_columns.push_back("close_performance");

  filtered = MakeDataFrame(all_series, all_columns);

  // last_n_gaps (take from tail because sorted by date ascending)
  std::optional<int> last_n_gaps{};
  if (last_n_gaps && filtered.num_rows() > 0) {
    filtered = filtered.tail(*last_n_gaps);
  }

  return filtered;
}

std::vector<CardDef>
GapReport::compute_summary_cards(const epoch_frame::DataFrame &gaps) const {
  std::vector<CardDef> cards;

  // Total gaps
  CardDef card1;
  card1.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_CARD);
  card1.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  card1.set_group_size(1);
  auto *data1 = card1.add_data();
  data1->set_title("Total Gaps");
  *data1->mutable_value() =
      ToProtoScalar(epoch_frame::Scalar{static_cast<int64_t>(gaps.num_rows())});
  data1->set_type(epoch_proto::EPOCH_FOLIO_TYPE_INTEGER);
  cards.push_back(std::move(card1));

  // Count gap types using boolean columns
  auto gap_up_count = gaps["gap_up"].sum().cast_int64().as_int64();
  auto gap_down_count = gaps["gap_down"].sum().cast_int64().as_int64();

  // Count filled gaps
  auto filled_count = gaps["gap_up_filled"].sum().cast_int64().as_int64() +
                      gaps["gap_down_filled"].sum().cast_int64().as_int64();

  // Calculate gap percentages from fractions
  auto gap_up_pct = gaps["gap_up_fraction"] * epoch_frame::Scalar{100.0};
  auto total_gap_pct = gap_up_pct.abs().sum().as_double();
  auto max_gap_pct = gap_up_pct.abs().max().as_double();

  // Gap up/down counts
  CardDef card2;
  card2.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_CARD);
  card2.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  card2.set_group_size(2);

  auto *data2a = card2.add_data();
  data2a->set_title("Gap Up");
  *data2a->mutable_value() = ToProtoScalar(epoch_frame::Scalar{gap_up_count});
  data2a->set_type(epoch_proto::EPOCH_FOLIO_TYPE_INTEGER);
  data2a->set_group(1);

  auto *data2b = card2.add_data();
  data2b->set_title("Gap Down");
  *data2b->mutable_value() = ToProtoScalar(epoch_frame::Scalar{gap_down_count});
  data2b->set_type(epoch_proto::EPOCH_FOLIO_TYPE_INTEGER);
  data2b->set_group(1);

  cards.push_back(std::move(card2));

  // Fill rate
  double fill_rate = gaps.num_rows() > 0 ? static_cast<double>(filled_count) /
                                               gaps.num_rows() * 100
                                         : 0;

  CardDef card3;
  card3.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_CARD);
  card3.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  card3.set_group_size(1);
  auto *data3 = card3.add_data();
  data3->set_title("Overall Fill Rate");
  *data3->mutable_value() = ToProtoScalar(epoch_frame::Scalar{fill_rate});
  data3->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);
  cards.push_back(std::move(card3));

  // Average and max gap size
  double avg_gap_pct =
      gaps.num_rows() > 0 ? total_gap_pct / gaps.num_rows() : 0;

  CardDef card4;
  card4.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_CARD);
  card4.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  card4.set_group_size(2);

  auto *data4a = card4.add_data();
  data4a->set_title("Avg Gap %");
  *data4a->mutable_value() = ToProtoScalar(epoch_frame::Scalar{avg_gap_pct});
  data4a->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);
  data4a->set_group(2);

  auto *data4b = card4.add_data();
  data4b->set_title("Max Gap %");
  *data4b->mutable_value() = ToProtoScalar(epoch_frame::Scalar{max_gap_pct});
  data4b->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);
  data4b->set_group(2);

  cards.push_back(std::move(card4));

  return cards;
}

BarDef GapReport::create_fill_rate_chart(const epoch_frame::DataFrame &gaps,
                                         const std::string &title) const {
  // Count gap types and fills using boolean columns
  auto gap_up_mask = gaps["gap_up"];
  auto gap_down_mask = gaps["gap_down"];
  auto gap_up_filled_mask = gaps["gap_up_filled"];
  auto gap_down_filled_mask = gaps["gap_down_filled"];

  auto gap_up_count = gap_up_mask.sum().cast_int64().as_int64();
  auto gap_down_count = gap_down_mask.sum().cast_int64().as_int64();
  auto gap_up_filled = gap_up_filled_mask.sum().cast_int64().as_int64();
  auto gap_down_filled = gap_down_filled_mask.sum().cast_int64().as_int64();

  double gap_up_fill_rate =
      gap_up_count > 0 ? static_cast<double>(gap_up_filled) / gap_up_count * 100
                       : 0;
  double gap_down_fill_rate =
      gap_down_count > 0
          ? static_cast<double>(gap_down_filled) / gap_down_count * 100
          : 0;

  // Create bar chart data
  arrow::DoubleBuilder builder;
  ARROW_UNUSED(builder.Append(gap_up_fill_rate));
  ARROW_UNUSED(builder.Append(gap_down_fill_rate));
  std::shared_ptr<arrow::Array> data_array;
  ARROW_UNUSED(builder.Finish(&data_array));

  BarDef bar_def;

  // Set up chart definition
  auto *chart_def = bar_def.mutable_chart_def();
  chart_def->set_id("gap_fill_rates");
  chart_def->set_title(title);
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_BAR);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);

  // Set up axes
  *chart_def->mutable_y_axis() = MakePercentageAxis("Fill Rate (%)");

  auto *x_axis = chart_def->mutable_x_axis();
  x_axis->set_type(kCategoryAxisType);
  x_axis->add_categories("Gap Up");
  x_axis->add_categories("Gap Down");

  // Set data
  // TODO: Convert Arrow array to protobuf scalar/array type properly
  // Temporarily disabled to unblock compilation
  // *bar_def.mutable_data() = ToProtoScalar(epoch_frame::Array{data_array});

  return bar_def;
}

Table GapReport::create_frequency_table(const epoch_frame::DataFrame &gaps,
                                        const std::string &category_col,
                                        const std::string &title) const {
  // Use group_by_agg for frequency counting
  auto grouped = gaps[std::vector<std::string>{category_col}]
                     .group_by_agg(category_col)
                     .count();

  std::unordered_map<std::string, int64_t> counts;
  for (size_t i = 0; i < static_cast<size_t>(grouped.num_rows()); ++i) {
    auto category = grouped[category_col].iloc(i).repr();
    auto count = grouped["count"].iloc(i).as_int64();
    counts[category] = count;
  }

  // Build table data
  arrow::StringBuilder category_builder;
  arrow::Int64Builder frequency_builder;
  arrow::DoubleBuilder percentage_builder;

  auto total = gaps.num_rows();
  for (const auto &[cat, count] : counts) {
    ARROW_UNUSED(category_builder.Append(cat));
    ARROW_UNUSED(frequency_builder.Append(count));
    double pct = total > 0 ? count * 100.0 / total : 0;
    ARROW_UNUSED(percentage_builder.Append(pct));
  }

  std::shared_ptr<arrow::Array> category_array, frequency_array,
      percentage_array;
  ARROW_UNUSED(category_builder.Finish(&category_array));
  ARROW_UNUSED(frequency_builder.Finish(&frequency_array));
  ARROW_UNUSED(percentage_builder.Finish(&percentage_array));

  auto schema = arrow::schema({arrow::field("Category", arrow::utf8()),
                               arrow::field("Frequency", arrow::int64()),
                               arrow::field("Percentage", arrow::float64())});

  auto table = arrow::Table::Make(
      schema, {category_array, frequency_array, percentage_array});

  Table result_table;
  result_table.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_DATA_TABLE);
  result_table.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  result_table.set_title(title);

  // Add columns
  auto *col1 = result_table.add_columns();
  col1->set_name("Category");
  // col1->set_display_name("Category");
  col1->set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);

  auto *col2 = result_table.add_columns();
  col2->set_name("Frequency");
  // col2->set_display_name("Frequency");
  col2->set_type(epoch_proto::EPOCH_FOLIO_TYPE_INTEGER);

  auto *col3 = result_table.add_columns();
  col3->set_name("Percentage");
  // col3->set_display_name("Percentage");
  col3->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);

  // Set data
  *result_table.mutable_data() = MakeTableDataFromArrow(table);

  return result_table;
}

XRangeDef GapReport::create_streak_chart(const epoch_frame::DataFrame &gaps,
                                         uint32_t max_streaks) const {
  std::vector<XRangePoint> points;
  std::vector<std::string> categories = {"Gap Up Streaks", "Gap Down Streaks"};

  // Collect last N gaps of each type using boolean columns
  std::vector<std::pair<int64_t, bool>> gap_up_list; // (index, is_filled)
  std::vector<std::pair<int64_t, bool>> gap_down_list;

  for (size_t i = 0; i < static_cast<size_t>(gaps.num_rows()); ++i) {
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto is_gap_down = gaps["gap_down"].iloc(i).as_bool();
    auto is_up_filled = gaps["gap_up_filled"].iloc(i).as_bool();
    auto is_down_filled = gaps["gap_down_filled"].iloc(i).as_bool();

    if (is_gap_up) {
      gap_up_list.push_back({i, is_up_filled});
    }
    if (is_gap_down) {
      gap_down_list.push_back({i, is_down_filled});
    }
  }

  // Take last N of each type
  auto add_points = [&](const std::vector<std::pair<int64_t, bool>> &list,
                        size_t category_idx) {
    size_t start = list.size() > max_streaks ? list.size() - max_streaks : 0;
    for (size_t j = start; j < list.size(); ++j) {
      auto [idx, is_filled] = list[j];
      auto date = gaps.index()->at(idx);

      XRangePoint point;
      *point.mutable_x() = ToProtoScalar(date);
      *point.mutable_x2() = ToProtoScalar(date);
      point.set_y(category_idx);
      point.set_is_long(is_filled);
      points.push_back(std::move(point));
    }
  };

  add_points(gap_up_list, 0);
  add_points(gap_down_list, 1);

  XRangeDef xrange_def;

  // Set up chart definition
  auto *chart_def = xrange_def.mutable_chart_def();
  chart_def->set_id("gap_streaks");
  chart_def->set_title("Recent Gap Streaks");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_XRANGE);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);

  // Set up axes
  auto *y_axis = chart_def->mutable_y_axis();
  y_axis->set_type(kCategoryAxisType);
  for (const auto &cat : categories) {
    y_axis->add_categories(cat);
  }

  *chart_def->mutable_x_axis() = MakeDateTimeAxis();

  // Set categories and points
  for (const auto &cat : categories) {
    xrange_def.add_categories(cat);
  }

  for (auto &point : points) {
    *xrange_def.add_points() = std::move(point);
  }

  return xrange_def;
}

HistogramDef
GapReport::create_gap_distribution(const epoch_frame::DataFrame &gaps,
                                   uint32_t bins) const {
  // Extract gap percentages from fractions
  auto gap_up_pct = gaps["gap_up_fraction"] * epoch_frame::Scalar{100.0};
  auto abs_gap_pct = gap_up_pct.abs();

  auto data_array = abs_gap_pct.array()->chunk(0);

  HistogramDef histogram_def;

  // Set up chart definition
  auto *chart_def = histogram_def.mutable_chart_def();
  chart_def->set_id("gap_distribution");
  chart_def->set_title("Gap Size Distribution");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_HISTOGRAM);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);

  // Set up axes
  *chart_def->mutable_y_axis() = MakeLinearAxis("Frequency");
  *chart_def->mutable_x_axis() = MakePercentageAxis("Gap Size (%)");

  // Set data and bins
  // TODO: Convert Arrow array to protobuf scalar/array type properly
  // Temporarily disabled to unblock compilation
  // *histogram_def.mutable_data() =
  // ToProtoScalar(epoch_frame::Array{data_array});
  histogram_def.set_bins_count(bins);

  return histogram_def;
}

PieDef
GapReport::create_time_distribution(const epoch_frame::DataFrame &gaps) const {
  // Derive time buckets from index and count
  std::unordered_map<std::string, int64_t> time_counts;

  // For now, create simple AM/PM buckets based on gaps
  // In practice, you'd derive this from the DataFrame index timestamps
  auto gap_up_count = gaps["gap_up"].sum().as_int64();
  auto gap_down_count = gaps["gap_down"].sum().as_int64();

  // Simplified time distribution for demo
  time_counts["Morning"] = gap_up_count / 2 + gap_down_count / 3;
  time_counts["Afternoon"] = gap_up_count - time_counts["Morning"] +
                             gap_down_count - gap_down_count / 3;

  std::vector<PieData> points;
  for (const auto &[bucket, count] : time_counts) {
    PieData point;
    point.set_name(bucket);
    *point.mutable_y() = ToProtoScalar(epoch_frame::Scalar{count});
    points.push_back(std::move(point));
  }

  PieDef pie_def;

  // Set up chart definition
  auto *chart_def = pie_def.mutable_chart_def();
  chart_def->set_id("gap_time_distribution");
  chart_def->set_title("Gap Timing Distribution");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_PIE);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);

  // Set up pie data
  auto *pie_data = pie_def.add_data();
  pie_data->set_name("Gap Timing");
  pie_data->set_size("90%");
  pie_data->set_inner_size("50%");

  for (auto &point : points) {
    *pie_data->add_points() = std::move(point);
  }

  return pie_def;
}

Table GapReport::create_gap_details_table(const epoch_frame::DataFrame &gaps,
                                          uint32_t limit) const {
  // Limit rows
  auto num_rows = std::min(static_cast<int64_t>(limit),
                           static_cast<int64_t>(gaps.num_rows()));

  // Build table columns
  arrow::StringBuilder date_builder, symbol_builder, gap_type_builder,
      performance_builder;
  arrow::DoubleBuilder gap_pct_builder, fill_pct_builder;
  arrow::BooleanBuilder is_filled_builder;

  for (int64_t i = 0; i < num_rows; ++i) {
    // Convert index timestamp to string for display
    auto date_scalar = gaps.index()->at(i);
    ARROW_UNUSED(date_builder.Append(date_scalar.repr()));

    // Derive symbol (if available) or use placeholder
    ARROW_UNUSED(symbol_builder.Append("SPY")); // placeholder

    // Determine gap type from boolean columns
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto gap_type_str = is_gap_up ? "gap_up" : "gap_down";
    ARROW_UNUSED(gap_type_builder.Append(gap_type_str));

    // Calculate gap percentage from fractions
    auto gap_up_frac = gaps["gap_up_fraction"].iloc(i);
    auto gap_down_frac = gaps["gap_down_fraction"].iloc(i);
    auto gap_pct = is_gap_up ? gap_up_frac.as_double() * 100
                             : gap_down_frac.as_double() * 100;
    ARROW_UNUSED(gap_pct_builder.Append(gap_pct));

    // Check if filled
    auto is_up_filled = gaps["gap_up_filled"].iloc(i).as_bool();
    auto is_down_filled = gaps["gap_down_filled"].iloc(i).as_bool();
    auto is_filled = is_up_filled || is_down_filled;
    ARROW_UNUSED(is_filled_builder.Append(is_filled));

    // Fill percentage (using same as gap percentage for now)
    ARROW_UNUSED(fill_pct_builder.Append(gap_pct));

    // Derive close performance from OHLC
    auto close_val = gaps["c"].iloc(i).as_double();
    auto open_val = gaps["o"].iloc(i).as_double();
    auto performance_str = close_val > open_val ? "green" : "red";
    ARROW_UNUSED(performance_builder.Append(performance_str));
  }

  std::shared_ptr<arrow::Array> date_array, symbol_array, gap_type_array,
      gap_pct_array, is_filled_array, fill_pct_array, performance_array;

  ARROW_UNUSED(date_builder.Finish(&date_array));
  ARROW_UNUSED(symbol_builder.Finish(&symbol_array));
  ARROW_UNUSED(gap_type_builder.Finish(&gap_type_array));
  ARROW_UNUSED(gap_pct_builder.Finish(&gap_pct_array));
  ARROW_UNUSED(is_filled_builder.Finish(&is_filled_array));
  ARROW_UNUSED(fill_pct_builder.Finish(&fill_pct_array));
  ARROW_UNUSED(performance_builder.Finish(&performance_array));

  auto schema = arrow::schema({arrow::field("Date", arrow::utf8()),
                               arrow::field("Symbol", arrow::utf8()),
                               arrow::field("Type", arrow::utf8()),
                               arrow::field("Gap %", arrow::float64()),
                               arrow::field("Filled", arrow::boolean()),
                               arrow::field("Fill %", arrow::float64()),
                               arrow::field("Performance", arrow::utf8())});

  auto table = arrow::Table::Make(
      schema, {date_array, symbol_array, gap_type_array, gap_pct_array,
               is_filled_array, fill_pct_array, performance_array});

  Table result_table;
  result_table.set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_DATA_TABLE);
  result_table.set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  result_table.set_title("Recent Gap Details");

  // Add columns
  auto *col1 = result_table.add_columns();
  col1->set_name("Date");
  // col1->set_display_name("Date");
  col1->set_type(epoch_proto::EPOCH_FOLIO_TYPE_DATE_TIME);

  auto *col2 = result_table.add_columns();
  col2->set_name("Symbol");
  // col2->set_display_name("Symbol");
  col2->set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);

  auto *col3 = result_table.add_columns();
  col3->set_name("Type");
  // col3->set_display_name("Type");
  col3->set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);

  auto *col4 = result_table.add_columns();
  col4->set_name("Gap %");
  // col4->set_display_name("Gap %");
  col4->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);

  auto *col5 = result_table.add_columns();
  col5->set_name("Filled");
  // col5->set_display_name("Filled");
  col5->set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);

  auto *col6 = result_table.add_columns();
  col6->set_name("Fill %");
  // col6->set_display_name("Fill %");
  col6->set_type(epoch_proto::EPOCH_FOLIO_TYPE_PERCENT);

  auto *col7 = result_table.add_columns();
  col7->set_name("Performance");
  // col7->set_display_name("Performance");
  col7->set_type(epoch_proto::EPOCH_FOLIO_TYPE_STRING);

  // Set data
  *result_table.mutable_data() = MakeTableDataFromArrow(table);

  return result_table;
}

LinesDef
GapReport::create_gap_trend_chart(const epoch_frame::DataFrame &gaps) const {
  using namespace epoch_frame;

  // Group by month using the available dt_month_name() or dt_month()
  // functionality For simplicity, we'll create a monthly aggregation manually
  std::map<std::string, int64_t> monthly_counts;

  // Extract month-year key from each date and count
  for (size_t i = 0; i < static_cast<size_t>(gaps.num_rows()); ++i) {
    auto timestamp = gaps.index()->at(i);
    // Extract year and month from timestamp
    // Extract year and month from timestamp (nanoseconds since epoch)
    auto timestamp_ns = timestamp.as_int64();
    // Convert to seconds and create time_t
    time_t time_seconds = timestamp_ns / 1000000000LL;
    std::tm *tm_info = std::gmtime(&time_seconds);
    auto year = tm_info->tm_year + 1900;
    auto month = tm_info->tm_mon + 1;
    auto month_key = std::to_string(year) + "-" + (month < 10 ? "0" : "") +
                     std::to_string(month);
    monthly_counts[month_key]++;
  }

  // Convert to sorted vectors for plotting
  std::vector<std::pair<std::string, int64_t>> sorted_counts(
      monthly_counts.begin(), monthly_counts.end());
  std::sort(sorted_counts.begin(), sorted_counts.end());

  Line line;
  line.set_name("Gap Frequency");

  for (const auto &[month_str, count] : sorted_counts) {
    // Create a timestamp for the first day of each month
    // For simplicity, using the month string as x-value
    auto *point = line.add_data();
    *point->mutable_x() = ToProtoScalar(Scalar{month_str});
    *point->mutable_y() = ToProtoScalar(Scalar{count});
  }

  LinesDef lines_def;

  // Set up chart definition
  auto *chart_def = lines_def.mutable_chart_def();
  chart_def->set_id("gap_trend");
  chart_def->set_title("Gap Frequency Trend (Monthly)");
  chart_def->set_type(epoch_proto::EPOCH_FOLIO_DASHBOARD_WIDGET_LINES);
  chart_def->set_category(epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);

  // Set up axes
  *chart_def->mutable_y_axis() = MakeLinearAxis("Number of Gaps");
  *chart_def->mutable_x_axis() = MakeDateTimeAxis();

  // Add the line
  *lines_def.add_lines() = std::move(line);

  return lines_def;
}

// Explicit registration function
void GapReport::register_report() {
  ReportRegistry::instance().register_report(
      s_metadata,
      [](const epoch_metadata::transform::TransformConfiguration *cfg) {
        return std::make_unique<GapReport>(cfg);
      });
}

// Register the report
EPOCH_REGISTER_REPORT(GapReport, epoch_folio::GapReport::s_metadata)

} // namespace epoch_folio