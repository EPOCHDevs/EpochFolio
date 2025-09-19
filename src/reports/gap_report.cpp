#include "gap_report.h"
#include "epoch_folio/tearsheet.h"
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/table.h>
#include <ctime>
#include <limits>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_metadata/metadata_options.h>
#include <spdlog/spdlog.h>

#include "common/card_helpers.h"
#include "common/chart_def.h"
#include "common/table_helpers.h"

namespace epoch_folio {
  const auto closeLiteral = epoch_metadata::EpochStratifyXConstants::instance().CLOSE();

void GapReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf) const {
  // Clear any previous tearsheet data
  m_tearsheet.Clear();

  // Generate the tearsheet using the existing implementation
  m_tearsheet = generate_impl(normalizedDf);
}

epoch_proto::TearSheet
GapReport::generate_impl(const epoch_frame::DataFrame &df) const {
  epoch_proto::TearSheet result;

  // Create a DataFrame with gap_down column if it doesn't exist
  epoch_frame::DataFrame working_df = df;
  bool has_gap_down = false;
  for (size_t i = 0; i < static_cast<size_t>(df.num_cols()); ++i) {
    if (df.table()->field(i)->name() == "gap_down") {
      has_gap_down = true;
      break;
    }
  }

  if (!has_gap_down) {
    // Create gap_down as inverse of gap_up
    auto gap_up_col = df["gap_up"];
    auto gap_down_col = gap_up_col == epoch_frame::Scalar{false};

    // Create new Series with proper name
    auto gap_down_series = epoch_frame::Series(gap_down_col.index(), gap_down_col.array(), "gap_down");

    // Create new DataFrame with gap_down column added
    std::vector<arrow::ChunkedArrayPtr> arrays;
    std::vector<std::string> column_names;

    // Copy existing columns
    for (size_t i = 0; i < static_cast<size_t>(df.num_cols()); ++i) {
      auto col_name = df.table()->field(i)->name();
      arrays.push_back(df[col_name].array());
      column_names.push_back(col_name);
    }

    // Add gap_down column
    arrays.push_back(gap_down_series.array());
    column_names.push_back("gap_down");

    working_df = epoch_frame::make_dataframe(df.index(), arrays, column_names);
  }

  auto filtered_gaps = filter_gaps(working_df);
  if (filtered_gaps.num_rows() == 0) {
    SPDLOG_WARN("No gaps found after filtering");
    return result;
  }

  // Debug: Check what columns are available
  SPDLOG_INFO("Filtered gaps DataFrame has {} columns:", filtered_gaps.num_cols());
  for (size_t i = 0; i < static_cast<size_t>(filtered_gaps.num_cols()); ++i) {
    auto col_name = filtered_gaps.table()->field(i)->name();
    SPDLOG_INFO("  Column {}: {}", i, col_name);
  }

  // 1. Summary cards
  auto cards = compute_summary_cards(filtered_gaps);
  for (auto &card : cards) {
    *result.mutable_cards()->add_cards() = std::move(card);
  }

  // Helper to read boolean options from configuration
  auto getBoolOpt = [&](std::string const &key, bool defVal) -> bool {
    try {
      return m_config.GetOptionValue(key).GetBoolean();
    } catch (...) {
      return defVal;
    }
  };
  auto getIntOpt = [&](std::string const &key, int defVal) -> int {
    try {
      return static_cast<int>(m_config.GetOptionValue(key).GetInteger());
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

  // 2. Fill rate analysis bar chart
  if (show_fill_analysis) {
    Chart chart;
    *chart.mutable_bar_def() =
        create_fill_rate_chart(filtered_gaps, "Gap Fill Analysis");
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 3. Day of week frequency bar chart (better visualization than table)
  if (show_day_of_week_analysis) {
    Chart chart;
    *chart.mutable_bar_def() = create_day_of_week_chart(
        filtered_gaps, "Gap Frequency by Day of Week");
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 4. Time bucket analysis table
  if (show_fill_time_analysis) {
    *result.mutable_tables()->add_tables() = create_frequency_table(
        filtered_gaps, "fill_time", "Gap Frequency by Time");
  }

  // 5. Streak visualization
  if (show_streak_analysis) {
    Chart chart;
    *chart.mutable_x_range_def() =
        create_streak_chart(filtered_gaps, std::numeric_limits<uint32_t>::max()); // No limit
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 6. Gap size distribution histogram
  if (show_distribution_histogram) {
    Chart chart;
    *chart.mutable_histogram_def() = create_gap_distribution(
        filtered_gaps, static_cast<uint32_t>(histogram_bins));
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 7. Performance analysis
  if (show_performance_analysis) {
    *result.mutable_tables()->add_tables() = create_frequency_table(
        filtered_gaps, "close_performance", "Gap Fill vs Close Performance");
  }

  // 8. Time distribution pie chart
  Chart chart;
  *chart.mutable_pie_def() = create_time_distribution(filtered_gaps);
  *result.mutable_charts()->add_charts() = std::move(chart);

  // 9. Gap details table
  *result.mutable_tables()->add_tables() = create_gap_details_table(
      filtered_gaps, std::numeric_limits<uint32_t>::max()); // No limit

  // 10. Trend analysis
  Chart chart2;
  *chart2.mutable_lines_def() = create_gap_trend_chart(filtered_gaps);
  *result.mutable_charts()->add_charts() = std::move(chart2);

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

  // Access columns directly - orchestrator ensures they exist
  auto is_up = df["gap_up"];
  auto is_down = df["gap_down"];

  auto fill_fraction = df["fill_fraction"];
  auto gap_size = df["gap_size"];

  // Determine if gaps are filled based on fill_fraction
  auto is_filled_up = is_up && (fill_fraction > Scalar{0.5});
  auto is_filled_down = is_down && (fill_fraction > Scalar{0.5});
  auto is_filled = is_filled_up || is_filled_down;

  // Convert gap_size to percentage
  auto gap_pct = gap_size * Scalar{100.0};
  auto pct_abs = gap_pct.abs();
  auto gap_up_pct = gap_pct.where(is_up, Scalar{0.0});
  auto gap_down_pct = gap_pct.abs().where(is_down, Scalar{0.0});

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

  // Direct access - orchestrator ensures columns exist
  auto gap_up = filtered["gap_up"];
  auto gap_down = filtered["gap_down"];

  // Add derived columns to the filtered DataFrame
  SPDLOG_INFO("Adding derived columns to {} rows", filtered.num_rows());
  // Derive day_of_week from index
  std::vector<std::string> day_of_week_data;
  std::vector<std::string> time_bucket_data;
  std::vector<std::string> close_performance_data;

  SPDLOG_INFO("Starting derived column computation loop for {} rows...", filtered.num_rows());
  for (size_t i = 0; i < static_cast<size_t>(filtered.num_rows()); ++i) {
    // Extract day of week from timestamp for all rows
    auto datetime = filtered.index()->at(i).to_datetime();

    // Day of week (0=Monday, 1=Tuesday, ..., 6=Sunday) - Python style
    const char *days[] = {"Monday", "Tuesday", "Wednesday",
                          "Thursday", "Friday", "Saturday", "Sunday"};
    day_of_week_data.push_back(days[datetime.weekday()]);

    // Time bucket (simplified - based on hour)
    auto time = datetime.time();
    time_bucket_data.push_back(
        std::format("{:02d}:{:02d}", time.hour.count(), time.minute.count()));

    // Close performance (green if close > prior session close, red otherwise)
    auto close_val = filtered[closeLiteral].iloc(i).as_double();
    auto prior_close = filtered["psc"].iloc(i).as_double();

    close_performance_data.push_back(close_val > prior_close ? "green" : "red");
  }

  SPDLOG_INFO("Created {} day_of_week entries, {} time_bucket entries, {} close_performance entries",
              day_of_week_data.size(), time_bucket_data.size(), close_performance_data.size());

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

  // Add gap_down column derived from gap_up if it doesn't exist
  bool has_gap_down = false;
  for (size_t i = 0; i < static_cast<size_t>(filtered.num_cols()); ++i) {
    if (filtered.table()->field(i)->name() == "gap_down") {
      has_gap_down = true;
      break;
    }
  }

  if (!has_gap_down) {
    auto gap_up_col = filtered["gap_up"];
    auto gap_down_col = gap_up_col == epoch_frame::Scalar{false};
    all_series.push_back(gap_down_col);
    all_columns.push_back("gap_down");
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
  card1.set_type(epoch_proto::WidgetCard);
  card1.set_category("Reports");
  card1.set_group_size(1);
  CardDataHelper::AddIntegerField(card1, "Total Gaps",
      epoch_frame::Scalar{static_cast<int64_t>(gaps.num_rows())});
  cards.push_back(std::move(card1));

  // Count gap types using boolean columns
  auto gap_up_series = gaps["gap_up"];
  auto gap_up_count = gap_up_series.sum().cast_int64().as_int64();

  // Calculate gap_down as inverse of gap_up when gap_up is not null
  int64_t gap_down_count = 0;
  try {
    auto gap_down_series = gaps["gap_down"];
    gap_down_count = gap_down_series.sum().cast_int64().as_int64();
  } catch (...) {
    // If gap_down doesn't exist, calculate as false values in gap_up
    auto gap_up_false = gap_up_series == epoch_frame::Scalar{false};
    gap_down_count = gap_up_false.sum().cast_int64().as_int64();
  }

  // Count filled gaps based on fill_fraction
  auto fill_fraction = gaps["fill_fraction"];
  auto is_filled = fill_fraction > epoch_frame::Scalar{0.5};
  auto filled_count = is_filled.sum().cast_int64().as_int64();

  // Calculate gap percentages from gap_size
  auto gap_pct = gaps["gap_size"] * epoch_frame::Scalar{100.0};
  auto total_gap_pct = gap_pct.abs().sum().as_double();
  auto max_gap_pct = gap_pct.abs().max().as_double();

  // Gap up/down counts
  CardDef card2;
  card2.set_type(epoch_proto::WidgetCard);
  card2.set_category("Reports");
  card2.set_group_size(2);

  CardDataHelper::AddIntegerField(card2, "Gap Up",
      epoch_frame::Scalar{gap_up_count}, 1);
  CardDataHelper::AddIntegerField(card2, "Gap Down",
      epoch_frame::Scalar{static_cast<int64_t>(gap_down_count)}, 1);

  cards.push_back(std::move(card2));

  // Fill rate
  double fill_rate = gaps.num_rows() > 0 ? static_cast<double>(filled_count) /
                                               gaps.num_rows() * 100
                                         : 0;

  CardDef card3;
  card3.set_type(epoch_proto::WidgetCard);
  card3.set_category("Reports");
  card3.set_group_size(1);
  CardDataHelper::AddPercentField(card3, "Overall Fill Rate",
      epoch_frame::Scalar{fill_rate});
  cards.push_back(std::move(card3));

  // Average and max gap size
  double avg_gap_pct =
      gaps.num_rows() > 0 ? total_gap_pct / gaps.num_rows() : 0;

  CardDef card4;
  card4.set_type(epoch_proto::WidgetCard);
  card4.set_category("Reports");
  card4.set_group_size(2);

  CardDataHelper::AddPercentField(card4, "Avg Gap %",
      epoch_frame::Scalar{avg_gap_pct}, 2);
  CardDataHelper::AddPercentField(card4, "Max Gap %",
      epoch_frame::Scalar{max_gap_pct}, 2);

  cards.push_back(std::move(card4));

  return cards;
}

BarDef GapReport::create_day_of_week_chart(const epoch_frame::DataFrame &gaps,
                                           const std::string &title) const {
  // Group gaps by day of week and count
  auto dow = gaps["day_of_week"];
  auto grouped = dow.contiguous_array().value_counts();

  // Create ordered map for consistent day ordering
  std::map<int, std::pair<std::string, int64_t>> ordered_days;
  const char *days[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

  // Initialize with zeros
  for (int i = 0; i < 7; ++i) {
    ordered_days[i] = {days[i], 0};
  }

  // Populate with actual counts
  for (size_t i = 0; i < static_cast<size_t>(grouped.first.length()); ++i) {
    auto day_name = grouped.first[i].repr();
    auto count = grouped.second[i].as_int64();

    // Find the day index
    for (int d = 0; d < 7; ++d) {
      if (day_name == days[d]) {
        ordered_days[d].second = count;
        break;
      }
    }
  }

  BarDef bar_def;

  // Set up chart definition
  auto *chart_def = bar_def.mutable_chart_def();
  chart_def->set_id("gap_day_of_week");
  chart_def->set_title(title);
  chart_def->set_type(epoch_proto::WidgetBar);
  chart_def->set_category("Reports");

  // Set up axes
  *chart_def->mutable_y_axis() = MakeLinearAxis("Gap Count");

  auto *x_axis = chart_def->mutable_x_axis();
  x_axis->set_type(kCategoryAxisType);

  // Add categories and data in order
  auto* bar_data = bar_def.mutable_data();
  for (const auto& [idx, day_data] : ordered_days) {
    x_axis->add_categories(day_data.first);
    bar_data->add_values()->set_integer_value(day_data.second);
  }

  return bar_def;
}

BarDef GapReport::create_fill_rate_chart(const epoch_frame::DataFrame &gaps,
                                         const std::string &title) const {
  // Count gap types and fills
  auto gap_up_mask = gaps["gap_up"];
  auto gap_down_mask = gaps["gap_down"];
  auto fill_fraction = gaps["fill_fraction"];

  // Gap is considered filled if fill_fraction > 0.5
  auto gap_up_filled_mask = gap_up_mask && (fill_fraction > epoch_frame::Scalar{0.5});
  auto gap_down_filled_mask = gap_down_mask && (fill_fraction > epoch_frame::Scalar{0.5});

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

  BarDef bar_def;

  // Set up chart definition
  auto *chart_def = bar_def.mutable_chart_def();
  chart_def->set_id("gap_fill_rates");
  chart_def->set_title(title);
  chart_def->set_type(epoch_proto::WidgetBar);
  chart_def->set_category("Reports");

  // Set up axes
  *chart_def->mutable_y_axis() = MakePercentageAxis("Fill Rate (%)");

  auto *x_axis = chart_def->mutable_x_axis();
  x_axis->set_type(kCategoryAxisType);
  x_axis->add_categories("Gap Up");
  x_axis->add_categories("Gap Down");

  // Set bar data
  auto* bar_data = bar_def.mutable_data();
  bar_data->add_values()->set_decimal_value(gap_up_fill_rate);
  bar_data->add_values()->set_decimal_value(gap_down_fill_rate);

  return bar_def;
}

Table GapReport::create_frequency_table(const epoch_frame::DataFrame &gaps,
                                        const std::string &category_col,
                                        const std::string &title) const {
  SPDLOG_INFO("Creating frequency table for column: {}", category_col);
  SPDLOG_INFO("Available columns in gaps DataFrame:");
  for (size_t i = 0; i < static_cast<size_t>(gaps.num_cols()); ++i) {
    auto col_name = gaps.table()->field(i)->name();
    SPDLOG_INFO("  Column {}: {}", i, col_name);
  }

  // Test if we can access the column directly first
  try {
    auto test_column = gaps[category_col];
    SPDLOG_INFO("Successfully accessed column: {}", category_col);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to access column {}: {}", category_col, e.what());
    throw;
  }

  auto dow = gaps[category_col];

  auto grouped = dow.contiguous_array().value_counts();
  SPDLOG_INFO("Successfully performed group_by_agg");

  std::unordered_map<std::string, int64_t> counts;
  for (size_t i = 0; i < static_cast<size_t>(grouped.first.length()); ++i) {
    auto category = grouped.first[i].repr();
    auto count = grouped.second[i].as_int64();
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
  result_table.set_type(epoch_proto::WidgetDataTable);
  result_table.set_category("Reports");
  result_table.set_title(title);

  // Add columns using helpers (id defaults to name if not provided)
  TableColumnHelper::AddStringColumn(result_table, "Category", "category");
  TableColumnHelper::AddIntegerColumn(result_table, "Frequency", "frequency");
  TableColumnHelper::AddPercentColumn(result_table, "Percentage", "percentage");

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
    auto fill_frac = gaps["fill_fraction"].iloc(i).as_double();
    bool is_filled = fill_frac > 0.5;

    if (is_gap_up) {
      gap_up_list.push_back({i, is_filled});
    }
    if (is_gap_down) {
      gap_down_list.push_back({i, is_filled});
    }
  }

  // Take all or last N of each type (no artificial limit if max_streaks is very large)
  auto add_points = [&](const std::vector<std::pair<int64_t, bool>> &list,
                        size_t category_idx) {
    size_t start = (max_streaks < list.size()) ? list.size() - max_streaks : 0;
    for (size_t j = start; j < list.size(); ++j) {
      auto [idx, is_filled] = list[j];
      auto date = gaps.index()->at(idx);
      auto point = MakeXRangePoint(date, category_idx, is_filled);
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
  chart_def->set_type(epoch_proto::WidgetXRange);
  chart_def->set_category("Reports");

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
  // Extract gap percentages from gap_size
  auto gap_pct = gaps["gap_size"] * epoch_frame::Scalar{100.0};
  auto abs_gap_pct = gap_pct.abs();

  auto data_array = abs_gap_pct.array()->chunk(0);

  HistogramDef histogram_def;

  // Set up chart definition
  auto *chart_def = histogram_def.mutable_chart_def();
  chart_def->set_id("gap_distribution");
  chart_def->set_title("Gap Size Distribution");
  chart_def->set_type(epoch_proto::WidgetHistogram);
  chart_def->set_category("Reports");

  // Set up axes
  *chart_def->mutable_y_axis() = MakeLinearAxis("Frequency");
  *chart_def->mutable_x_axis() = MakePercentageAxis("Gap Size (%)");

  // Set data and bins
  // Convert Arrow array to protobuf array
  auto chunked = arrow::ChunkedArray::Make({data_array});
  *histogram_def.mutable_data() = MakeArrayFromArrow(chunked.MoveValueUnsafe());
  histogram_def.set_bins_count(bins);

  return histogram_def;
}

PieDef
GapReport::create_time_distribution(const epoch_frame::DataFrame &gaps) const {
  // Derive time buckets from actual timestamps
  std::unordered_map<std::string, int64_t> time_counts;

  // Count gaps by time period
  for (size_t i = 0; i < static_cast<size_t>(gaps.num_rows()); ++i) {
    auto datetime = gaps.index()->at(i).to_datetime();
    auto hour = datetime.time().hour.count();

    // Categorize by trading session periods
    std::string period;
    if (hour < 10) {
      period = "Pre-Market (9:30-10:00)";
    } else if (hour < 12) {
      period = "Morning (10:00-12:00)";
    } else if (hour < 14) {
      period = "Midday (12:00-14:00)";
    } else if (hour < 16) {
      period = "Afternoon (14:00-16:00)";
    } else {
      period = "After-Hours";
    }

    time_counts[period]++;
  }

  std::vector<PieData> points;
  for (const auto &[bucket, count] : time_counts) {
    PieData point;
    point.set_name(bucket);
    point.set_y(static_cast<double>(count));
    points.push_back(std::move(point));
  }

  PieDef pie_def;

  // Set up chart definition
  auto *chart_def = pie_def.mutable_chart_def();
  chart_def->set_id("gap_time_distribution");
  chart_def->set_title("Gap Timing Distribution");
  chart_def->set_type(epoch_proto::WidgetPie);
  chart_def->set_category("Reports");

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
  auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");
  arrow::TimestampBuilder date_builder(timestamp_type, arrow::default_memory_pool());
  arrow::StringBuilder symbol_builder, gap_type_builder, performance_builder;
  arrow::DoubleBuilder gap_pct_builder, fill_pct_builder;
  arrow::BooleanBuilder is_filled_builder;

  for (int64_t i = 0; i < num_rows; ++i) {
    // Convert index timestamp to milliseconds for proper display
    auto date_scalar = gaps.index()->at(i);
    // Convert nanoseconds to milliseconds
    int64_t timestamp_ms = date_scalar.timestamp().value / 1000000;
    ARROW_UNUSED(date_builder.Append(timestamp_ms));

    // Derive symbol (if available) or use placeholder
    ARROW_UNUSED(symbol_builder.Append("SPY")); // placeholder

    // Determine gap type from boolean columns
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto gap_type_str = is_gap_up ? "gap_up" : "gap_down";
    ARROW_UNUSED(gap_type_builder.Append(gap_type_str));

    // Calculate gap percentage from gap_size
    auto gap_size = gaps["gap_size"].iloc(i).as_double();
    auto gap_pct = std::abs(gap_size * 100);
    ARROW_UNUSED(gap_pct_builder.Append(gap_pct));

    // Check if filled based on fill_fraction
    auto fill_frac = gaps["fill_fraction"].iloc(i).as_double();
    auto is_filled = fill_frac > 0.5;
    ARROW_UNUSED(is_filled_builder.Append(is_filled));

    // Fill percentage
    auto fill_pct = fill_frac * gap_pct;
    ARROW_UNUSED(fill_pct_builder.Append(fill_pct));

    // Derive close performance from close vs psc
    auto close_val = gaps[closeLiteral].iloc(i).as_double();
    auto psc_val = gaps["psc"].iloc(i).as_double();
    auto performance_str = close_val > psc_val ? "green" : "red";
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

  auto schema = arrow::schema({arrow::field("Date", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
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
  result_table.set_type(epoch_proto::WidgetDataTable);
  result_table.set_category("Reports");
  result_table.set_title("Recent Gap Details");

  // Add columns using helpers
  TableColumnHelper::AddTimestampColumn(result_table, "Date", "date");
  TableColumnHelper::AddStringColumn(result_table, "Symbol", "symbol");
  TableColumnHelper::AddStringColumn(result_table, "Type", "type");
  TableColumnHelper::AddPercentColumn(result_table, "Gap %", "gap_percent");
  TableColumnHelper::AddStringColumn(result_table, "Filled", "filled");
  TableColumnHelper::AddPercentColumn(result_table, "Fill %", "fill_percent");
  TableColumnHelper::AddStringColumn(result_table, "Performance", "performance");

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
    auto datetime = gaps.index()->at(i).to_datetime();
    auto date = datetime.date();

    // Use DateTime object to get year and month directly
    auto year = static_cast<int>(date.year);
    auto month = static_cast<unsigned>(date.month);
    auto month_key = std::format("{}-{:02d}", year, month);
    monthly_counts[month_key]++;
  }

  // Convert to sorted vectors for plotting
  std::vector<std::pair<std::string, int64_t>> sorted_counts(
      monthly_counts.begin(), monthly_counts.end());
  std::sort(sorted_counts.begin(), sorted_counts.end());

  Line line;
  line.set_name("Gap Frequency");

  for (const auto &[month_str, count] : sorted_counts) {
    // Use helper to convert month string to proper timestamp
    auto timestamp_ms = MonthStringToTimestampMs(month_str);
    auto point = MakeLinePoint(timestamp_ms, static_cast<double>(count));
    *line.add_data() = std::move(point);
  }

  LinesDef lines_def;

  // Set up chart definition
  auto *chart_def = lines_def.mutable_chart_def();
  chart_def->set_id("gap_trend");
  chart_def->set_title("Gap Frequency Trend (Monthly)");
  chart_def->set_type(epoch_proto::WidgetLines);
  chart_def->set_category("Reports");

  // Set up axes
  *chart_def->mutable_y_axis() = MakeLinearAxis("Number of Gaps");
  *chart_def->mutable_x_axis() = MakeDateTimeAxis();

  // Add the line
  *lines_def.add_lines() = std::move(line);

  return lines_def;
}


} // namespace epoch_folio