#include "gap_report.h"
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/table.h>
#include <ctime>
#include <epoch_frame/factory/series_factory.h>
#include <spdlog/spdlog.h>

namespace epoch_folio {

// Static metadata definition
ReportMetadata GapReport::s_metadata = {
    .id = "gap_report",
    .displayName = "Price Gap Analysis",
    .summary =
        "Analyzes opening price gaps, their fills, and patterns over time",
    .category = epoch_core::EpochFolioCategory::RiskAnalysis,
    .tags = {"gaps", "overnight", "price-action", "fill-analysis",
             "market-microstructure"},
    .requiredColumns =
        {{"gap_up", "Gap Up", epoch_core::EpochFolioType::Boolean},
         {"gap_down", "Gap Down", epoch_core::EpochFolioType::Boolean},
         {"gap_up_filled", "Gap Up Filled",
          epoch_core::EpochFolioType::Boolean},
         {"gap_down_filled", "Gap Down Filled",
          epoch_core::EpochFolioType::Boolean},
         {"gap_up_size", "Gap Up Size", epoch_core::EpochFolioType::Decimal},
         {"gap_down_size", "Gap Down Size",
          epoch_core::EpochFolioType::Decimal},
         {"gap_up_fraction", "Gap Up Fraction",
          epoch_core::EpochFolioType::Decimal},
         {"gap_down_fraction", "Gap Down Fraction",
          epoch_core::EpochFolioType::Decimal},
         {"o", "Open", epoch_core::EpochFolioType::Decimal},
         {"h", "High", epoch_core::EpochFolioType::Decimal},
         {"l", "Low", epoch_core::EpochFolioType::Decimal},
         {"c", "Close", epoch_core::EpochFolioType::Decimal},
         {"v", "Volume", epoch_core::EpochFolioType::Decimal}},
    .typicalOutputs = {epoch_core::EpochFolioDashboardWidget::Card,
                       epoch_core::EpochFolioDashboardWidget::Bar,
                       epoch_core::EpochFolioDashboardWidget::DataTable,
                       epoch_core::EpochFolioDashboardWidget::XRange,
                       epoch_core::EpochFolioDashboardWidget::Histogram,
                       epoch_core::EpochFolioDashboardWidget::Pie,
                       epoch_core::EpochFolioDashboardWidget::Lines},
    .defaultOptions = {},
    .version = "0.1.0",
    .owner = "epoch"};

const ReportMetadata &GapReport::metadata() const { return s_metadata; }

TearSheet GapReport::generate(const epoch_frame::DataFrame &df,
                              const glz::json_t &optionsJson) const {
  auto options = glz::read_json<GapReportOptions>(optionsJson);
  if (!options) {
    SPDLOG_ERROR("Failed to parse GapReportOptions: {}",
                 glz::format_error(options.error()));
    return {};
  }

  return generate_impl(df, options.value());
}

std::unordered_map<std::string, TearSheet> GapReport::generate_per_asset(
    const std::unordered_map<std::string, epoch_frame::DataFrame> &assetToDf,
    const glz::json_t &optionsJson) const {
  auto options = glz::read_json<GapReportOptions>(optionsJson);
  if (!options) {
    SPDLOG_ERROR("Failed to parse GapReportOptions: {}",
                 glz::format_error(options.error()));
    return {};
  }

  std::unordered_map<std::string, TearSheet> results;
  for (const auto &[symbol, df] : assetToDf) {
    results[symbol] = generate_impl(df, options.value());
  }

  return results;
}

TearSheet GapReport::generate_impl(const epoch_frame::DataFrame &df,
                                   const GapReportOptions &options) const {
  TearSheet result;

  auto filtered_gaps = filter_gaps(df, options);
  if (filtered_gaps.num_rows() == 0) {
    SPDLOG_WARN("No gaps found after filtering");
    return result;
  }

  // 1. Summary cards
  result.cards = compute_summary_cards(filtered_gaps);

  // 2. Fill rate analysis bar chart
  if (options.show_fill_analysis) {
    result.charts.emplace_back(
        create_fill_rate_chart(filtered_gaps, "Gap Fill Analysis"));
  }

  // 3. Day of week frequency table
  if (options.show_day_of_week_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "day_of_week", "Gap Frequency by Day of Week"));
  }

  // 4. Time bucket analysis table
  if (options.show_fill_time_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "fill_time", "Gap Frequency by Time"));
  }

  // 5. Streak visualization
  if (options.show_streak_analysis) {
    result.charts.emplace_back(
        create_streak_chart(filtered_gaps, options.max_streaks));
  }

  // 6. Gap size distribution histogram
  if (options.show_distribution_histogram) {
    result.charts.emplace_back(
        create_gap_distribution(filtered_gaps, options.histogram_bins));
  }

  // 7. Performance analysis
  if (options.show_performance_analysis) {
    result.tables.emplace_back(create_frequency_table(
        filtered_gaps, "close_performance", "Gap Fill vs Close Performance"));
  }

  // 8. Time distribution pie chart
  result.charts.emplace_back(create_time_distribution(filtered_gaps));

  // 9. Gap details table
  result.tables.emplace_back(
      create_gap_details_table(filtered_gaps, options.max_table_rows));

  // 10. Trend analysis
  result.charts.emplace_back(create_gap_trend_chart(filtered_gaps));

  return result;
}

epoch_frame::DataFrame
GapReport::filter_gaps(const epoch_frame::DataFrame &df,
                       const GapReportOptions &options) const {
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
  if (!options.include_gap_up || !options.include_gap_down) {
    Series type_mask;
    if (options.include_gap_up && !options.include_gap_down) {
      type_mask = is_up;
    } else if (!options.include_gap_up && options.include_gap_down) {
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
  mask = mask && (pct_abs >= Scalar{options.min_gap_pct}) &&
         (pct_abs <= Scalar{options.max_gap_pct});

  // Filled / unfilled filter
  if (options.only_filled && !options.only_unfilled) {
    mask = mask && is_filled;
  } else if (options.only_unfilled && !options.only_filled) {
    mask = mask && !is_filled;
  }

  // Time range filter (index is date-sorted)
  if (options.start_timestamp_ns) {
    auto start_date = epoch_frame::Scalar{*options.start_timestamp_ns};
    // Create a date mask by converting index to Series and comparing
    auto index_series = make_series(
        df.index(), df.index()->array().to_vector<int64_t>(), "index");
    auto date_mask = index_series >= start_date;
    mask = mask && date_mask;
  }
  if (options.end_timestamp_ns) {
    auto end_date = epoch_frame::Scalar{*options.end_timestamp_ns};
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
  if (options.last_n_gaps && filtered.num_rows() > 0) {
    filtered = filtered.tail(*options.last_n_gaps);
  }

  return filtered;
}

std::vector<CardDef>
GapReport::compute_summary_cards(const epoch_frame::DataFrame &gaps) const {
  std::vector<CardDef> cards;

  // Total gaps
  cards.push_back({.type = epoch_core::EpochFolioDashboardWidget::Card,
                   .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                   .data = {{.title = "Total Gaps",
                             .value = epoch_frame::Scalar{static_cast<int64_t>(
                                 gaps.num_rows())},
                             .type = epoch_core::EpochFolioType::Integer}},
                   .group_size = 1});

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
  cards.push_back({.type = epoch_core::EpochFolioDashboardWidget::Card,
                   .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                   .data = {{.title = "Gap Up",
                             .value = epoch_frame::Scalar{gap_up_count},
                             .type = epoch_core::EpochFolioType::Integer,
                             .group = 1},
                            {.title = "Gap Down",
                             .value = epoch_frame::Scalar{gap_down_count},
                             .type = epoch_core::EpochFolioType::Integer,
                             .group = 1}},
                   .group_size = 2});

  // Fill rate
  double fill_rate = gaps.num_rows() > 0 ? static_cast<double>(filled_count) /
                                               gaps.num_rows() * 100
                                         : 0;

  cards.push_back({.type = epoch_core::EpochFolioDashboardWidget::Card,
                   .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                   .data = {{.title = "Overall Fill Rate",
                             .value = epoch_frame::Scalar{fill_rate},
                             .type = epoch_core::EpochFolioType::Percent}},
                   .group_size = 1});

  // Average and max gap size
  double avg_gap_pct =
      gaps.num_rows() > 0 ? total_gap_pct / gaps.num_rows() : 0;

  cards.push_back({.type = epoch_core::EpochFolioDashboardWidget::Card,
                   .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                   .data = {{.title = "Avg Gap %",
                             .value = epoch_frame::Scalar{avg_gap_pct},
                             .type = epoch_core::EpochFolioType::Percent,
                             .group = 2},
                            {.title = "Max Gap %",
                             .value = epoch_frame::Scalar{max_gap_pct},
                             .type = epoch_core::EpochFolioType::Percent,
                             .group = 2}},
                   .group_size = 2});

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

  return {.chartDef = {.id = "gap_fill_rates",
                       .title = title,
                       .type = epoch_core::EpochFolioDashboardWidget::Bar,
                       .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                       .yAxis = MakePercentageAxis("Fill Rate (%)"),
                       .xAxis = AxisDef{.type = kCategoryAxisType,
                                        .categories = {"Gap Up", "Gap Down"}}},
          .data = epoch_frame::Array{data_array}};
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

  return {
      .type = epoch_core::EpochFolioDashboardWidget::DataTable,
      .category = epoch_core::EpochFolioCategory::RiskAnalysis,
      .title = title,
      .columns = {{"Category", "Category", epoch_core::EpochFolioType::String},
                  {"Frequency", "Frequency",
                   epoch_core::EpochFolioType::Integer},
                  {"Percentage", "Percentage",
                   epoch_core::EpochFolioType::Percent}},
      .data = table};
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

      points.push_back(
          {.x = date, .x2 = date, .y = category_idx, .is_long = is_filled});
    }
  };

  add_points(gap_up_list, 0);
  add_points(gap_down_list, 1);

  return {.chartDef = {.id = "gap_streaks",
                       .title = "Recent Gap Streaks",
                       .type = epoch_core::EpochFolioDashboardWidget::XRange,
                       .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                       .yAxis = AxisDef{.type = kCategoryAxisType,
                                        .categories = categories},
                       .xAxis = MakeDateTimeAxis()},
          .categories = categories,
          .points = points};
}

HistogramDef
GapReport::create_gap_distribution(const epoch_frame::DataFrame &gaps,
                                   uint32_t bins) const {
  // Extract gap percentages from fractions
  auto gap_up_pct = gaps["gap_up_fraction"] * epoch_frame::Scalar{100.0};
  auto abs_gap_pct = gap_up_pct.abs();

  auto data_array = abs_gap_pct.array()->chunk(0);

  return {.chartDef = {.id = "gap_distribution",
                       .title = "Gap Size Distribution",
                       .type = epoch_core::EpochFolioDashboardWidget::Histogram,
                       .category = epoch_core::EpochFolioCategory::RiskAnalysis,
                       .yAxis = MakeLinearAxis("Frequency"),
                       .xAxis = MakePercentageAxis("Gap Size (%)")},
          .data = epoch_frame::Array{data_array},
          .binsCount = bins};
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

  PieDataPoints points;
  for (const auto &[bucket, count] : time_counts) {
    points.push_back({.name = bucket, .y = epoch_frame::Scalar{count}});
  }

  return {
      .chartDef = {.id = "gap_time_distribution",
                   .title = "Gap Timing Distribution",
                   .type = epoch_core::EpochFolioDashboardWidget::Pie,
                   .category = epoch_core::EpochFolioCategory::RiskAnalysis},
      .data = {{.name = "Gap Timing",
                .points = points,
                .size = "90%",
                .innerSize = "50%"}}};
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

  return {.type = epoch_core::EpochFolioDashboardWidget::DataTable,
          .category = epoch_core::EpochFolioCategory::RiskAnalysis,
          .title = "Recent Gap Details",
          .columns = {{"Date", "Date", epoch_core::EpochFolioType::DateTime},
                      {"Symbol", "Symbol", epoch_core::EpochFolioType::String},
                      {"Type", "Type", epoch_core::EpochFolioType::String},
                      {"Gap %", "Gap %", epoch_core::EpochFolioType::Percent},
                      {"Filled", "Filled", epoch_core::EpochFolioType::String},
                      {"Fill %", "Fill %", epoch_core::EpochFolioType::Percent},
                      {"Performance", "Performance",
                       epoch_core::EpochFolioType::String}},
          .data = table};
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
  line.name = "Gap Frequency";
  line.data.reserve(sorted_counts.size());

  for (const auto &[month_str, count] : sorted_counts) {
    // Create a timestamp for the first day of each month
    // For simplicity, using the month string as x-value
    line.data.push_back({Scalar{month_str}, Scalar{count}});
  }

  return {
      .chartDef =
          {.id = "gap_trend",
           .title = "Gap Frequency Trend (Monthly)",
           .type = epoch_core::EpochFolioDashboardWidget::Lines,
           .category = epoch_core::EpochFolioCategory::RiskAnalysis,
           .yAxis = MakeLinearAxis("Number of Gaps"),
           .xAxis = MakeDateTimeAxis()}, // Use datetime axis for month strings
      .lines = {line}};
}

// Explicit registration function
void GapReport::register_report() {
  ReportRegistry::instance().register_report(
      s_metadata, []() { return std::make_unique<GapReport>(); });
}

// Register the report
EPOCH_REGISTER_REPORT(GapReport, epoch_folio::GapReport::s_metadata)

} // namespace epoch_folio