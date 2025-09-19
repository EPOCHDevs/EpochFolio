#include "gap_report.h"
#include "epoch_folio/tearsheet.h"
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/table.h>
#include <cmath>
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

  // Filter out rows where gap_up is null (no gap exists)
  auto gap_up_col = df["gap_up"];
  auto has_gap_mask = !gap_up_col.is_null();
  auto working_df = df.loc(has_gap_mask);

  if (working_df.num_rows() == 0) {
    SPDLOG_WARN("No gaps found in data");
    return result;
  }

  SPDLOG_INFO("Before filter_gaps: {} rows", working_df.num_rows());
  auto filtered_gaps = filter_gaps(working_df);
  SPDLOG_INFO("After filter_gaps: {} rows", filtered_gaps.num_rows());
  if (filtered_gaps.num_rows() == 0) {
    SPDLOG_WARN("No gaps found after filtering");
    return result;
  }

  // Build comprehensive table first - this becomes our single source of truth
  // We'll always build it internally even if not displayed
  auto comprehensive_table_data = build_comprehensive_table_data(filtered_gaps);

  // Now all other visualizations are derived from this table data

  // Debug: Check what columns are available in the table
  SPDLOG_INFO("Comprehensive table has {} columns:", comprehensive_table_data.arrow_table->num_columns());
  for (int i = 0; i < comprehensive_table_data.arrow_table->num_columns(); ++i) {
    auto col_name = comprehensive_table_data.arrow_table->field(i)->name();
    SPDLOG_INFO("  Column {}: {}", i, col_name);
  }

  // 1. Summary cards - now from table data
  auto cards = compute_summary_cards_from_table(comprehensive_table_data);
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

  bool show_fill_analysis = getBoolOpt("show_fill_analysis", true);
  bool show_day_of_week_analysis =
      getBoolOpt("show_day_of_week_analysis", true);
  bool show_fill_time_analysis = getBoolOpt("show_fill_time_analysis", true);
  bool show_performance_analysis =
      getBoolOpt("show_performance_analysis", true);
  bool show_distribution_histogram =
      getBoolOpt("show_distribution_histogram", true);
  // histogram_bins now handled inside create_gap_distribution_from_data

  // 2. Fill rate analysis tables (separate for gap up and gap down) - from table data
  if (show_fill_analysis) {
    auto [gap_up_table, gap_down_table] = create_fill_rate_tables_from_data(comprehensive_table_data);
    SPDLOG_INFO("Adding fill rate tables - Gap Up table title: {}, Gap Down table title: {}",
                gap_up_table.title(), gap_down_table.title());
    *result.mutable_tables()->add_tables() = std::move(gap_up_table);
    *result.mutable_tables()->add_tables() = std::move(gap_down_table);
  }

  // 3. Day of week frequency bar chart - from table data
  if (show_day_of_week_analysis) {
    Chart chart;
    *chart.mutable_bar_def() = create_day_of_week_chart_from_data(comprehensive_table_data);
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 4. Time bucket analysis table
  if (show_fill_time_analysis) {
    *result.mutable_tables()->add_tables() = create_frequency_table(
        filtered_gaps, "fill_time", "Gap Frequency by Time");
  }

  // 5. Streak visualization - removed per requirements

  // 6. Gap size distribution histogram - from table data
  if (show_distribution_histogram) {
    Chart chart;
    *chart.mutable_histogram_def() = create_gap_distribution_from_data(comprehensive_table_data);
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 7. Performance analysis
  if (show_performance_analysis) {
    *result.mutable_tables()->add_tables() = create_frequency_table(
        filtered_gaps, "close_performance", "Gap Fill vs Close Performance");
  }

  // 8. Time distribution pie chart - from table data
  if (show_fill_time_analysis) {
    Chart chart;
    *chart.mutable_pie_def() = create_time_distribution_from_data(comprehensive_table_data);
    *result.mutable_charts()->add_charts() = std::move(chart);
  }

  // 9. Comprehensive gap table (replaces simple details table if enabled)
  bool show_comprehensive_table = getBoolOpt("show_comprehensive_table", true);
  if (show_comprehensive_table) {
    auto comp_table = create_comprehensive_gap_table(filtered_gaps);
    SPDLOG_INFO("Adding comprehensive gap table with {} rows", comp_table.data().rows_size());

    *result.mutable_tables()->add_tables() = std::move(comp_table);
  } else {
    // Fallback to simple gap details table
    *result.mutable_tables()->add_tables() = create_gap_details_table(
        filtered_gaps, std::numeric_limits<uint32_t>::max()); // No limit
  }

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

  // Start with full mask (all true) - input already filtered for non-null gaps
  SPDLOG_INFO("filter_gaps: Starting with {} rows", df.num_rows());
  auto mask =
      make_series(df.index(), std::vector<bool>(df.num_rows(), true), "mask");

  // Access columns directly - gap_up is boolean (true=up, false=down)
  auto gap_up = df["gap_up"];
  auto gap_filled = df["gap_filled"];  // Use directly from input
  auto gap_size = df["gap_size"];

  // Derive gap types from gap_up boolean
  auto is_up = gap_up == Scalar{true};
  auto is_down = gap_up == Scalar{false};

  // Convert gap_size (absolute price) to percentage using prior session close
  auto psc = df["psc"];
  auto gap_pct = (gap_size / psc) * Scalar{100.0};
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
  SPDLOG_INFO("filter_gaps: Applying percentage bounds filter: {} to {}", min_gap_pct, max_gap_pct);
  mask = mask && (pct_abs >= Scalar{min_gap_pct}) &&
         (pct_abs <= Scalar{max_gap_pct});
  // Check mask status
  auto mask_sum = mask.sum().cast_int64().as_int64();
  SPDLOG_INFO("filter_gaps: After percentage filter, {} rows pass", mask_sum);

  // Filled / unfilled filter
  bool only_filled = false;
  bool only_unfilled = false;
  if (only_filled && !only_unfilled) {
    SPDLOG_INFO("filter_gaps: Applying only_filled filter");
    mask = mask && gap_filled;
  } else if (only_unfilled && !only_filled) {
    SPDLOG_INFO("filter_gaps: Applying only_unfilled filter");
    mask = mask && !gap_filled;
  }
  auto mask_sum2 = mask.sum().cast_int64().as_int64();
  SPDLOG_INFO("filter_gaps: After filled/unfilled filter, {} rows pass", mask_sum2);

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
  SPDLOG_INFO("filter_gaps: Applying mask to filter rows");
  auto filtered = df.loc(mask);
  SPDLOG_INFO("filter_gaps: After mask application, {} rows remain", filtered.num_rows());
  auto index_dt = filtered.index()->array().dt();

  // Direct access - gap_up is boolean (true=up, false=down)
  auto gap_up_filtered = filtered["gap_up"];
  auto gap_filled_filtered = filtered["gap_filled"];

  // Add derived columns to the filtered DataFrame
  SPDLOG_INFO("Adding derived columns to {} rows", filtered.num_rows());
  // Derive day_of_week from index
  std::vector<std::string> day_of_week_data;
  std::vector<std::string> time_bucket_data;
  std::vector<std::string> close_performance_data;

  // Reserve memory for efficiency
  day_of_week_data.reserve(filtered.num_rows());
  time_bucket_data.reserve(filtered.num_rows());
  close_performance_data.reserve(filtered.num_rows());

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

  // Add gap_down column derived from gap_up (true=up, false=down)
  auto gap_down_col = gap_up_filtered == epoch_frame::Scalar{false};
  all_series.push_back(gap_down_col);
  all_columns.push_back("gap_down");

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

  // Count gap types using gap_up boolean (true=up, false=down)
  auto gap_up_series = gaps["gap_up"];
  auto gap_up_true = gap_up_series == epoch_frame::Scalar{true};
  auto gap_up_count = gap_up_true.sum().cast_int64().as_int64();

  auto gap_up_false = gap_up_series == epoch_frame::Scalar{false};
  auto gap_down_count = gap_up_false.sum().cast_int64().as_int64();

  // Count filled gaps using gap_filled input column
  auto gap_filled = gaps["gap_filled"];
  auto filled_count = gap_filled.sum().cast_int64().as_int64();

  // Calculate gap percentages from gap_size (absolute price) and prior close
  auto gap_pct = (gaps["gap_size"] / gaps["psc"]) * epoch_frame::Scalar{100.0};
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

// Removed: Duplicate function - use create_day_of_week_chart_from_data instead

std::pair<Table, Table> GapReport::create_fill_rate_tables(const epoch_frame::DataFrame &gaps) const {
  // Count gap types and fills
  auto gap_up_mask = gaps["gap_up"] == epoch_frame::Scalar{true};
  auto gap_down_mask = gaps["gap_up"] == epoch_frame::Scalar{false};
  auto gap_filled = gaps["gap_filled"];

  // Check which gaps are filled
  auto gap_up_filled_mask = gap_up_mask && gap_filled;
  auto gap_down_filled_mask = gap_down_mask && gap_filled;

  // Count unfilled gaps
  auto gap_up_unfilled_mask = gap_up_mask && !gap_filled;
  auto gap_down_unfilled_mask = gap_down_mask && !gap_filled;

  auto gap_up_total = gap_up_mask.sum().cast_int64().as_int64();
  auto gap_up_filled = gap_up_filled_mask.sum().cast_int64().as_int64();
  auto gap_up_unfilled = gap_up_unfilled_mask.sum().cast_int64().as_int64();

  auto gap_down_total = gap_down_mask.sum().cast_int64().as_int64();
  auto gap_down_filled = gap_down_filled_mask.sum().cast_int64().as_int64();
  auto gap_down_unfilled = gap_down_unfilled_mask.sum().cast_int64().as_int64();

  // Calculate percentages relative to total gaps
  auto total_gaps = gap_up_total + gap_down_total;

  double gap_up_total_pct = total_gaps > 0 ?
    (static_cast<double>(gap_up_total) / total_gaps * 100) : 0;
  double gap_up_filled_pct = gap_up_total > 0 ?
    (static_cast<double>(gap_up_filled) / gap_up_total * 100) : 0;
  double gap_up_unfilled_pct = gap_up_total > 0 ?
    (static_cast<double>(gap_up_unfilled) / gap_up_total * 100) : 0;

  double gap_down_total_pct = total_gaps > 0 ?
    (static_cast<double>(gap_down_total) / total_gaps * 100) : 0;
  double gap_down_filled_pct = gap_down_total > 0 ?
    (static_cast<double>(gap_down_filled) / gap_down_total * 100) : 0;
  double gap_down_unfilled_pct = gap_down_total > 0 ?
    (static_cast<double>(gap_down_unfilled) / gap_down_total * 100) : 0;

  // Create Gap Up table
  arrow::StringBuilder gap_up_category_builder;
  arrow::Int64Builder gap_up_frequency_builder;
  arrow::DoubleBuilder gap_up_percentage_builder;

  // Add rows for gap up table
  ARROW_UNUSED(gap_up_category_builder.Append("gap up"));
  ARROW_UNUSED(gap_up_frequency_builder.Append(gap_up_total));
  ARROW_UNUSED(gap_up_percentage_builder.Append(gap_up_total_pct));

  ARROW_UNUSED(gap_up_category_builder.Append("gap up filled"));
  ARROW_UNUSED(gap_up_frequency_builder.Append(gap_up_filled));
  ARROW_UNUSED(gap_up_percentage_builder.Append(gap_up_filled_pct));

  ARROW_UNUSED(gap_up_category_builder.Append("gap up not filled"));
  ARROW_UNUSED(gap_up_frequency_builder.Append(gap_up_unfilled));
  ARROW_UNUSED(gap_up_percentage_builder.Append(gap_up_unfilled_pct));

  std::shared_ptr<arrow::Array> gap_up_category_array, gap_up_frequency_array, gap_up_percentage_array;
  ARROW_UNUSED(gap_up_category_builder.Finish(&gap_up_category_array));
  ARROW_UNUSED(gap_up_frequency_builder.Finish(&gap_up_frequency_array));
  ARROW_UNUSED(gap_up_percentage_builder.Finish(&gap_up_percentage_array));

  auto gap_up_schema = arrow::schema({
      arrow::field("category", arrow::utf8()),
      arrow::field("frequency", arrow::int64()),
      arrow::field("percentage", arrow::float64())
  });

  auto gap_up_table_data = arrow::Table::Make(
      gap_up_schema,
      {gap_up_category_array, gap_up_frequency_array, gap_up_percentage_array}
  );

  Table gap_up_table;
  gap_up_table.set_type(epoch_proto::WidgetDataTable);
  gap_up_table.set_category("Reports");
  gap_up_table.set_title("Gap Up Fill Analysis");

  TableColumnHelper::AddStringColumn(gap_up_table, "category", "category");
  TableColumnHelper::AddIntegerColumn(gap_up_table, "frequency", "frequency");
  TableColumnHelper::AddPercentColumn(gap_up_table, "percentage", "percentage");

  *gap_up_table.mutable_data() = MakeTableDataFromArrow(gap_up_table_data);

  // Create Gap Down table
  arrow::StringBuilder gap_down_category_builder;
  arrow::Int64Builder gap_down_frequency_builder;
  arrow::DoubleBuilder gap_down_percentage_builder;

  // Add rows for gap down table
  ARROW_UNUSED(gap_down_category_builder.Append("gap down"));
  ARROW_UNUSED(gap_down_frequency_builder.Append(gap_down_total));
  ARROW_UNUSED(gap_down_percentage_builder.Append(gap_down_total_pct));

  ARROW_UNUSED(gap_down_category_builder.Append("gap down filled"));
  ARROW_UNUSED(gap_down_frequency_builder.Append(gap_down_filled));
  ARROW_UNUSED(gap_down_percentage_builder.Append(gap_down_filled_pct));

  ARROW_UNUSED(gap_down_category_builder.Append("gap down not filled"));
  ARROW_UNUSED(gap_down_frequency_builder.Append(gap_down_unfilled));
  ARROW_UNUSED(gap_down_percentage_builder.Append(gap_down_unfilled_pct));

  std::shared_ptr<arrow::Array> gap_down_category_array, gap_down_frequency_array, gap_down_percentage_array;
  ARROW_UNUSED(gap_down_category_builder.Finish(&gap_down_category_array));
  ARROW_UNUSED(gap_down_frequency_builder.Finish(&gap_down_frequency_array));
  ARROW_UNUSED(gap_down_percentage_builder.Finish(&gap_down_percentage_array));

  auto gap_down_schema = arrow::schema({
      arrow::field("category", arrow::utf8()),
      arrow::field("frequency", arrow::int64()),
      arrow::field("percentage", arrow::float64())
  });

  auto gap_down_table_data = arrow::Table::Make(
      gap_down_schema,
      {gap_down_category_array, gap_down_frequency_array, gap_down_percentage_array}
  );

  Table gap_down_table;
  gap_down_table.set_type(epoch_proto::WidgetDataTable);
  gap_down_table.set_category("Reports");
  gap_down_table.set_title("Gap Down Fill Analysis");

  TableColumnHelper::AddStringColumn(gap_down_table, "category", "category");
  TableColumnHelper::AddIntegerColumn(gap_down_table, "frequency", "frequency");
  TableColumnHelper::AddPercentColumn(gap_down_table, "percentage", "percentage");

  *gap_down_table.mutable_data() = MakeTableDataFromArrow(gap_down_table_data);

  return {std::move(gap_up_table), std::move(gap_down_table)};
}

GapTableData GapReport::build_comprehensive_table_data(const epoch_frame::DataFrame &gaps) const {
  GapTableData data;

  // Always include all columns in the internal data structure
  // The display functions will choose what to show based on configuration

  auto num_rows = static_cast<int>(gaps.num_rows());

  // Create builders for all columns
  auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");
  arrow::TimestampBuilder date_builder(timestamp_type, arrow::default_memory_pool());
  arrow::DoubleBuilder gap_size_builder;
  arrow::StringBuilder gap_type_builder, gap_filled_builder;
  arrow::StringBuilder weekday_builder, gap_category_builder, performance_builder, fill_time_builder;

  // Track aggregations as we build
  data.total_gaps = num_rows;

  for (int64_t i = 0; i < num_rows; ++i) {
    // Date column
    auto date_scalar = gaps.index()->at(i);
    int64_t timestamp_ms = date_scalar.timestamp().value / 1000000;
    ARROW_UNUSED(date_builder.Append(timestamp_ms));

    // Gap size (absolute price) - convert to percentage
    auto gap_size = gaps["gap_size"].iloc(i).as_double();
    auto psc_val = gaps["psc"].iloc(i).as_double();
    auto gap_size_pct = std::abs(gap_size / psc_val * 100);
    ARROW_UNUSED(gap_size_builder.Append(gap_size_pct));

    // Gap type and filled status
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto is_filled = gaps["gap_filled"].iloc(i).as_bool();

    ARROW_UNUSED(gap_type_builder.Append(is_gap_up ? "gap up" : "gap down"));
    ARROW_UNUSED(gap_filled_builder.Append(is_filled ? "filled" : "not filled"));

    // Update aggregations
    if (is_gap_up) {
      data.gap_up_count++;
      if (is_filled) data.gap_up_filled++;
    } else {
      data.gap_down_count++;
      if (is_filled) data.gap_down_filled++;
    }
    if (is_filled) data.filled_count++;

    // Weekday
    auto datetime = gaps.index()->at(i).to_datetime();
    const char *days[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
    ARROW_UNUSED(weekday_builder.Append(days[datetime.weekday()]));

    // Gap category
    std::string category;
    if (gap_size_pct < 0.2) {
      category = "0-0.19%";
    } else if (gap_size_pct < 0.4) {
      category = "0.2-0.39%";
    } else if (gap_size_pct < 0.6) {
      category = "0.4-0.59%";
    } else if (gap_size_pct < 1.0) {
      category = "0.6-0.99%";
    } else if (gap_size_pct < 2.0) {
      category = "1.0-1.99%";
    } else {
      category = "2.0%+";
    }
    ARROW_UNUSED(gap_category_builder.Append(category));

    // Performance
    auto close_val = gaps[closeLiteral].iloc(i).as_double();
    ARROW_UNUSED(performance_builder.Append(close_val > psc_val ? "green" : "red"));

    // Fill time with configurable pivot
    if (is_filled) {
      auto hour = datetime.time().hour.count();
      int pivot_hour = 13; // Default, will be configured from options
      try {
        pivot_hour = static_cast<int>(m_config.GetOptionValue("fill_time_pivot_hour").GetInteger());
      } catch (...) {}

      std::string before_label = "before " + std::to_string(pivot_hour) + ":00";
      std::string after_label = "after " + std::to_string(pivot_hour) + ":00";
      ARROW_UNUSED(fill_time_builder.Append(hour < pivot_hour ? before_label : after_label));
    } else {
      ARROW_UNUSED(fill_time_builder.Append("not filled"));
    }
  }

  // Build the complete table with all columns
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  // Build all arrays
  std::shared_ptr<arrow::Array> date_array, gap_size_array, gap_type_array, gap_filled_array;
  std::shared_ptr<arrow::Array> weekday_array, gap_category_array, performance_array, fill_time_array;

  ARROW_UNUSED(date_builder.Finish(&date_array));
  ARROW_UNUSED(gap_size_builder.Finish(&gap_size_array));
  ARROW_UNUSED(gap_type_builder.Finish(&gap_type_array));
  ARROW_UNUSED(gap_filled_builder.Finish(&gap_filled_array));
  ARROW_UNUSED(weekday_builder.Finish(&weekday_array));
  ARROW_UNUSED(gap_category_builder.Finish(&gap_category_array));
  ARROW_UNUSED(performance_builder.Finish(&performance_array));
  ARROW_UNUSED(fill_time_builder.Finish(&fill_time_array));

  // Add all fields and arrays
  fields.push_back(arrow::field("date", timestamp_type));
  arrays.push_back(date_array);
  data.date_col = 0;

  fields.push_back(arrow::field("gap_size", arrow::float64()));
  arrays.push_back(gap_size_array);
  data.gap_size_col = 1;

  fields.push_back(arrow::field("gap_type", arrow::utf8()));
  arrays.push_back(gap_type_array);
  data.gap_type_col = 2;

  fields.push_back(arrow::field("gap_filled", arrow::utf8()));
  arrays.push_back(gap_filled_array);
  data.gap_filled_col = 3;

  fields.push_back(arrow::field("weekday", arrow::utf8()));
  arrays.push_back(weekday_array);
  data.weekday_col = 4;

  fields.push_back(arrow::field("gap_category", arrow::utf8()));
  arrays.push_back(gap_category_array);
  data.gap_category_col = 5;

  fields.push_back(arrow::field("performance", arrow::utf8()));
  arrays.push_back(performance_array);
  data.performance_col = 6;

  fields.push_back(arrow::field("fill_time", arrow::utf8()));
  arrays.push_back(fill_time_array);
  data.fill_time_col = 7;

  auto schema = arrow::schema(fields);
  data.arrow_table = arrow::Table::Make(schema, arrays);

  return data;
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


HistogramDef
GapReport::create_gap_distribution(const epoch_frame::DataFrame &gaps,
                                   uint32_t bins) const {
  // Extract gap percentages from gap_size (absolute price) and prior close
  auto gap_pct = (gaps["gap_size"] / gaps["psc"]) * epoch_frame::Scalar{100.0};
  auto abs_gap_pct = gap_pct.abs();

  auto data_array = abs_gap_pct.array()->chunk(0);

  // Use the new HistogramBuilder for cleaner construction
  return HistogramBuilder({
      .id = "gap_distribution",
      .title = "Gap Size Distribution",
      .category = "Reports"
  })
  .setXAxis(MakePercentageAxis("Gap Size (%)"))
  .setYAxis(MakeLinearAxis("Frequency"))
  .setData(data_array)
  .setBins(bins)
  .build();
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
    if (hour == 9 && datetime.time().minute.count() >= 30) {
      period = "Opening (9:30-10:00)";
    } else if (hour == 10) {
      period = "Early Morning (10:00-11:00)";
    } else if (hour == 11) {
      period = "Late Morning (11:00-12:00)";
    } else if (hour >= 12 && hour < 14) {
      period = "Midday (12:00-14:00)";
    } else if (hour >= 14 && hour < 16) {
      period = "Afternoon (14:00-16:00)";
    } else {
      period = "After-Hours";
    }

    time_counts[period]++;
  }

  // Use builder pattern for cleaner construction
  std::vector<PieSlice> time_slices;
  for (const auto &[bucket, count] : time_counts) {
    time_slices.emplace_back(bucket, static_cast<double>(count));
  }

  return PieChartBuilder({
      .id = "gap_time_distribution",
      .title = "Gap Timing Distribution",
      .category = "Reports"
  })
  .addDataSeries({.name = "Gap Timing"})
  .addSlices(time_slices)
  .build();
}

Table GapReport::create_comprehensive_gap_table(const epoch_frame::DataFrame &gaps) const {
  // Get configuration options using the same pattern as other options
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

  bool show_weekday = getBoolOpt("table_show_weekday", true);
  bool show_gap_category = getBoolOpt("table_show_gap_category", true);
  bool show_performance = getBoolOpt("table_show_performance", true);
  bool show_fill_time = getBoolOpt("table_show_fill_time", true);
  bool combine_gap_direction = getBoolOpt("table_combine_gap_direction", false);
  int max_rows = getIntOpt("table_max_rows", 100);

  // Limit rows
  auto num_rows = std::min(max_rows, static_cast<int>(gaps.num_rows()));

  // Create builders for each potential column
  auto timestamp_type = arrow::timestamp(arrow::TimeUnit::MILLI, "UTC");
  arrow::TimestampBuilder date_builder(timestamp_type, arrow::default_memory_pool());
  arrow::DoubleBuilder gap_size_builder;
  arrow::StringBuilder gap_type_builder, gap_filled_builder, combined_gap_builder;
  arrow::StringBuilder weekday_builder, gap_category_builder, performance_builder, fill_time_builder;

  for (int64_t i = 0; i < num_rows; ++i) {
    // Date column (always included)
    auto date_scalar = gaps.index()->at(i);
    int64_t timestamp_ms = date_scalar.timestamp().value / 1000000;
    ARROW_UNUSED(date_builder.Append(timestamp_ms));

    // Gap size column (always included) - convert from absolute to percentage
    auto gap_size = gaps["gap_size"].iloc(i).as_double();
    auto psc_val = gaps["psc"].iloc(i).as_double();
    auto gap_size_pct = std::abs(gap_size / psc_val * 100);
    ARROW_UNUSED(gap_size_builder.Append(gap_size_pct));

    // Gap type and filled status
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto is_filled = gaps["gap_filled"].iloc(i).as_bool();

    if (combine_gap_direction) {
      // Combined column: "gap up filled", "gap down not filled", etc.
      std::string combined = is_gap_up ? "gap up" : "gap down";
      combined += is_filled ? " filled" : "";
      ARROW_UNUSED(combined_gap_builder.Append(combined));
    } else {
      // Separate columns
      ARROW_UNUSED(gap_type_builder.Append(is_gap_up ? "gap up" : "gap down"));
      ARROW_UNUSED(gap_filled_builder.Append(is_filled ? "filled" : "not filled"));
    }

    // Optional columns
    if (show_weekday) {
      auto datetime = gaps.index()->at(i).to_datetime();
      const char *days[] = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
      ARROW_UNUSED(weekday_builder.Append(days[datetime.weekday()]));
    }

    if (show_gap_category) {
      // Categorize gap size
      std::string category;
      if (gap_size_pct < 0.2) {
        category = "0-0.19%";
      } else if (gap_size_pct < 0.4) {
        category = "0.2-0.39%";
      } else if (gap_size_pct < 0.6) {
        category = "0.4-0.59%";
      } else if (gap_size_pct < 1.0) {
        category = "0.6-0.99%";
      } else if (gap_size_pct < 2.0) {
        category = "1.0-1.99%";
      } else {
        category = "2.0%+";
      }
      ARROW_UNUSED(gap_category_builder.Append(category));
    }

    if (show_performance) {
      auto close_val = gaps[closeLiteral].iloc(i).as_double();
      auto psc_val = gaps["psc"].iloc(i).as_double();
      ARROW_UNUSED(performance_builder.Append(close_val > psc_val ? "green" : "red"));
    }

    if (show_fill_time && is_filled) {
      // Simple categorization - can be enhanced with actual fill time data
      auto datetime = gaps.index()->at(i).to_datetime();
      auto hour = datetime.time().hour.count();
      int pivot_hour = getIntOpt("fill_time_pivot_hour", 13);

      std::string before_label = "before " + std::to_string(pivot_hour) + ":00";
      std::string after_label = "after " + std::to_string(pivot_hour) + ":00";
      ARROW_UNUSED(fill_time_builder.Append(hour < pivot_hour ? before_label : after_label));
    } else if (show_fill_time) {
      ARROW_UNUSED(fill_time_builder.Append("not filled"));
    }
  }

  // Build schema and arrays based on enabled columns
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;

  // Always include date
  fields.push_back(arrow::field("date", timestamp_type));
  std::shared_ptr<arrow::Array> date_array;
  ARROW_UNUSED(date_builder.Finish(&date_array));
  arrays.push_back(date_array);

  // Always include gap size
  fields.push_back(arrow::field("gap size", arrow::float64()));
  std::shared_ptr<arrow::Array> gap_size_array;
  ARROW_UNUSED(gap_size_builder.Finish(&gap_size_array));
  arrays.push_back(gap_size_array);

  // Gap category (if enabled and before gap type for logical ordering)
  if (show_gap_category) {
    fields.push_back(arrow::field("gap category", arrow::utf8()));
    std::shared_ptr<arrow::Array> gap_category_array;
    ARROW_UNUSED(gap_category_builder.Finish(&gap_category_array));
    arrays.push_back(gap_category_array);
  }

  // Gap type and filled status
  if (combine_gap_direction) {
    fields.push_back(arrow::field("gap direction", arrow::utf8()));
    std::shared_ptr<arrow::Array> combined_gap_array;
    ARROW_UNUSED(combined_gap_builder.Finish(&combined_gap_array));
    arrays.push_back(combined_gap_array);
  } else {
    fields.push_back(arrow::field("gap type", arrow::utf8()));
    std::shared_ptr<arrow::Array> gap_type_array;
    ARROW_UNUSED(gap_type_builder.Finish(&gap_type_array));
    arrays.push_back(gap_type_array);

    fields.push_back(arrow::field("gap filled", arrow::utf8()));
    std::shared_ptr<arrow::Array> gap_filled_array;
    ARROW_UNUSED(gap_filled_builder.Finish(&gap_filled_array));
    arrays.push_back(gap_filled_array);
  }

  // Optional columns
  if (show_weekday) {
    fields.push_back(arrow::field("weekday", arrow::utf8()));
    std::shared_ptr<arrow::Array> weekday_array;
    ARROW_UNUSED(weekday_builder.Finish(&weekday_array));
    arrays.push_back(weekday_array);
  }

  if (show_performance) {
    fields.push_back(arrow::field("performance", arrow::utf8()));
    std::shared_ptr<arrow::Array> performance_array;
    ARROW_UNUSED(performance_builder.Finish(&performance_array));
    arrays.push_back(performance_array);
  }

  if (show_fill_time) {
    fields.push_back(arrow::field("gap fill time", arrow::utf8()));
    std::shared_ptr<arrow::Array> fill_time_array;
    ARROW_UNUSED(fill_time_builder.Finish(&fill_time_array));
    arrays.push_back(fill_time_array);
  }

  auto schema = arrow::schema(fields);
  auto table = arrow::Table::Make(schema, arrays);

  Table result_table;
  result_table.set_type(epoch_proto::WidgetDataTable);
  result_table.set_category("Reports");
  result_table.set_title("Comprehensive Gap Analysis");

  // Add columns to protobuf table definition
  TableColumnHelper::AddTimestampColumn(result_table, "date", "date");
  TableColumnHelper::AddPercentColumn(result_table, "gap size", "gap_size");

  if (show_gap_category) {
    TableColumnHelper::AddStringColumn(result_table, "gap category", "gap_category");
  }

  if (combine_gap_direction) {
    TableColumnHelper::AddStringColumn(result_table, "gap direction", "gap_direction");
  } else {
    TableColumnHelper::AddStringColumn(result_table, "gap type", "gap_type");
    TableColumnHelper::AddStringColumn(result_table, "gap filled", "gap_filled");
  }

  if (show_weekday) {
    TableColumnHelper::AddStringColumn(result_table, "weekday", "weekday");
  }

  if (show_performance) {
    TableColumnHelper::AddStringColumn(result_table, "performance", "performance");
  }

  if (show_fill_time) {
    TableColumnHelper::AddStringColumn(result_table, "gap fill time", "gap_fill_time");
  }

  *result_table.mutable_data() = MakeTableDataFromArrow(table);

  return result_table;
}

Table GapReport::create_gap_details_table(const epoch_frame::DataFrame &gaps,
                                          uint32_t limit) const {
  // Limit rows
  auto num_rows = std::min(static_cast<int64_t>(limit),
                           static_cast<int64_t>(gaps.num_rows()));

  // Build table columns
  arrow::Date32Builder date_builder;
  arrow::StringBuilder gap_type_builder, performance_builder, is_filled_builder;
  arrow::DoubleBuilder gap_pct_builder, fill_pct_builder;

  for (int64_t i = 0; i < num_rows; ++i) {
    // Convert index to date32 (days since epoch)
    auto date_scalar = gaps.index()->at(i).to_date().date();
    ARROW_UNUSED(date_builder.Append(static_cast<int32_t>(date_scalar.toordinal())));

    // Determine gap type from boolean columns
    auto is_gap_up = gaps["gap_up"].iloc(i).as_bool();
    auto gap_type_str = is_gap_up ? "Gap Up" : "Gap Down";
    ARROW_UNUSED(gap_type_builder.Append(gap_type_str));

    // Calculate gap percentage from gap_size (absolute price)
    auto gap_size = gaps["gap_size"].iloc(i).as_double();
    auto psc_val = gaps["psc"].iloc(i).as_double();
    auto gap_pct = std::abs(gap_size / psc_val * 100);
    ARROW_UNUSED(gap_pct_builder.Append(gap_pct));

    // Check if filled using gap_filled column
    auto is_filled = gaps["gap_filled"].iloc(i).as_bool();
    ARROW_UNUSED(is_filled_builder.Append(is_filled ? "Yes" : "No"));

    // Fill percentage (from fill_fraction * 100, not gap_pct)
    auto fill_frac = gaps["fill_fraction"].iloc(i).as_double();
    auto fill_pct = fill_frac * 100.0;
    ARROW_UNUSED(fill_pct_builder.Append(fill_pct));

    // Derive close performance from close vs psc
    auto close_val = gaps[closeLiteral].iloc(i).as_double();
    auto performance_str = close_val > psc_val ? "green" : "red";
    ARROW_UNUSED(performance_builder.Append(performance_str));
  }

  std::shared_ptr<arrow::Array> date_array, gap_type_array,
      gap_pct_array, is_filled_array, fill_pct_array, performance_array;

  ARROW_UNUSED(date_builder.Finish(&date_array));
  ARROW_UNUSED(gap_type_builder.Finish(&gap_type_array));
  ARROW_UNUSED(gap_pct_builder.Finish(&gap_pct_array));
  ARROW_UNUSED(is_filled_builder.Finish(&is_filled_array));
  ARROW_UNUSED(fill_pct_builder.Finish(&fill_pct_array));
  ARROW_UNUSED(performance_builder.Finish(&performance_array));

  auto schema = arrow::schema({arrow::field("Date", arrow::date32()),
                               arrow::field("Type", arrow::utf8()),
                               arrow::field("Gap %", arrow::float64()),
                               arrow::field("Filled", arrow::utf8()),
                               arrow::field("Fill %", arrow::float64()),
                               arrow::field("Performance", arrow::utf8())});

  auto table = arrow::Table::Make(
      schema, {date_array, gap_type_array, gap_pct_array,
               is_filled_array, fill_pct_array, performance_array});

  Table result_table;
  result_table.set_type(epoch_proto::WidgetDataTable);
  result_table.set_category("Reports");
  result_table.set_title("Recent Gap Details");

  // Add columns using helpers - Use Date not Timestamp
  TableColumnHelper::AddDateColumn(result_table, "Date", "date");
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
    // Parse YYYY-MM format to timestamp
    // month_str format: "2024-01"
    int year, month;
    std::sscanf(month_str.c_str(), "%d-%d", &year, &month);

    // Convert to timestamp (first day of month, midnight UTC)
    std::tm timeinfo = {};
    timeinfo.tm_year = year - 1900;
    timeinfo.tm_mon = month - 1;
    timeinfo.tm_mday = 1;
    auto timestamp_s = std::mktime(&timeinfo);
    int64_t timestamp_ms = timestamp_s * 1000;

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

// Helper functions that work with GapTableData
std::vector<CardDef> GapReport::compute_summary_cards_from_table(const GapTableData &data) const {
  std::vector<CardDef> cards;

  // Total gaps card
  CardDef total_gaps_card;
  total_gaps_card.set_type(epoch_proto::WidgetCard);
  total_gaps_card.set_category("Reports");
  CardDataHelper::AddIntegerField(total_gaps_card, "Total Gaps",
      epoch_frame::Scalar{data.total_gaps});
  cards.push_back(std::move(total_gaps_card));

  // Gap up percentage - floor to match expected integer values
  double gap_up_pct = data.total_gaps > 0 ?
      std::floor(data.gap_up_count * 100.0 / data.total_gaps) : 0;
  CardDef gap_up_pct_card;
  gap_up_pct_card.set_type(epoch_proto::WidgetCard);
  gap_up_pct_card.set_category("Reports");
  CardDataHelper::AddPercentField(gap_up_pct_card, "Gap Up %",
      epoch_frame::Scalar{gap_up_pct});
  cards.push_back(std::move(gap_up_pct_card));

  // Gap down percentage - floor to match expected integer values
  double gap_down_pct = data.total_gaps > 0 ?
      std::floor(data.gap_down_count * 100.0 / data.total_gaps) : 0;
  CardDef gap_down_pct_card;
  gap_down_pct_card.set_type(epoch_proto::WidgetCard);
  gap_down_pct_card.set_category("Reports");
  CardDataHelper::AddPercentField(gap_down_pct_card, "Gap Down %",
      epoch_frame::Scalar{gap_down_pct});
  cards.push_back(std::move(gap_down_pct_card));

  // Overall fill rate - floor to match expected integer values
  double fill_rate = data.total_gaps > 0 ?
      std::floor(data.filled_count * 100.0 / data.total_gaps) : 0;
  CardDef fill_rate_card;
  fill_rate_card.set_type(epoch_proto::WidgetCard);
  fill_rate_card.set_category("Reports");
  CardDataHelper::AddPercentField(fill_rate_card, "Fill Rate",
      epoch_frame::Scalar{fill_rate});
  cards.push_back(std::move(fill_rate_card));

  return cards;
}

std::pair<Table, Table> GapReport::create_fill_rate_tables_from_data(const GapTableData &data) const {
  // Gap Up Fill Rate Table
  Table gap_up_table;
  gap_up_table.set_type(epoch_proto::WidgetDataTable);
  gap_up_table.set_category("Reports");
  gap_up_table.set_title("Gap Up Fill Analysis");

  // Build Gap Up table
  arrow::StringBuilder category_builder_up;
  arrow::Int64Builder count_builder_up;
  arrow::DoubleBuilder percentage_builder_up;

  ARROW_UNUSED(category_builder_up.Append("Filled"));
  ARROW_UNUSED(count_builder_up.Append(data.gap_up_filled));
  double gap_up_filled_pct = data.gap_up_count > 0 ?
      (data.gap_up_filled * 100.0 / data.gap_up_count) : 0;
  ARROW_UNUSED(percentage_builder_up.Append(gap_up_filled_pct));

  ARROW_UNUSED(category_builder_up.Append("Not Filled"));
  int64_t gap_up_not_filled = data.gap_up_count - data.gap_up_filled;
  ARROW_UNUSED(count_builder_up.Append(gap_up_not_filled));
  double gap_up_not_filled_pct = data.gap_up_count > 0 ?
      (gap_up_not_filled * 100.0 / data.gap_up_count) : 0;
  ARROW_UNUSED(percentage_builder_up.Append(gap_up_not_filled_pct));

  std::shared_ptr<arrow::Array> category_array_up, count_array_up, percentage_array_up;
  ARROW_UNUSED(category_builder_up.Finish(&category_array_up));
  ARROW_UNUSED(count_builder_up.Finish(&count_array_up));
  ARROW_UNUSED(percentage_builder_up.Finish(&percentage_array_up));

  auto schema_up = arrow::schema({
      arrow::field("Category", arrow::utf8()),
      arrow::field("Count", arrow::int64()),
      arrow::field("Percentage", arrow::float64())
  });

  auto table_up = arrow::Table::Make(schema_up,
      {category_array_up, count_array_up, percentage_array_up});

  TableColumnHelper::AddStringColumn(gap_up_table, "Category", "category");
  TableColumnHelper::AddIntegerColumn(gap_up_table, "Count", "count");
  TableColumnHelper::AddPercentColumn(gap_up_table, "Percentage", "percentage");

  *gap_up_table.mutable_data() = MakeTableDataFromArrow(table_up);

  // Gap Down Fill Rate Table
  Table gap_down_table;
  gap_down_table.set_type(epoch_proto::WidgetDataTable);
  gap_down_table.set_category("Reports");
  gap_down_table.set_title("Gap Down Fill Analysis");

  // Build Gap Down table
  arrow::StringBuilder category_builder_down;
  arrow::Int64Builder count_builder_down;
  arrow::DoubleBuilder percentage_builder_down;

  ARROW_UNUSED(category_builder_down.Append("Filled"));
  ARROW_UNUSED(count_builder_down.Append(data.gap_down_filled));
  double gap_down_filled_pct = data.gap_down_count > 0 ?
      (data.gap_down_filled * 100.0 / data.gap_down_count) : 0;
  ARROW_UNUSED(percentage_builder_down.Append(gap_down_filled_pct));

  ARROW_UNUSED(category_builder_down.Append("Not Filled"));
  int64_t gap_down_not_filled = data.gap_down_count - data.gap_down_filled;
  ARROW_UNUSED(count_builder_down.Append(gap_down_not_filled));
  double gap_down_not_filled_pct = data.gap_down_count > 0 ?
      (gap_down_not_filled * 100.0 / data.gap_down_count) : 0;
  ARROW_UNUSED(percentage_builder_down.Append(gap_down_not_filled_pct));

  std::shared_ptr<arrow::Array> category_array_down, count_array_down, percentage_array_down;
  ARROW_UNUSED(category_builder_down.Finish(&category_array_down));
  ARROW_UNUSED(count_builder_down.Finish(&count_array_down));
  ARROW_UNUSED(percentage_builder_down.Finish(&percentage_array_down));

  auto schema_down = arrow::schema({
      arrow::field("Category", arrow::utf8()),
      arrow::field("Count", arrow::int64()),
      arrow::field("Percentage", arrow::float64())
  });

  auto table_down = arrow::Table::Make(schema_down,
      {category_array_down, count_array_down, percentage_array_down});

  TableColumnHelper::AddStringColumn(gap_down_table, "Category", "category");
  TableColumnHelper::AddIntegerColumn(gap_down_table, "Count", "count");
  TableColumnHelper::AddPercentColumn(gap_down_table, "Percentage", "percentage");

  *gap_down_table.mutable_data() = MakeTableDataFromArrow(table_down);

  return {gap_up_table, gap_down_table};
}

BarDef GapReport::create_day_of_week_chart_from_data(const GapTableData &data) const {
  // Count gaps by weekday from the table data
  std::unordered_map<std::string, int64_t> weekday_counts;

  if (data.weekday_col >= 0) {
    auto weekday_array = std::static_pointer_cast<arrow::StringArray>(
        data.arrow_table->column(data.weekday_col)->chunk(0));

    for (int64_t i = 0; i < weekday_array->length(); ++i) {
      if (!weekday_array->IsNull(i)) {
        std::string weekday = weekday_array->GetString(i);
        weekday_counts[weekday]++;
      }
    }
  }

  // Order weekdays properly
  const std::vector<std::string> ordered_days = {
      "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
  };

  // Build values in the same order as categories
  std::vector<int64_t> values;
  values.reserve(ordered_days.size());
  for (const auto &day : ordered_days) {
    values.push_back(weekday_counts.count(day) ? weekday_counts.at(day) : 0);
  }

  // Use the new BarChartBuilder for cleaner construction
  auto x_axis = AxisDef{};
  x_axis.set_type(epoch_proto::AxisCategory);
  x_axis.set_label("Day of Week");

  return BarChartBuilder({
      .id = "day_of_week_gaps",
      .title = "Gap Frequency by Day of Week",
      .category = "Reports"
  })
  .setXAxis(x_axis)
  .setYAxis(MakeLinearAxis("Number of Gaps"))
  .addCategories(ordered_days)
  .addValues(values)
  .build();
}

PieDef GapReport::create_time_distribution_from_data(const GapTableData &data) const {
  // Count gaps by fill time from the table data
  std::unordered_map<std::string, int64_t> time_counts;

  if (data.fill_time_col >= 0) {
    auto fill_time_array = std::static_pointer_cast<arrow::StringArray>(
        data.arrow_table->column(data.fill_time_col)->chunk(0));

    for (int64_t i = 0; i < fill_time_array->length(); ++i) {
      if (!fill_time_array->IsNull(i)) {
        std::string time_category = fill_time_array->GetString(i);
        time_counts[time_category]++;
      }
    }
  }

  // Build pie chart
  std::vector<PieSlice> time_slices;
  for (const auto &[category, count] : time_counts) {
    time_slices.emplace_back(category, static_cast<double>(count));
  }

  return PieChartBuilder({
      .id = "gap_fill_time_distribution",
      .title = "Gap Fill Time Distribution",
      .category = "Reports"
  })
  .addDataSeries({.name = "Fill Time"})
  .addSlices(time_slices)
  .build();
}

HistogramDef GapReport::create_gap_distribution_from_data(const GapTableData &data) const {
  // Return empty histogram if no data
  if (data.gap_size_col < 0) {
    return HistogramDef{};
  }

  auto gap_size_array = std::static_pointer_cast<arrow::DoubleArray>(
      data.arrow_table->column(data.gap_size_col)->chunk(0));

  // Get bins count from config
  uint32_t bins = 20; // Default
  try {
    bins = static_cast<uint32_t>(m_config.GetOptionValue("histogram_bins").GetInteger());
  } catch (...) {}

  // Use the new HistogramBuilder for cleaner construction
  return HistogramBuilder({
      .id = "gap_distribution",
      .title = "Gap Size Distribution",
      .category = "Reports"
  })
  .setXAxis(MakePercentageAxis("Gap Size (%)"))
  .setYAxis(MakeLinearAxis("Frequency"))
  .setData(gap_size_array)
  .setBins(bins)
  .build();
}


} // namespace epoch_folio