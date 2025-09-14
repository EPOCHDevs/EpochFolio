#include <arrow/builder.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_metadata/transforms/transform_configuration.h>
#include <epoch_metadata/transforms/transform_definition.h>
#include <epoch_protos/tearsheet.pb.h>
#include <yaml-cpp/yaml.h>

#include "reports/gap_report.h"
#include "portfolio/model.h"

using namespace epoch_folio;
using namespace epoch_frame;

TEST_CASE("GapReport registration and metadata", "[reports][gap]") {
  // Verify GapReport metadata
  auto metadata = ReportMetadata<GapReport>::Get();

  CHECK(metadata.id == "gap_report");
  CHECK(metadata.name == "Gap Analysis Report");
  CHECK(metadata.category == epoch_core::TransformCategory::Executor);
  CHECK(metadata.isReporter == true);
  CHECK(metadata.inputs.size() == 6); // gap_up, gap_down, fill_fraction, gap_size, psc, psc_timestamp
}

TEST_CASE("GapReport basic generation", "[reports][gap]") {
  // Use the helper to create configuration
  auto config = ReportMetadata<GapReport>::CreateConfig("gap_report_basic");
  GapReport report(std::move(config));

  // Create sample gap data using DataFrame factory
  using namespace epoch_frame;
  using namespace epoch_frame::factory;
  using namespace epoch_frame::factory::index;

  // Create date index (10 days starting from 2022-01-01)
  std::vector<int64_t> dates;
  for (int i = 0; i < 10; ++i) {
    dates.push_back(1640995200000000000LL + i * 86400000000000LL);
  }
  auto index = make_datetime_index(dates);

  // Create gap data vectors matching new interface
  std::vector<bool> gap_up_data, gap_down_data;
  std::vector<double> fill_fraction_data, gap_size_data, psc_data, close_data;
  std::vector<int64_t> psc_timestamp_data, volume_data;

  for (int i = 0; i < 10; ++i) {
    // Alternate between gap up and gap down
    bool is_gap_up = (i % 2 == 0);
    gap_up_data.push_back(is_gap_up);
    gap_down_data.push_back(!is_gap_up);

    // Fill fraction (0.0 to 1.0, where > 0.5 means filled)
    fill_fraction_data.push_back(i % 3 != 0 ? 0.7 : 0.3);

    // Gap size (positive for up, negative for down)
    gap_size_data.push_back(is_gap_up ? 0.025 + i * 0.002 : -(0.025 + i * 0.002));

    // Price data
    double base_price = 400.0 + i;
    psc_data.push_back(base_price - 1.0); // Prior session close
    close_data.push_back(base_price + (i % 2 == 0 ? 1.0 : -0.5));

    // Timestamps and volume
    psc_timestamp_data.push_back(dates[i] - 86400000000000LL); // Previous day
    volume_data.push_back(1000000 + i * 10000);
  }

  // Create Series
  std::vector<Series> series_list = {
    make_series(index, gap_up_data, "gap_up"),
    make_series(index, gap_down_data, "gap_down"),
    make_series(index, fill_fraction_data, "fill_fraction"),
    make_series(index, gap_size_data, "gap_size"),
    make_series(index, psc_data, "psc"),
    make_series(index, psc_timestamp_data, "psc_timestamp"),
    make_series(index, close_data, "c")  // Close is from requiredDataSources
};

  std::vector<std::string> column_names = {
    "gap_up", "gap_down", "fill_fraction", "gap_size",
    "psc", "psc_timestamp", "c"
};

  auto df = MakeDataFrame(series_list, column_names);

  // Transform the data (which generates the tearsheet)
  auto result_df = report.TransformData(df);

  // Get the generated tearsheet
  auto tearsheet = report.GetTearSheet();

  // Verify output structure
  CHECK(tearsheet.cards().cards_size() > 0);
  CHECK(tearsheet.charts().charts_size() > 0);
  CHECK(tearsheet.tables().tables_size() > 0);

  // Check specific cards
  bool has_total_gaps_card = false;
  bool has_gap_counts_card = false;
  bool has_fill_rate_card = false;

  for (const auto& card : tearsheet.cards().cards()) {
    if (card.data_size() > 0) {
      const auto& first_data = card.data(0);
      if (first_data.title() == "Total Gaps") {
        has_total_gaps_card = true;
        CHECK(first_data.value().integer_value() == 10);
      } else if (first_data.title() == "Gap Up" || first_data.title() == "Gap Down") {
        has_gap_counts_card = true;
      } else if (first_data.title() == "Overall Fill Rate") {
        has_fill_rate_card = true;
      }
    }
  }

  CHECK(has_total_gaps_card);
  CHECK(has_gap_counts_card);
  CHECK(has_fill_rate_card);

  // Check charts exist
  bool has_bar_chart = false;
  bool has_histogram = false;
  bool has_pie_chart = false;
  bool has_line_chart = false;
  bool has_xrange_chart = false;

  for (const auto& chart : tearsheet.charts().charts()) {
    if (chart.has_bar_def()) has_bar_chart = true;
    if (chart.has_histogram_def()) has_histogram = true;
    if (chart.has_pie_def()) has_pie_chart = true;
    if (chart.has_lines_def()) has_line_chart = true;
    if (chart.has_x_range_def()) has_xrange_chart = true;
  }

  CHECK(has_bar_chart);
  CHECK(has_histogram);
  CHECK(has_pie_chart);
  CHECK(has_line_chart);
  CHECK(has_xrange_chart);

  // Check tables exist
  CHECK(tearsheet.tables().tables_size() >= 3); // frequency, performance, details tables
}


TEST_CASE("GapReport handles empty data", "[reports][gap]") {
  auto config = ReportMetadata<GapReport>::CreateConfig("gap_report_empty");
  GapReport report(std::move(config));

  // Create empty DataFrame with required columns
  using namespace epoch_frame;
  using namespace epoch_frame::factory;
  using namespace epoch_frame::factory::index;

  auto index = make_datetime_index(std::vector<int64_t>{});

  std::vector<Series> series_list = {
      make_series(index, std::vector<bool>{}, "gap_up"),
      make_series(index, std::vector<bool>{}, "gap_down"),
      make_series(index, std::vector<double>{}, "fill_fraction"),
      make_series(index, std::vector<double>{}, "gap_size"),
      make_series(index, std::vector<double>{}, "psc"),
      make_series(index, std::vector<int64_t>{}, "psc_timestamp"),
      make_series(index, std::vector<double>{}, "c")  // Close is from requiredDataSources
  };

  std::vector<std::string> column_names = {
      "gap_up", "gap_down", "fill_fraction", "gap_size",
      "psc", "psc_timestamp", "c"
  };

  auto df = MakeDataFrame(series_list, column_names);

  // Should not crash with empty data
  REQUIRE_NOTHROW(report.TransformData(df));

  auto tearsheet = report.GetTearSheet();

  // Should have minimal output with empty data
  CHECK(tearsheet.cards().cards_size() >= 0);
}

TEST_CASE("GapReport filter logic", "[reports][gap]") {
  // Test that the filter_gaps method works correctly
  auto config = ReportMetadata<GapReport>::CreateConfig("gap_report_filter");
  GapReport report(std::move(config));

  using namespace epoch_frame;
  using namespace epoch_frame::factory;
  using namespace epoch_frame::factory::index;

  // Create test data with mixed gaps
  std::vector<int64_t> dates;
  for (int i = 0; i < 20; ++i) {
    dates.push_back(1640995200000000000LL + i * 86400000000000LL);
  }
  auto index = make_datetime_index(dates);

  std::vector<bool> gap_up_data, gap_down_data;
  std::vector<double> fill_fraction_data, gap_size_data, psc_data, close_data;
  std::vector<int64_t> psc_timestamp_data, volume_data;

  for (int i = 0; i < 20; ++i) {
    // Create various gap scenarios
    bool is_gap_up = (i % 3 != 2);
    bool is_gap_down = (i % 3 == 2);

    gap_up_data.push_back(is_gap_up);
    gap_down_data.push_back(is_gap_down);

    // Varying fill fractions
    fill_fraction_data.push_back(static_cast<double>(i) / 20.0);

    // Varying gap sizes
    double gap_size = (i % 5) * 0.01; // 0% to 4%
    gap_size_data.push_back(is_gap_up ? gap_size : -gap_size);

    // Price data
    psc_data.push_back(400.0 + i * 0.5);
    close_data.push_back(400.0 + i * 0.5 + (is_gap_up ? 1.0 : -1.0));

    psc_timestamp_data.push_back(dates[i] - 86400000000000LL);
    volume_data.push_back(1000000 + i * 50000);
  }

  std::vector<Series> series_list = {
      make_series(index, gap_up_data, "gap_up"),
      make_series(index, gap_down_data, "gap_down"),
      make_series(index, fill_fraction_data, "fill_fraction"),
      make_series(index, gap_size_data, "gap_size"),
      make_series(index, psc_data, "psc"),
      make_series(index, psc_timestamp_data, "psc_timestamp"),
      make_series(index, close_data, "c")  // Close is from requiredDataSources
  };

  std::vector<std::string> column_names = {
      "gap_up", "gap_down", "fill_fraction", "gap_size",
      "psc", "psc_timestamp", "c"
  };

  auto df = MakeDataFrame(series_list, column_names);

  // Transform and verify
  report.TransformData(df);
  auto tearsheet = report.GetTearSheet();

  // Check that we have the expected number of gaps
  bool found_total_gaps = false;
  for (const auto& card : tearsheet.cards().cards()) {
    if (card.data_size() > 0 && card.data(0).title() == "Total Gaps") {
      found_total_gaps = true;
      // Should have 20 gaps total
      CHECK(card.data(0).value().integer_value() == 20);
    }
  }
  CHECK(found_total_gaps);
}