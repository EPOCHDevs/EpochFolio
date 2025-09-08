#include <arrow/builder.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_protos/table_def.pb.h>
#include <glaze/glaze.hpp>

#include "portfolio/model.h" // For MakeDataFrame
#include "reports/ireport.h"

using namespace epoch_folio;
using namespace epoch_frame;

TEST_CASE("GapReport registration and metadata", "[reports][gap]") {
  auto &registry = ReportRegistry::instance();
  auto reports = registry.list_reports();

  auto gap_report_it = std::find_if(
      reports.begin(), reports.end(),
      [](const ReportMetadata &meta) { return meta.id == "gap_report"; });

  REQUIRE(gap_report_it != reports.end());
  CHECK(gap_report_it->displayName == "Price Gap Analysis");
  CHECK(gap_report_it->category ==
        epoch_proto::EPOCH_FOLIO_CATEGORY_RISK_ANALYSIS);
  CHECK(gap_report_it->requiredColumns.size() == 13);
}

TEST_CASE("GapReport basic generation", "[reports][gap]") {
  auto &registry = ReportRegistry::instance();
  auto report = registry.create("gap_report", nullptr);
  REQUIRE(report != nullptr);

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

  // Create gap data vectors
  std::vector<bool> gap_up_data, gap_down_data, gap_up_filled_data,
      gap_down_filled_data;
  std::vector<double> gap_up_size_data, gap_down_size_data,
      gap_up_fraction_data, gap_down_fraction_data;
  std::vector<double> o_data, h_data, l_data, c_data;
  std::vector<int64_t> v_data;

  for (int i = 0; i < 10; ++i) {
    // Alternate between gap up and gap down
    bool is_gap_up = (i % 2 == 0);
    gap_up_data.push_back(is_gap_up);
    gap_down_data.push_back(!is_gap_up);

    // Fill status
    bool is_filled = (i % 3 != 0);
    gap_up_filled_data.push_back(is_gap_up && is_filled);
    gap_down_filled_data.push_back(!is_gap_up && is_filled);

    // Gap sizes
    gap_up_size_data.push_back(is_gap_up ? 2.5 + i * 0.1 : 0.0);
    gap_down_size_data.push_back(!is_gap_up ? 2.5 + i * 0.1 : 0.0);

    // Gap fractions (as decimal, will be converted to %)
    gap_up_fraction_data.push_back(is_gap_up ? 0.005 + i * 0.0002 : 0.0);
    gap_down_fraction_data.push_back(!is_gap_up ? 0.005 + i * 0.0002 : 0.0);

    // OHLCV data
    double base_price = 400.0 + i;
    o_data.push_back(base_price);
    h_data.push_back(base_price + 2.0);
    l_data.push_back(base_price - 1.0);
    c_data.push_back(base_price + (i % 2 == 0 ? 1.0 : -0.5));
    v_data.push_back(1000000 + i * 10000);
  }

  // Create Series
  std::vector<Series> series_list = {
      make_series(index, gap_up_data, "gap_up"),
      make_series(index, gap_down_data, "gap_down"),
      make_series(index, gap_up_filled_data, "gap_up_filled"),
      make_series(index, gap_down_filled_data, "gap_down_filled"),
      make_series(index, gap_up_size_data, "gap_up_size"),
      make_series(index, gap_down_size_data, "gap_down_size"),
      make_series(index, gap_up_fraction_data, "gap_up_fraction"),
      make_series(index, gap_down_fraction_data, "gap_down_fraction"),
      make_series(index, o_data, "o"),
      make_series(index, h_data, "h"),
      make_series(index, l_data, "l"),
      make_series(index, c_data, "c"),
      make_series(index, v_data, "v")};

  std::vector<std::string> column_names = {"gap_up",
                                           "gap_down",
                                           "gap_up_filled",
                                           "gap_down_filled",
                                           "gap_up_size",
                                           "gap_down_size",
                                           "gap_up_fraction",
                                           "gap_down_fraction",
                                           "o",
                                           "h",
                                           "l",
                                           "c",
                                           "v"};

  auto df = MakeDataFrame(series_list, column_names);

  // Test with default options
  // Use default options from empty configuration
  auto tearsheet = report->generate(df);

  // Verify output structure
  CHECK(!tearsheet.cards.empty());
  CHECK(!tearsheet.charts.empty());
  CHECK(!tearsheet.tables.empty());

  // Check specific outputs
  bool has_fill_rate_chart = false;
  bool has_histogram = false;
  bool has_pie_chart = false;

  for (const auto &chart : tearsheet.charts) {
    std::visit(
        [&](const auto &c) {
          using T = std::decay_t<decltype(c)>;
          if constexpr (std::is_same_v<T, epoch_proto::BarDef>) {
            if (c.chartDef.title == "Gap Fill Analysis") {
              has_fill_rate_chart = true;
            }
          } else if constexpr (std::is_same_v<T, epoch_proto::HistogramDef>) {
            has_histogram = true;
          } else if constexpr (std::is_same_v<T, epoch_proto::PieDef>) {
            has_pie_chart = true;
          }
        },
        chart);
  }

  CHECK(has_fill_rate_chart);
  CHECK(has_histogram);
  CHECK(has_pie_chart);

  // Check cards
  auto total_gaps_card = std::find_if(
      tearsheet.cards.begin(), tearsheet.cards.end(),
      [](const epoch_proto::CardDef &card) {
        return !card.data.empty() && card.data[0].title == "Total Gaps";
      });
  REQUIRE(total_gaps_card != tearsheet.cards.end());
  CHECK(total_gaps_card->data[0].value.cast_int64().as_int64() == 10);
}

TEST_CASE("GapReport per-asset generation", "[reports][gap]") {
  auto &registry = ReportRegistry::instance();
  auto report = registry.create("gap_report", nullptr);
  REQUIRE(report != nullptr);

  // Create sample data for two assets using DataFrame factory
  using namespace epoch_frame;
  using namespace epoch_frame::factory;
  using namespace epoch_frame::factory::index;

  std::unordered_map<std::string, DataFrame> asset_data;

  for (const auto &symbol : {"SPY", "QQQ"}) {
    // Create date index (5 days starting from 2022-01-01)
    std::vector<int64_t> dates;
    for (int i = 0; i < 5; ++i) {
      dates.push_back(1640995200000000000LL + i * 86400000000000LL);
    }
    auto index = make_datetime_index(dates);

    // Create gap data vectors - all gap up for simplicity
    std::vector<bool> gap_up_data(5, true);
    std::vector<bool> gap_down_data(5, false);
    std::vector<bool> gap_up_filled_data(5, true);
    std::vector<bool> gap_down_filled_data(5, false);

    std::vector<double> gap_up_size_data, gap_down_size_data;
    std::vector<double> gap_up_fraction_data, gap_down_fraction_data;
    std::vector<double> o_data, h_data, l_data, c_data;
    std::vector<int64_t> v_data;

    for (int i = 0; i < 5; ++i) {
      gap_up_size_data.push_back(1.0 + i * 0.1);
      gap_down_size_data.push_back(0.0);
      gap_up_fraction_data.push_back(0.003 + i * 0.0001);
      gap_down_fraction_data.push_back(0.0);

      double base_price = 400.0 + i;
      o_data.push_back(base_price);
      h_data.push_back(base_price + 2.0);
      l_data.push_back(base_price - 1.0);
      c_data.push_back(base_price + 1.0);
      v_data.push_back(1000000);
    }

    // Create Series
    std::vector<Series> series_list = {
        make_series(index, gap_up_data, "gap_up"),
        make_series(index, gap_down_data, "gap_down"),
        make_series(index, gap_up_filled_data, "gap_up_filled"),
        make_series(index, gap_down_filled_data, "gap_down_filled"),
        make_series(index, gap_up_size_data, "gap_up_size"),
        make_series(index, gap_down_size_data, "gap_down_size"),
        make_series(index, gap_up_fraction_data, "gap_up_fraction"),
        make_series(index, gap_down_fraction_data, "gap_down_fraction"),
        make_series(index, o_data, "o"),
        make_series(index, h_data, "h"),
        make_series(index, l_data, "l"),
        make_series(index, c_data, "c"),
        make_series(index, v_data, "v")};

    std::vector<std::string> column_names = {"gap_up",
                                             "gap_down",
                                             "gap_up_filled",
                                             "gap_down_filled",
                                             "gap_up_size",
                                             "gap_down_size",
                                             "gap_up_fraction",
                                             "gap_down_fraction",
                                             "o",
                                             "h",
                                             "l",
                                             "c",
                                             "v"};

    asset_data[symbol] = MakeDataFrame(series_list, column_names);
  }

  // Single interface only: call generate for each asset
  for (const auto &[symbol, df] : asset_data) {
    auto ts = report->generate(df);
    CHECK(!ts.cards.empty());
    CHECK(!ts.charts.empty());
    CHECK(!ts.tables.empty());
  }
}