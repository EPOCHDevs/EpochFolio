#pragma once
#include "ireport.h"
#include <glaze/glaze.hpp>
#include <optional>

namespace epoch_folio {

struct GapReportOptions {
  // Time filtering
  std::optional<int64_t> start_timestamp_ns{};
  std::optional<int64_t> end_timestamp_ns{};
  std::optional<int> last_n_gaps{}; // Show last N gaps instead of date range

  // Gap filters
  bool include_gap_up{true};
  bool include_gap_down{true};
  double min_gap_pct{0.0};
  double max_gap_pct{100.0};
  bool only_filled{false};
  bool only_unfilled{false};

  // Analysis toggles
  bool show_fill_analysis{true};
  bool show_day_of_week_analysis{true};
  bool show_fill_time_analysis{true};
  bool show_performance_analysis{true};
  bool show_streak_analysis{true};
  bool show_distribution_histogram{true};

  // Display options
  uint32_t histogram_bins{20};
  uint32_t max_table_rows{100};
  uint32_t max_streaks{5}; // Last N streaks to show

  // Aggregation
  bool per_symbol{false}; // If true, generate per-symbol analysis
};

class GapReport : public IReport {
public:
  const ReportMetadata &metadata() const override;

  TearSheet generate(const epoch_frame::DataFrame &df,
                     const glz::json_t &optionsJson) const override;

  std::unordered_map<std::string, TearSheet> generate_per_asset(
      const std::unordered_map<std::string, epoch_frame::DataFrame> &assetToDf,
      const glz::json_t &optionsJson) const override;

private:
  TearSheet generate_impl(const epoch_frame::DataFrame &df,
                          const GapReportOptions &options) const;

  // Analysis helpers
  std::vector<CardDef>
  compute_summary_cards(const epoch_frame::DataFrame &gaps) const;

  BarDef create_fill_rate_chart(const epoch_frame::DataFrame &gaps,
                                const std::string &title) const;

  Table create_frequency_table(const epoch_frame::DataFrame &gaps,
                               const std::string &category_col,
                               const std::string &title) const;

  XRangeDef create_streak_chart(const epoch_frame::DataFrame &gaps,
                                uint32_t max_streaks) const;

  HistogramDef create_gap_distribution(const epoch_frame::DataFrame &gaps,
                                       uint32_t bins) const;

  PieDef create_time_distribution(const epoch_frame::DataFrame &gaps) const;

  Table create_gap_details_table(const epoch_frame::DataFrame &gaps,
                                 uint32_t limit) const;

  LinesDef create_gap_trend_chart(const epoch_frame::DataFrame &gaps) const;

  // Utility
  epoch_frame::DataFrame filter_gaps(const epoch_frame::DataFrame &df,
                                     const GapReportOptions &options) const;

public:
  static ReportMetadata s_metadata;

  // Explicit registration function
  static void register_report();
};

} // namespace epoch_folio
