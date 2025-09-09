#pragma once
#include "common/chart_def.h"
#include "ireport.h"
#include <glaze/glaze.hpp>

namespace epoch_folio {

class GapReport : public IReport {
public:
  explicit GapReport(
      const epoch_metadata::transform::TransformConfiguration *config)
      : IReport(config) {}
  const ReportMetadata &metadata() const override;

  epoch_proto::TearSheet
  generate(const epoch_frame::DataFrame &df) const override;

private:
  epoch_proto::TearSheet generate_impl(const epoch_frame::DataFrame &df) const;

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
  epoch_frame::DataFrame filter_gaps(const epoch_frame::DataFrame &df) const;

public:
  static ReportMetadata s_metadata;

  // Explicit registration function
  static void register_report();
};

} // namespace epoch_folio
