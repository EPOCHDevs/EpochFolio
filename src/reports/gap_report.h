#pragma once
#include "../../include/epoch_folio/ireport.h"
#include "common/chart_def.h"
#include <glaze/glaze.hpp>

#include <epoch_metadata/bar_attribute.h>

namespace epoch_folio {

class GapReport : public IReporter {
public:
  explicit GapReport(epoch_metadata::transform::TransformConfiguration config)
      : IReporter(std::move(config)) {}

protected:
  // Implementation of IReporter's virtual method
  void
  generateTearsheet(const epoch_frame::DataFrame &normalizedDf) const override;

public:
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
};

// Template specialization for GapReport metadata
template <> struct ReportMetadata<GapReport> {
  constexpr static const char *kReportId = "gap_report";

  static epoch_metadata::transforms::TransformsMetaData Get() {
    return {
        .id = kReportId,
        .category = epoch_core::TransformCategory::Executor,
        .renderKind = epoch_core::TransformNodeRenderKind::Standard,
        .name = "Gap Analysis Report",
        .options =
            {{.id = "show_fill_analysis",
              .name = "Show Fill Analysis",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Display gap fill rate analysis comparing the percentage of gaps that get filled during the trading session. Shows separate fill rates for gap up and gap down scenarios, helping identify which gap types are more likely to close. Essential for gap fade trading strategies.",
              .isRequired = false},
             {.id = "show_day_of_week_analysis",
              .name = "Show Day of Week Analysis",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Generate frequency table showing gap occurrence patterns by day of the week. Identifies if certain weekdays have higher gap probabilities, useful for timing gap trading strategies and understanding weekly market dynamics like Monday gaps or Friday-to-Monday weekend gaps.",
              .isRequired = false},
             {.id = "show_fill_time_analysis",
              .name = "Show Fill Time Analysis",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Analyze gap frequency by time of day, showing when gaps are most likely to occur and get filled. Breaks down gap activity by hourly buckets to identify optimal entry/exit times for gap trading and understand intraday gap behavior patterns.",
              .isRequired = false},
             {.id = "show_performance_analysis",
              .name = "Show Performance Analysis",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Display table comparing gap fill outcomes with closing price performance. Shows whether gaps that fill tend to close green or red relative to prior session close, helping assess the profitability of gap fade versus gap continuation strategies.",
              .isRequired = false},
             {.id = "show_streak_analysis",
              .name = "Show Streak Analysis",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Visualize consecutive gap patterns using XRange chart showing recent gap up and gap down streaks over time. Identifies clustering of similar gap types and helps detect regime changes in market gap behavior, useful for adjusting trading strategies based on current market conditions.",
              .isRequired = false},
             {.id = "show_distribution_histogram",
              .name = "Show Distribution Histogram",
              .type = epoch_core::MetaDataOptionType::Boolean,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{true},
              .desc = "Generate histogram showing the distribution of gap sizes as percentages. Reveals the most common gap magnitudes, identifies outlier gaps, and helps set appropriate thresholds for gap trading strategies based on historical size distributions.",
              .isRequired = false},
             {.id = "histogram_bins",
              .name = "Histogram Bins",
              .type = epoch_core::MetaDataOptionType::Integer,
              .defaultValue = epoch_metadata::MetaDataOptionDefinition{20.0},
              .desc = "Number of bins for the gap size distribution histogram. Lower values provide broader gap size categories while higher values offer more granular distribution analysis. Adjust based on data volume and desired granularity of gap size analysis.",
              .isRequired = false,
              .min = 5,
              .max = 100}},
        .isCrossSectional = false,
        .desc = "Comprehensive gap analysis report that examines price gaps "
                "between trading sessions. Analyzes opening price gaps relative "
                "to prior session close, tracking gap direction (up/down), "
                "size distribution, fill rates, and performance patterns. "
                "Generates visualizations including fill rate charts, streak "
                "analysis, time-of-day distributions, and trend analysis to "
                "identify gap trading opportunities and patterns across different "
                "market conditions and timeframes.",
        .inputs = {{epoch_core::IODataType::Boolean, "gap_up", "Gap Up"},
                   {epoch_core::IODataType::Boolean, "gap_filled",
                    "Gap Filled"},
                   {epoch_core::IODataType::Decimal, "fill_fraction",
                    "Fill Fraction"},
                   {epoch_core::IODataType::Decimal, "gap_size", "Gap Size"},
                   {epoch_core::IODataType::Decimal, "psc",
                    "Prior Session Close"},
                   {epoch_core::IODataType::Integer, "psc_timestamp",
                    "PSC Timestamp"}},
        .outputs = {},
        .tags = {"gap_classify"},
        .requiredDataSources =
            {epoch_metadata::EpochStratifyXConstants::instance().CLOSE()},
        .isReporter = true};
  }

  // Helper to create a TransformConfiguration from a gap classifier config
  static epoch_metadata::transform::TransformConfiguration
  CreateConfig(const std::string &instance_id,
               const epoch_metadata::transform::TransformConfiguration
                   &gap_classifier_config,
               const YAML::Node &options = {}) {

    YAML::Node config;
    config["id"] = instance_id;
    config["type"] = kReportId;
    // Just pass timeframe as string
    config["timeframe"] = "1D"; // Default for now, could get from
                                // gap_classifier_config if method exists

    // Map the gap classifier's outputs to our inputs
    // The gap classifier produces columns that we need
    YAML::Node inputs;
    std::string gap_id = gap_classifier_config.GetId();

    // Map each required input to the gap classifier's output columns
    auto metadata = Get();
    for (const auto &input : metadata.inputs) {
      // Map input name to gap_classifier_id#column_name format
      inputs[input.name].push_back(gap_id + "#" + input.name);
    }

    config["inputs"] = inputs;
    // SessionRange is optional, skip it for now
    config["options"] = options;

    return epoch_metadata::transform::TransformConfiguration{
        epoch_metadata::TransformDefinition{config}};
  }

  // Simpler helper for testing without a preceding node
  static epoch_metadata::transform::TransformConfiguration
  CreateConfig(const std::string &instance_id,
               const std::string &timeframe = "1D",
               const YAML::Node &options = {}) {

    YAML::Node config;
    config["id"] = instance_id;
    config["type"] = kReportId;
    config["timeframe"] = timeframe;
    config["inputs"] = YAML::Node(); // Empty for testing
    config["sessionRange"] = YAML::Node();
    config["options"] = options;

    return epoch_metadata::transform::TransformConfiguration{
        epoch_metadata::TransformDefinition{config}};
  }
};

} // namespace epoch_folio
