//
// Created by adesola on 1/8/25.
//

#pragma once
#include <epoch_core/enum_wrapper.h>
#include <epoch_frame/common.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/series.h>
#include <epoch_protos/chart_def.pb.h>
#include <epoch_protos/common.pb.h>
#include <epoch_protos/table_def.pb.h>
#include <vector>

CREATE_ENUM(TurnoverDenominator, AGB, PortfolioValue);

namespace epoch_folio {
using ColumnDefs = std::vector<epoch_proto::ColumnDef>;
struct MaxDrawDownUnderwater {
  epoch_frame::Scalar peak{};
  epoch_frame::Scalar valley{};
  epoch_frame::Scalar recovery{};
};

using MaxDrawDownUnderwaterList = std::vector<MaxDrawDownUnderwater>;

struct DrawDownTableRow {
  int64_t index;
  epoch_frame::Date peakDate{};
  epoch_frame::Date valleyDate{};
  std::optional<epoch_frame::Date> recoveryDate;
  epoch_frame::Scalar netDrawdown{};
  epoch_frame::Scalar duration;
};
using DrawDownTable = std::vector<DrawDownTableRow>;

const epoch_frame::Series EMPTY_SERIES{};
const epoch_frame::DataFrame EMPTY_DATAFRAME{};

struct InterestingDateRange {
  std::string name;
  epoch_frame::Date start;
  epoch_frame::Date end;
};
using InterestingDateRanges = std::vector<InterestingDateRange>;
using InterestingDateRangeReturns =
    std::vector<std::pair<std::string, epoch_frame::Series>>;

struct StrategyWithBenchmarkSeries {
  epoch_frame::Series strategy{EMPTY_SERIES}, benchmark{EMPTY_SERIES};
};

struct SeriesWithAverage {
  epoch_frame::Series series{EMPTY_SERIES};
  epoch_frame::Scalar average{0};

  static SeriesWithAverage Make(epoch_frame::Series _series) {
    return {std::move(_series), _series.mean()};
  }
};

struct StrategyBenchmarkPairedWithAverage {
  SeriesWithAverage strategy;
  epoch_frame::Series benchmark{EMPTY_SERIES};
};
struct StressEvent {
  std::string event;
  epoch_frame::Scalar mean{}, min{}, max{};
};
struct StressEventWithBenchmark {
  StressEvent strategy, benchmark;
};

using StressEvents = std::vector<StressEventWithBenchmark>;

struct StressEventSeries {
  std::string event;
  epoch_frame::Series strategy{EMPTY_SERIES}, benchmark{EMPTY_SERIES};
};
using StressEventSeriesList = std::vector<StressEventSeries>;

struct Allocation {
  std::string asset;
  double allocation{};
};
using TopAllocations = std::vector<Allocation>;

struct AllocationSummary {
  epoch_frame::Series maxLong{EMPTY_SERIES}, medianLong{EMPTY_SERIES},
      medianShort{EMPTY_SERIES}, maxShort{EMPTY_SERIES};
};

struct SectorAllocation {
  std::string sector;
  epoch_frame::Series values{EMPTY_SERIES};
};
using SectorAllocations = std::vector<SectorAllocation>;

epoch_frame::DataFrame
MakeDataFrame(std::vector<epoch_frame::Series> const &series,
              std::vector<std::string> const &columns);

using SectorMapping = std::unordered_map<std::string, std::string>;

struct TearSheetDataOption {
  epoch_frame::Series equity;
  epoch_frame::Series benchmark;
  epoch_frame::Series cash;
  epoch_frame::DataFrame positions;
  epoch_frame::DataFrame transactions;
  epoch_frame::DataFrame roundTrip;
  SectorMapping sectorMapping;
  bool isEquity{true};
};

struct TearSheetOption {
  epoch_core::TurnoverDenominator turnoverDenominator =
      epoch_core::TurnoverDenominator::AGB;
  uint8_t topKPositions = 10;
  std::vector<uint8_t> rollingBetaPeriodsInMonths{6, 12};
  uint8_t rollingVolatilityPeriodInMonths{6};
  uint8_t rollingSharpePeriodInMonths{6};
  uint8_t topKDrawDowns{5};
  int bootstrapKSamples{1000};
  std::optional<InterestingDateRanges> interestingDateRanges{std::nullopt};
  size_t transactionBinMinutes{5};
  std::string transactionTimezone{"America/New_York"};
};

struct TearSheet {
  std::vector<epoch_proto::CardDef> cards{};
  std::vector<epoch_proto::Chart> charts;
  std::vector<epoch_proto::Table> tables{};
};

struct FullTearSheet {
  TearSheet strategy_benchmark;
  TearSheet risk_analysis;
  TearSheet returns_distribution;
  TearSheet positions;
  TearSheet transactions;
  TearSheet round_trip;
};

using BoxPlotOutliers = std::vector<epoch_proto::BoxPlotOutlier>;
std::pair<epoch_proto::BoxPlotDataPoint, BoxPlotOutliers>
MakeBoxPlotDataPoint(int64_t category_index, const epoch_frame::Series &x);
} // namespace epoch_folio