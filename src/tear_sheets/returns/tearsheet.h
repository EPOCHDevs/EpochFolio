//
// Created by adesola on 1/13/25.
//

#pragma once
#include "common/chart_def.h"
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"

namespace epoch_folio::returns {
class TearSheetFactory {
public:
  TearSheetFactory(epoch_frame::DataFrame positions,
                   epoch_frame::DataFrame transactions,
                   epoch_frame::Series cash, epoch_frame::Series strategy,
                   epoch_frame::Series benchmark);

  void Make(epoch_core::TurnoverDenominator turnoverDenominator,
            int64_t topKDrawDowns, FullTearSheet &output) const;

  epoch_frame::DataFrame GetStrategyAndBenchmark() const;

protected:
  TearSheetFactory() = default;

  void SetStrategyReturns(epoch_frame::Series const &strategyReturns);

  void SetBenchmark(epoch_frame::Series const &benchmarkReturns);

  void SetCash(epoch_frame::Series const &cash) { m_cash = cash; }

  void SetPositions(epoch_frame::DataFrame positions) {
    m_positions = std::move(positions);
  }

  void SetTransactions(epoch_frame::DataFrame transactions) {
    m_transactions = std::move(transactions);
  }

  std::vector<epoch_proto::Chart> MakeStrategyBenchmarkLineCharts() const;

  epoch_proto::CardDef
  MakePerformanceStats(epoch_core::TurnoverDenominator turnoverDenominator =
                           epoch_core::TurnoverDenominator::AGB) const;

  epoch_proto::Table MakeStressEventTable() const;

  epoch_proto::Table MakeWorstDrawdownTable(int64_t top,
                                            DrawDownTable &data) const;

  TearSheet MakeStrategyBenchmark(
      epoch_core::TurnoverDenominator turnoverDenominator) const;

  TearSheet MakeRiskAnalysis(int64_t topKDrawDowns) const;

  TearSheet MakeReturnsDistribution() const;

private:
  epoch_frame::Series m_cash;
  epoch_frame::DataFrame m_positions;
  epoch_frame::DataFrame m_transactions;

  epoch_frame::Series m_strategy;
  epoch_frame::Series m_benchmark;

  epoch_frame::Series m_strategyCumReturns;
  epoch_frame::Series m_benchmarkCumReturns;

  InterestingDateRangeReturns m_strategyReturnsInteresting;
  InterestingDateRangeReturns m_benchmarkReturnsInteresting;

  void AlignReturnsAndBenchmark(epoch_frame::Series const &returns,
                                epoch_frame::Series const &benchmark);

  std::vector<epoch_proto::Chart>
  MakeReturnsLineCharts(const epoch_frame::DataFrame &df) const;

  void MakeRollingBetaCharts(std::vector<epoch_proto::Chart> &lines) const;
  void MakeRollingSharpeCharts(std::vector<epoch_proto::Chart> &lines) const;
  void
  MakeRollingVolatilityCharts(std::vector<epoch_proto::Chart> &lines) const;
  void MakeRollingMaxDrawdownCharts(std::vector<epoch_proto::Chart> &lines,
                                    DrawDownTable &drawDownTable,
                                    int64_t topKDrawDowns) const;
  void MakeUnderwaterCharts(std::vector<epoch_proto::Chart> &lines) const;
  void MakeInterestingDateRangeLineCharts(
      std::vector<epoch_proto::Chart> &lines) const;

  epoch_proto::HeatMapDef BuildMonthlyReturnsHeatMap() const;
  epoch_proto::BarDef BuildAnnualReturnsBar() const;
  epoch_proto::HistogramDef BuildMonthlyReturnsHistogram() const;
  epoch_proto::BoxPlotDef BuildReturnQuantiles() const;
};
} // namespace epoch_folio::returns