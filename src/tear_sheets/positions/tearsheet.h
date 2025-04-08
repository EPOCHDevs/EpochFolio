//
// Created by adesola on 1/13/25.
//

#pragma once
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"

namespace epoch_folio::positions {
    class TearSheetFactory {
    public:
        TearSheetFactory(epoch_frame::Series cash,
                         epoch_frame::DataFrame positions,
                         epoch_frame::Series returns,
                         std::unordered_map<std::string, std::string> sectorMappings);

        void Make(uint32_t k, FullTearSheet&) const;

    protected:
        TearSheetFactory() = default;

        void SetStrategyReturns(epoch_frame::Series const &strategyReturns) {
            m_strategy = strategyReturns;
        }

        void SetCash(epoch_frame::Series const &cash) {
            m_cash = cash;
        }

        void SetPositions(epoch_frame::DataFrame positions) {
            m_positionsNoCash = std::move(positions);
        }

        std::vector<Chart> MakeTopPositionsLineCharts(epoch_frame::DataFrame const &positions,
            epoch_frame::DataFrame const& topPositionAllocations) const;

    private:
        epoch_frame::Series m_cash;
        epoch_frame::DataFrame m_positionsNoCash;
        epoch_frame::Series m_strategy;
        std::unordered_map<std::string, std::string> m_sectorMappings;

        LinesDef MakeExposureOverTimeChart(epoch_frame::DataFrame const &positions, epoch_frame::DataFrame const &isLong, epoch_frame::DataFrame const &isShort) const;
        LinesDef MakeAllocationOverTimeChart(epoch_frame::DataFrame const & topPositionAllocations) const;
        LinesDef MakeAllocationSummaryChart(epoch_frame::DataFrame const &positions) const;
        LinesDef MakeTotalHoldingsChart(epoch_frame::DataFrame const &positions) const;
        LinesDef MakeLongShortHoldingsChart(epoch_frame::DataFrame const &isLong, epoch_frame::DataFrame const &isShort) const;
        LinesDef MakeGrossLeverageChart() const;
        LinesDef MakeSectorExposureChart() const;
    };
}