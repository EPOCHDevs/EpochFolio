//
// Created by adesola on 3/30/25.
//

#pragma once
#include "epoch_frame/dataframe.h"
#include "portfolio/model.h"


namespace epoch_folio::round_trip {
    class TearSheetFactory {
        public:
            TearSheetFactory(epoch_frame::DataFrame round_trip, epoch_frame::Series returns, epoch_frame::DataFrame positions, SectorMapping sector_mapping);

        void Make(FullTearSheet& output) const;
    private:
        epoch_frame::DataFrame m_round_trip;
        epoch_frame::Series m_returns;
        epoch_frame::DataFrame m_positions;
        SectorMapping m_sector_mapping;

        epoch_frame::DataFrame ExtractRoundTrips() const;

        XRangeDef MakeXRangeDef(epoch_frame::DataFrame const &trades) const;

        LinesDef MakeProbProfitChart(epoch_frame::DataFrame const &trades) const;

        HistogramDef MakeHoldingTimeChart(epoch_frame::DataFrame const &trades) const;

        HistogramDef MakePnlPerRoundTripDollarsChart(epoch_frame::DataFrame const &trades) const;

        HistogramDef MakeReturnsPerRoundTripDollarsChart(epoch_frame::DataFrame const &trades) const;

        PieDef MakeProfitabilityPieChart(epoch_frame::DataFrame const &trades) const;
    };
}