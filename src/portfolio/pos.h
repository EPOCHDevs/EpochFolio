//
// Created by adesola on 1/9/25.
//

#pragma once
#include <epoch_frame/series.h>
#include <epoch_frame/dataframe.h>

namespace epoch_folio {
    inline epoch_frame::DataFrame GetPercentAlloc(const epoch_frame::DataFrame &values) {
        return values / values.sum(epoch_frame::AxisType::Column);
    }

    std::array<epoch_frame::Series, 3>
    GetTopLongShortAbs(const epoch_frame::DataFrame &positions, int top = 10);

    epoch_frame::DataFrame GetMaxMedianPositionConcentration(const epoch_frame::DataFrame &positions);

    epoch_frame::DataFrame GetSectorExposure(const epoch_frame::DataFrame &positions,
                                     const std::unordered_map<std::string, std::string> &sectorMapping);
}