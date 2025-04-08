//
// Created by adesola on 1/14/25.
//

#pragma once

#include <epoch_core/enum_wrapper.h>
#include <string>
#include <vector>
#include <glaze/glaze.hpp>


CREATE_ENUM(EpochFolioDashboardWidget,
    Card, DataTable,
        Lines,
        Area,
        HeatMap,
        Bar,
        Column,
        Histogram,
        BoxPlot,
        XRange,
        Pie);

CREATE_ENUM(EpochFolioType, Percent, Decimal, Integer, Date, DateTime, Monetary, String, Duration, DayDuration);
CREATE_ENUM(EpochFolioCategory, StrategyBenchmark, RiskAnalysis, ReturnsDistribution, Positions, Transactions, RoundTrip);

namespace epoch_metadata::epoch_folio {
    struct CategoryMetaData {
        epoch_core::EpochFolioCategory value;
        std::string label;
        std::string description;
    };

    std::vector<CategoryMetaData> GetCategoryMetaData();
}