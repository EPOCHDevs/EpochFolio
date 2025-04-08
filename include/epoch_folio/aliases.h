//
// Created by adesola on 3/27/25.
//

#pragma once
#include <epoch_frame/series.h>


namespace epoch_folio {
    using ReturnsStat = std::function<double(epoch_frame::Series const &)>;
    using FactorReturnsStat = std::function<double(epoch_frame::DataFrame const &)>;
    using RollingReturnsStatT = std::function<double(epoch_frame::Series const &, int64_t)>;
}