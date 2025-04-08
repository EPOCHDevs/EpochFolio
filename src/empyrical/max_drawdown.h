//
// Created by adesola on 1/6/25.
//

#pragma once
#include "ireturn_stat.h"
#include "periods.h"
#include "stats.h"


namespace epoch_folio::ep {
    struct MaxDrawDown {
        double operator()(epoch_frame::Series const &returns) const {
            if (returns.empty()) {
                return NAN_SCALAR;
            }
            return DrawDownSeries(returns).min().as_double();
        }
    };

    using RollMaxDrawDown = RollingReturnsStat<MaxDrawDown>;
}