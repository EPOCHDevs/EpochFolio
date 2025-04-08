//
// Created by adesola on 1/8/25.
//

#pragma once
#include "model.h"


namespace epoch_folio {
    inline epoch_frame::Series ABG(epoch_frame::DataFrame const& positions)
    {
        return positions.drop("cash").abs().sum(epoch_frame::AxisType::Column);
    }
    epoch_frame::DataFrame GetTransactionVolume(epoch_frame::DataFrame const &);

    epoch_frame::Series GetTurnover(epoch_frame::DataFrame const &positions,
                           epoch_frame::DataFrame const &transactions,
                           epoch_core::TurnoverDenominator turnoverDenominator=epoch_core::TurnoverDenominator::AGB);
}