//
// Created by adesola on 3/31/25.
//

#pragma once
#include "model.h"


namespace epoch_folio {
    std::vector<Table> GetRoundTripStats(epoch_frame::DataFrame const &round_trip);

    epoch_frame::DataFrame GetProfitAttribution(epoch_frame::DataFrame const &round_trip, std::string const &col="symbol");
}
