//
// Created by adesola on 1/6/25.
//

#pragma once
#include <epoch_core/enum_wrapper.h>


CREATE_ENUM(EmpyricalPeriods, daily, weekly, monthly, quarterly, yearly);

namespace epoch_folio::ep {
    constexpr int APPROX_BDAYS_PER_MONTH = 21;
    constexpr int APPROX_BDAYS_PER_YEAR = 252;

    constexpr int MONTHS_PER_YEAR = 12;
    constexpr int WEEKS_PER_YEAR = 52;
    constexpr int QTRS_PER_YEAR = 4;

    static const std::unordered_map<epoch_core::EmpyricalPeriods, int> ANNUALIZATION_FACTORS{
            {epoch_core::EmpyricalPeriods::daily,     APPROX_BDAYS_PER_YEAR},
            {epoch_core::EmpyricalPeriods::weekly,    WEEKS_PER_YEAR},
            {epoch_core::EmpyricalPeriods::monthly,   MONTHS_PER_YEAR},
            {epoch_core::EmpyricalPeriods::quarterly, QTRS_PER_YEAR},
            {epoch_core::EmpyricalPeriods::yearly,    1},
    };

}