//
// Created by adesola on 1/8/25.
//

#pragma once
#include "empyrical/stats.h"
#include "interesting_periods.h"
#include <epoch_frame/scalar.h>

namespace epoch_folio {

    /*
    Normalizes a returns timeseries based on the first value.

    Parameters
    ----------
    returns : pd.Series
        Daily returns of the strategy, noncumulative.
         - See full explanation in tears.create_full_tear_sheet.
    starting_value : float, optional
       The starting returns (default 1).

    Returns
    -------
    pd.Series
        Normalized returns.
    */
    inline epoch_frame::Series Normalize(epoch_frame::Series const &returns, double startingValue = 1) {
        return epoch_frame::Scalar{std::move(startingValue)} * (returns / returns.iloc(0));
    }

    epoch_frame::Series RollingBeta(epoch_frame::DataFrame const &df,
                           int64_t rollingWindow = ep::APPROX_BDAYS_PER_MONTH * 6);

    epoch_frame::Series GrossLeverage(epoch_frame::DataFrame const &);

    inline double ValueAtRisk(epoch_frame::Series const &returns, double sigma = 2.0) {
        return (returns.mean() - epoch_frame::Scalar{std::move(sigma)} * returns.stddev(arrow::compute::VarianceOptions{1})).as_double();
    }

    inline double ValueAtRisk(epoch_frame::Series const &returns, epoch_core::EmpyricalPeriods period, double sigma = 2.0) {
        return ValueAtRisk(ep::AggregateReturns(returns, period), sigma);
    }

    epoch_frame::Series GetUnderwaterFromCumReturns(epoch_frame::Series const &dfCum);

    epoch_frame::Series GetUnderwater(epoch_frame::Series const &returns);

    MaxDrawDownUnderwater GetMaxDrawDownUnderWater(epoch_frame::Series const &underwater);

    MaxDrawDownUnderwater GetMaxDrawDown(epoch_frame::Series const &returns);

    MaxDrawDownUnderwaterList GetTopDrawDownsFromReturns(epoch_frame::Series const &ret, int top = 10);
    MaxDrawDownUnderwaterList GetTopDrawDownsFromCumReturns(epoch_frame::Series const &dfCum, int top = 10);

    DrawDownTable GenerateDrawDownTable(epoch_frame::Series const &returns, int64_t top);

    epoch_frame::Series RollingVolatility(epoch_frame::Series const &returns, int64_t rollingVolWindow);

    epoch_frame::Series RollingSharpe(epoch_frame::Series const &returns, int64_t rollingSharpeWindow);

    InterestingDateRangeReturns ExtractInterestingDateRanges(epoch_frame::Series const &, InterestingDateRanges const &);

    inline InterestingDateRangeReturns ExtractInterestingDateRanges(epoch_frame::Series const &returns) {
        return ExtractInterestingDateRanges(returns, PERIODS);
    };
}
