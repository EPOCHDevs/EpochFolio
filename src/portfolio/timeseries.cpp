//
// Created by adesola on 1/8/25.
//

#include "timeseries.h"

#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <oneapi/tbb/parallel_for.h>

#include "txn.h"
#include "interesting_periods.h"
#include "empyrical/alpha_beta.h"


using namespace epoch_frame;

namespace epoch_folio {
    Series RollingBeta(DataFrame const& df, int64_t rollingWindow) {
        auto index_array = df.index()->array();
        auto beg_array = index_array[{0, -rollingWindow}];
        auto end_array = index_array[{rollingWindow, std::nullopt}];

        std::vector result(index_array.length(), ep::NAN_SCALAR);

        const auto length = std::min(end_array.length(), beg_array.length());
        parallel_for(tbb::blocked_range<int64_t>(0, length), [&](tbb::blocked_range<int64_t> const &r) {
            for (int64_t i = r.begin(); i < r.end(); ++i) {
                auto iIndex = rollingWindow+i;
                auto beg_scalar = beg_array[i];
                auto end_scalar = end_array[i];
                auto chunk = df.loc({beg_scalar, end_scalar});
                result[iIndex] = ep::Beta{}(chunk);
            }
        });

        return make_series(df.index(), result);
    }

    Series GrossLeverage(DataFrame const &positions) {
        return ABG(positions) / positions.sum(AxisType::Column);
    }

    MaxDrawDownUnderwater GetMaxDrawDownUnderWater(epoch_frame::Series const &underwater) {
        auto valley = underwater.idx_min();

        Series underwater_slice_to_valley = underwater.loc({Scalar{}, valley});
        Series underwater_slice_from_valley = underwater.loc({valley, Scalar{}});

        // find first 0
        auto peak = underwater_slice_to_valley.loc(underwater_slice_to_valley == Scalar{0}).index()->at(-1);

        auto recovery_index = underwater_slice_from_valley.loc(underwater_slice_from_valley == Scalar{0}).index();
        Scalar recovery;
        if (!recovery_index->empty()) {
            recovery = recovery_index->at(0);
        }

        return { .peak = peak, .valley = valley, .recovery = recovery};
    }

    Series GetUnderwaterFromCumReturns(Series const &dfCum) {
        auto running_max = dfCum.cumulative_max(true);
        return dfCum / running_max - 1.0_scalar;
    }

    Series GetUnderwater(Series const &returns) {
        return GetUnderwaterFromCumReturns(ep::CumReturns(returns, 1.0));
    }

    MaxDrawDownUnderwater GetMaxDrawDown(Series const &returns) {
        return GetMaxDrawDownUnderWater(GetUnderwater(returns));
    }

    MaxDrawDownUnderwaterList GetTopDrawDownsFromReturns(epoch_frame::Series const &ret, int top) {
        return GetTopDrawDownsFromCumReturns(ep::CumReturns(ret, 1.0), top);
    }
    MaxDrawDownUnderwaterList GetTopDrawDownsFromCumReturns(epoch_frame::Series const &dfCum, int top) {
        Series underWater = GetUnderwaterFromCumReturns(dfCum);
        MaxDrawDownUnderwaterList drawDowns;

        while (top-- > 0) {
            auto [peak, valley, recovery] = GetMaxDrawDownUnderWater(underWater);

            if (recovery.is_valid()) {
                underWater = underWater.drop(underWater.loc({peak, recovery}).index()->iloc({1, -1}));
            } else {
                underWater = underWater.loc({Scalar{}, peak});
            }

            drawDowns.emplace_back(peak, valley, recovery);
            if (dfCum.empty() || underWater.empty() or underWater.min().as_double() == 0) {
                break;
            }
        }

        return drawDowns;
    }


    DrawDownTable GenerateDrawDownTable(epoch_frame::Series const &returns, int64_t top) {
        auto dfCum = ep::CumReturns(returns, 1.0);
        auto drawDownPeriods = GetTopDrawDownsFromCumReturns(dfCum, top);

        DrawDownTable table;
        table.reserve(top);

        for (auto const &[i, underwaterRow]: std::views::enumerate(drawDownPeriods)) {
            auto [peak, valley, recovery] = underwaterRow;
            DrawDownTableRow row{
                .index = i,
                .peakDate = peak.to_date().date(),
                .valleyDate = valley.to_date().date(),
                .recoveryDate = std::nullopt,
                .netDrawdown = Scalar{},
                .duration = Scalar{MakeNullScalar(arrow::uint64())}
            };
            if (recovery.is_valid()) {
                row.duration = Scalar{factory::index::date_range({.start=peak.timestamp(), .end=recovery.timestamp(), .offset=factory::offset::bday(1)})->size()};
                row.recoveryDate = recovery.to_date().date();
            }

            row.netDrawdown = ((dfCum.loc(peak) - dfCum.loc(valley)) / dfCum.loc(peak)) * 100.0_scalar;

            table.emplace_back(row);
        }
        return table;
    }

    Series RollingVolatility(epoch_frame::Series const &returns, int64_t rollingVolWindow) {
        static const Scalar multiplier{std::sqrt(ep::APPROX_BDAYS_PER_YEAR)};
        return returns.rolling_agg(window::RollingWindowOptions{ .window_size = rollingVolWindow }).stddev() * multiplier;
    }

    Series RollingSharpe(epoch_frame::Series const &returns, int64_t rollingVolWindow) {
        static const Scalar multiplier{std::sqrt(ep::APPROX_BDAYS_PER_YEAR)};
        auto agg = returns.rolling_agg(window::RollingWindowOptions{ .window_size = rollingVolWindow });
        return (agg.mean() / agg.stddev()) * multiplier;
    }

    InterestingDateRangeReturns ExtractInterestingDateRanges(epoch_frame::Series const &returns,
                                                             InterestingDateRanges const &periods) {

        InterestingDateRangeReturns ranges;
        for (auto const &[name, start, end]: periods) {
            try {
                Series period = returns.loc({Scalar{DateTime{start, {.tz="UTC"}}}, Scalar{DateTime{end, {.tz="UTC"}}}});
                if (period.empty()) {
                    continue;
                }
                ranges.emplace_back(name, period);
            } catch (...) {}
        }
        return ranges;
    }
}