//
// Created by adesola on 1/9/25.
//

#include "pos.h"
#include <spdlog/spdlog.h>
#include <epoch_frame/series.h>
#include <epoch_frame/frame_or_series.h>
#include <epoch_frame/common.h>
#include <epoch_frame/factory/dataframe_factory.h>

namespace epoch_folio {

    std::array<epoch_frame::Series, 3>
    GetTopLongShortAbs(const epoch_frame::DataFrame &positions, int top) {
        using namespace epoch_frame;

        auto pos = positions.drop("cash");

        Series df_max = pos.max(AxisType::Row);
        Series df_min = pos.min(AxisType::Row);
        Series df_abs_max = pos.abs().max(AxisType::Row);

        Series df_top_long = df_max.loc(df_max > Scalar{0}).n_largest(top);
        Series df_top_short = df_min.loc(df_min < Scalar{0}).n_smallest(top);
        Series df_top_abs = df_abs_max.n_largest(top);

        return {df_top_long, df_top_short, df_top_abs};
    }

    epoch_frame::DataFrame GetMaxMedianPositionConcentration(const epoch_frame::DataFrame &positions) {
        using namespace epoch_frame;
        DataFrame expos = GetPercentAlloc(positions).drop("cash");

        DataFrame longs = expos.where(expos > epoch_frame::Scalar{0}, Scalar{});
        DataFrame shorts = expos.where(expos < epoch_frame::Scalar{0}, Scalar{});

        arrow::ChunkedArrayVector alloc_vectors(4);

        // approx median does not work as expected
        arrow::compute::QuantileOptions options{0.5};
        options.interpolation = arrow::compute::QuantileOptions::LINEAR;

        alloc_vectors[0] = longs.max(AxisType::Column).array();
        alloc_vectors[1] = longs.quantile(options, AxisType::Column).array();
        alloc_vectors[2] = shorts.quantile(options, AxisType::Column).array();
        alloc_vectors[3] = shorts.min(AxisType::Column).array();

        return make_dataframe(positions.index(), alloc_vectors, {"max_long", "median_long", "median_short", "max_short"});
    }

    epoch_frame::DataFrame GetSectorExposure(const epoch_frame::DataFrame &positions,
                                     const std::unordered_map<std::string, std::string> &sectorMapping) {
        auto index = positions.index();
        std::unordered_map<std::string, std::pair<std::vector<std::string>, arrow::ChunkedArrayVector>> grouper;
        for (auto const &asset: positions.column_names()) {
            auto sectorIter = sectorMapping.find(asset);
            if (sectorIter != sectorMapping.end()) {
                auto sector = sectorIter->second;
                grouper[sector].first.emplace_back(asset);
                grouper[sector].second.emplace_back(positions[asset].array());
            } else {
                SPDLOG_WARN("Warning: {} has no sector mapping. They will not be included in sector allocations",
                            asset);
            }
        }

        std::vector<std::string> columns;
        std::vector<arrow::ChunkedArrayPtr> values;
        for (auto const &[sector, arrayTable]: grouper) {
            columns.emplace_back(sector);
            values.emplace_back(make_dataframe(index, arrayTable.second, arrayTable.first).sum(
                    epoch_frame::AxisType::Column).array());
        }

        return make_dataframe(index, values, columns);
    }
}
