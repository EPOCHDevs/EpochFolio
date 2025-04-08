#include "chart_def.h"
#include <tbb/tbb.h>
namespace epoch_folio {
        constexpr size_t kParallelThreshold = 10;

    SeriesLines MakeSeriesLines(const epoch_frame::DataFrame& df) {
        auto index = df.index()->array();
        SeriesLines result(df.num_cols());

        auto inner_loop = [](auto start, auto end, auto& result, auto& df, auto& index) {
            for (size_t i = start; i < end; ++i) {
                const auto& col = df.table()->field(i)->name();
                result[i].data.reserve(df.num_rows());
                result[i].name = col;
                epoch_frame::Series column = df[col];
                for (size_t row = 0; row < df.num_rows(); ++row) {
                    result[i].data.emplace_back(Point{index[row], column.iloc(row)});
                }
            }
        };

        if (df.num_cols() < kParallelThreshold) {
            inner_loop(0, df.num_cols(), result, df, index);
        } else {
            tbb::parallel_for(tbb::blocked_range<size_t>(0, df.num_cols()), [&](const tbb::blocked_range<size_t>& range) {
                inner_loop(range.begin(), range.end(), result, df, index);
            });
        }

        return result;
    }

    Line MakeSeriesLine(const epoch_frame::Series& series, std::optional<std::string> const& name) {
        auto df = series.to_frame(name);
        return MakeSeriesLines(df)[0];
    }

    SeriesLines MakeSeriesLines(const epoch_frame::Series& seriesA, const epoch_frame::Series& seriesB, std::optional<std::string> const& nameA, std::optional<std::string> const& nameB) {
        AssertFromFormat(seriesA.index()->equals(seriesB.index()), "Series A and B must have the same index");

        auto index = seriesA.index()->array();
        
        auto columnA = nameA.value_or(seriesA.name().value_or(""));
        auto columnB = nameB.value_or(seriesB.name().value_or(""));

        SeriesLines result(2);
        result[0].name = columnA;
        result[1].name = columnB;

        for (size_t i = 0; i < static_cast<size_t>(index.length()); ++i) {
            auto x = index[i];
            result[0].data.emplace_back(Point{x, seriesA.iloc(i)});
            result[1].data.emplace_back(Point{x, seriesB.iloc(i)});
        }

        return result;
    }   

}

