//
// Created by adesola on 1/6/25.
//

#pragma once
#include <epoch_frame/common.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include "epoch_folio/aliases.h"


namespace epoch_folio::ep {
    template<typename T, typename ... Args>
    class RollingReturnsStat {

    public:
        explicit RollingReturnsStat(Args &&... args) : m_stat(T{std::forward<Args>(args) ...}) {}

        epoch_frame::Series operator()(epoch_frame::Series const &array, int64_t window) const {
            AssertFromFormat(window > 0, "window must be greater than 0");

            if (array.size() < window) return epoch_frame::Series{};

            epoch_frame::Series result = array.rolling_apply({ window
            }).apply([&](epoch_frame::Series const &chunk) {
                return epoch_frame::Scalar{std::move(m_stat(chunk))};
            });

            return result.iloc({.start=window-1});
        };

    private:
        T m_stat;
    };

    template<typename T, typename ... Args>
    class RollingFactorReturnsStat {

    public:
        explicit RollingFactorReturnsStat(Args &&... args) : m_stat(T{std::forward<Args>(args) ...}) {}

        epoch_frame::Series operator()(epoch_frame::DataFrame const &df, int64_t window) const {
            if (df.size() < static_cast<size_t>(window)) return epoch_frame::Series{};

            auto factor = epoch_frame::make_dataframe(df.index(), df.table()->columns(), {"strategy", "benchmark"});

            auto result = factor.rolling_apply({window}).apply([&](epoch_frame::DataFrame const &df) {
                return epoch_frame::Scalar{m_stat(df)};
            });

            return result.iloc({.start=window-1});
        };

    private:
        T m_stat;
    };
}