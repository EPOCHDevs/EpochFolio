//
// Created by adesola on 1/6/25.
//

#pragma once
#include <epoch_frame/series.h>
#include <epoch_frame/dataframe.h>
#include <epoch_core/common_utils.h>

namespace epoch_folio::ep {

    using RollingUnaryVectorizedFunction = std::function<epoch_frame::Series(epoch_frame::Series const &, int)>;
    using RollingBinaryVectorizedFunction = std::function<epoch_frame::Series(epoch_frame::Series const &, epoch_frame::Series const &, int)>;

    // template<typename T>
    // RollingUnaryVectorizedFunction CreateUnaryVectorizedRollFunction(T const &function) {
    //     return [function](epoch_frame::Series const &arr, int64_t window) -> epoch_frame::Series {
    //         return arr.rolling_apply(function, window);
    //     };
    // }

    // RollingBinaryVectorizedFunction CreateBinaryVectorizedRollFunction(auto const &function) {
    //     return [function](epoch_frame::Series const &arr1, epoch_frame::Series const &arr2, int64_t window) -> epoch_frame::Series {
            
    //         epoch_frame::DataFrame df{
    //                 std::vector{arr1.values<double>(), arr2.values<double>()},
    //                 std::vector<std::string>{"a", "b"},
    //                 arr1.indexArray()
    //         };
    //         return df.rolling<double>([function](epoch_frame::Series const &a, epoch_frame::Series const &b) {
    //             return function(a, b);
    //         }, window, "a", "b");
    //     };
    // }
}