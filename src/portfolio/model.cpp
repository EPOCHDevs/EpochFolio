//
// Created by adesola on 3/31/25.
//
#include "model.h"


namespace epoch_folio {

     std::pair<BoxPlotDataPoint, BoxPlotOutliers>  BoxPlotDataPoint::Make(
        int64_t category_index,
        const epoch_frame::Series &x) {
         const epoch_frame::Scalar offset{1.5};
         // Calculate quartiles
         auto _q1 = x.quantile(arrow::compute::QuantileOptions{0.25});
         auto _median = x.quantile(arrow::compute::QuantileOptions{0.5});
         auto _q3 = x.quantile(arrow::compute::QuantileOptions{0.75});

         // Calculate IQR
         auto iqr = _q3 - _q1;

         // Determine whisker bounds
         auto lower_whisker = std::max(_q1 - offset * iqr, x.min());
         auto upper_whisker = std::min(_q3 + offset * iqr, x.max());

         BoxPlotOutliers outliers;
         for (auto i = 0UL; i < x.size(); ++i) {
             auto value = x.iloc(i);
             if (value < lower_whisker || value > upper_whisker) {
                 outliers.emplace_back(category_index, value.as_double());
             }
         }

         return {BoxPlotDataPoint{
             lower_whisker.as_double(),
             _q1.as_double(),
             _median.as_double(),
             _q3.as_double(),
             upper_whisker.as_double()
     }, outliers};
     }

     epoch_frame::DataFrame MakeDataFrame(std::vector<epoch_frame::Series> const &series, std::vector<std::string> const &columns) {
        AssertFromFormat(series.size() == columns.size(), "Series and columns must be the same size");

        if (series.empty()) {
            return EMPTY_DATAFRAME;
        }

        arrow::ChunkedArrayVector frames;
        frames.reserve(series.size());

        for (auto const& frame : series) {
            frames.emplace_back(frame.array());
        }

        return make_dataframe(series.front().index(), frames, columns);
    }
}