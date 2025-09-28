//
// Created by adesola on 3/31/25.
//
#include "model.h"

namespace epoch_folio {

epoch_frame::DataFrame
MakeDataFrame(std::vector<epoch_frame::Series> const &series,
              std::vector<std::string> const &columns) {
  AssertFromFormat(series.size() == columns.size(),
                   "Series and columns must be the same size");

  if (series.empty()) {
    return EMPTY_DATAFRAME;
  }

  arrow::ChunkedArrayVector frames;
  frames.reserve(series.size());

  for (auto const &frame : series) {
    frames.emplace_back(frame.array());
  }

  return make_dataframe(series.front().index(), frames, columns);
}
} // namespace epoch_folio