//
// Created by assistant on 9/8/25.
//

#pragma once

#include <arrow/chunked_array.h>
#include <epoch_protos/common.pb.h>
#include <memory>
#include <vector>

#include <arrow/table.h>
#include <epoch_frame/scalar.h>

#include <epoch_protos/table_def.pb.h>

namespace epoch_folio {
epoch_proto::Array
MakeArrayFromArrow(const std::shared_ptr<arrow::ChunkedArray> &array);

epoch_proto::TableData
MakeTableDataFromArrow(const std::shared_ptr<arrow::Table> &table);

} // namespace epoch_folio
