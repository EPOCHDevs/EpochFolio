//
// Created by assistant on 9/8/25.
//

#pragma once

#include "chart_def.h"
#include <arrow/chunked_array.h>
#include <epoch_protos/common.pb.h>
#include <memory>

#include <arrow/table.h>
#include <epoch_frame/scalar.h>
#include <epoch_frame/frame_or_series.h>

#include <epoch_protos/table_def.pb.h>

namespace epoch_folio {

epoch_proto::Array
MakeArrayFromArrow(const std::shared_ptr<arrow::ChunkedArray> &chunked_array);

epoch_proto::TableData
MakeTableDataFromArrow(const std::shared_ptr<arrow::Table> &table);

// TableBuilder - Helper for building tables from DataFrames
class TableBuilder {
private:
  epoch_proto::Table table_;
  std::vector<std::shared_ptr<arrow::Field>> fields_;
  std::vector<std::shared_ptr<arrow::Array>> arrays_;
  bool include_index_ = false;
  std::optional<std::string> index_name_;
  std::optional<uint32_t> limit_;

public:
  explicit TableBuilder(const std::string& title = "",
                       const std::string& category = "Tables") {
    table_.set_type(epoch_proto::WidgetDataTable);
    table_.set_title(title);
    table_.set_category(category);
  }

  TableBuilder& setTitle(const std::string& title) {
    table_.set_title(title);
    return *this;
  }

  TableBuilder& setCategory(const std::string& category) {
    table_.set_category(category);
    return *this;
  }

  TableBuilder& includeIndex(const std::string& name = "Date") {
    include_index_ = true;
    index_name_ = name;
    return *this;
  }

  TableBuilder& limit(uint32_t max_rows) {
    limit_ = max_rows;
    return *this;
  }

  // Build table from DataFrame
  epoch_proto::Table fromDataFrame(const epoch_frame::DataFrame& df);

  // Build table from Arrow table directly
  epoch_proto::Table fromArrowTable(const std::shared_ptr<arrow::Table>& arrow_table) {
    *table_.mutable_data() = MakeTableDataFromArrow(arrow_table);
    return std::move(table_);
  }
};

// Convenience function to create a simple table from DataFrame
epoch_proto::Table MakeTableFromDataFrame(
    const epoch_frame::DataFrame& df,
    const std::string& title,
    bool include_index = false,
    std::optional<uint32_t> limit = std::nullopt);

} // namespace epoch_folio
