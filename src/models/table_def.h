//
// Created by adesola on 1/12/25.
//

#pragma once
#include <epoch_frame/frame_or_series.h>
#include <string>
#include <vector>

#include "epoch_folio/metadata.h"

namespace epoch_folio {
constexpr auto string_field = [](std::string const &s) {
  return arrow::field(s, arrow::utf8());
};
constexpr auto datetime_field = [](std::string const &s) {
  return arrow::field(s, arrow::timestamp(arrow::TimeUnit::NANO));
};
constexpr auto bool_field = [](std::string const &s) {
  return arrow::field(s, arrow::boolean());
};
constexpr auto int64_field = [](std::string const &s) {
  return arrow::field(s, arrow::int64());
};
constexpr auto uint64_field = [](std::string const &s) {
  return arrow::field(s, arrow::uint64());
};
constexpr auto day_time_interval_field = [](std::string const &s) {
  return arrow::field(s, arrow::day_time_interval());
};
constexpr auto float64_field = [](std::string const &s) {
  return arrow::field(s, arrow::float64());
};

struct SubCategoryDef {
  epoch_core::EpochFolioCategory type;
  std::string name;
};
using SubCategories = std::vector<SubCategoryDef>;

struct CategoryDef {
  epoch_core::EpochFolioCategory type;
  std::string name;
  std::vector<SubCategoryDef> subCategories;
};
using Categories = std::vector<CategoryDef>;

struct ColumnDef {
  std::string id;
  std::string name;
  epoch_core::EpochFolioType type;
};
using ColumnDefs = std::vector<ColumnDef>;

struct Table {
  epoch_core::EpochFolioDashboardWidget type;
  epoch_core::EpochFolioCategory category;
  std::string title;
  ColumnDefs columns;
  arrow::TablePtr data;
};

struct CardData {
  std::string title;
  epoch_frame::Scalar value;
  epoch_core::EpochFolioType type;
  uint64_t group{0};
};

struct CardDef {
  epoch_core::EpochFolioDashboardWidget type;
  epoch_core::EpochFolioCategory category;
  std::vector<CardData> data;
  uint64_t group_size;
};
} // namespace epoch_folio