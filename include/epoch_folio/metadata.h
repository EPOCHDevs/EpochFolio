//
// Created by adesola on 1/14/25.
//

#pragma once

#include <epoch_core/enum_wrapper.h>
#include <epoch_protos/common.pb.h>
#include <glaze/glaze.hpp>
#include <string>
#include <vector>

namespace epoch_folio {
struct CategoryMetaData {
  std::string value;
  std::string label;
  std::string description;
};

std::vector<CategoryMetaData> GetCategoryMetaData();
} // namespace epoch_folio