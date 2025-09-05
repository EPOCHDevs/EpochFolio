#pragma once
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glaze/glaze.hpp>

#include <epoch_frame/frame_or_series.h>

#include "epoch_folio/metadata.h"
#include "models/table_def.h" // ColumnDef, enums in metadata
#include "portfolio/model.h"  // TearSheet

namespace epoch_folio {

using ReportId = std::string;

struct ReportMetadata {
  ReportId id;                               // stable id e.g. "gap_report"
  std::string displayName;                   // human friendly
  std::string summary;                       // short description
  epoch_core::EpochFolioCategory category{}; // classification
  std::vector<std::string> tags;             // discovery/AI hints
  ColumnDefs requiredColumns;                // expected input columns
  std::vector<epoch_core::EpochFolioDashboardWidget>
      typicalOutputs;           // for UI pre-layout
  glz::json_t defaultOptions{}; // JSON schema-like defaults
  std::string version{"0.1.0"};
  std::string owner{"epoch"};
};

class IReport {
public:
  virtual ~IReport() = default;

  virtual const ReportMetadata &metadata() const = 0;

  // Single dataset -> one TearSheet
  virtual TearSheet generate(const epoch_frame::DataFrame &df,
                             const glz::json_t &optionsJson) const = 0;

  // Asset-mapped datasets -> per-asset TearSheet
  virtual std::unordered_map<std::string, TearSheet> generate_per_asset(
      const std::unordered_map<std::string, epoch_frame::DataFrame> &assetToDf,
      const glz::json_t &optionsJson) const = 0;
};

using ReportCreator = std::function<std::unique_ptr<IReport>()>;

class ReportRegistry {
public:
  static ReportRegistry &instance();

  void register_report(const ReportMetadata &meta, ReportCreator creator);

  std::vector<ReportMetadata> list_reports() const;

  std::unique_ptr<IReport> create(const ReportId &id) const;

private:
  mutable std::mutex m_mutex{};
  std::unordered_map<ReportId, std::pair<ReportMetadata, ReportCreator>>
      m_reports{};
};

// Helper macro to register a report inside a single .cpp of the report
#define EPOCH_REGISTER_REPORT(ReportClass, MetaExpr)                           \
  namespace {                                                                  \
  struct ReportClass##Registrar {                                              \
    ReportClass##Registrar() {                                                 \
      epoch_folio::ReportRegistry::instance().register_report(                 \
          (MetaExpr), []() { return std::make_unique<ReportClass>(); });       \
    }                                                                          \
  };                                                                           \
  static ReportClass##Registrar global_##ReportClass##_registrar{};            \
  }

// Static initialization function to register all reports
void initialize_all_reports();

} // namespace epoch_folio
