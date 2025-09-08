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
#include "epoch_protos/table_def.pb.h" // ColumnDef, enums in metadata
#include "portfolio/model.h"           // TearSheet
#include <epoch_metadata/transforms/transform_configuration.h>

namespace epoch_folio {

using ReportId = std::string;

struct ReportMetadata {
  ReportId id;                                // stable id e.g. "gap_report"
  std::string displayName;                    // human friendly
  std::string summary;                        // short description
  epoch_proto::EpochFolioCategory category{}; // classification
  std::vector<std::string> tags;              // discovery/AI hints
  std::vector<epoch_proto::ColumnDef> requiredColumns; // expected input columns
  std::vector<epoch_proto::EpochFolioDashboardWidget>
      typicalOutputs;           // for UI pre-layout
  glz::json_t defaultOptions{}; // JSON schema-like defaults
  std::string version{"0.1.0"};
  std::string owner{"epoch"};
};

class IReport {
public:
  virtual ~IReport() = default;

  virtual const ReportMetadata &metadata() const = 0;

  // Single dataset -> one TearSheet. Options and input mapping come from
  // configuration.
  virtual TearSheet generate(const epoch_frame::DataFrame &df) const = 0;

protected:
  explicit IReport(
      const epoch_metadata::transform::TransformConfiguration *config)
      : m_config(config) {}
  const epoch_metadata::transform::TransformConfiguration *m_config{nullptr};
};

using ReportCreator = std::function<std::unique_ptr<IReport>(
    const epoch_metadata::transform::TransformConfiguration *)>;

class ReportRegistry {
public:
  static ReportRegistry &instance();

  void register_report(const ReportMetadata &meta, ReportCreator creator);

  std::vector<ReportMetadata> list_reports() const;

  std::unique_ptr<IReport>
  create(const ReportId &id,
         const epoch_metadata::transform::TransformConfiguration *config) const;

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
          (MetaExpr),                                                          \
          [](const epoch_metadata::transform::TransformConfiguration *cfg) {   \
            return std::make_unique<ReportClass>(cfg);                         \
          });                                                                  \
    }                                                                          \
  };                                                                           \
  static ReportClass##Registrar global_##ReportClass##_registrar{};            \
  }

// Static initialization function to register all reports
void initialize_all_reports();

} // namespace epoch_folio
