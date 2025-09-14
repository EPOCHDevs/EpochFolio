#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <epoch_frame/frame_or_series.h>
#include <epoch_metadata/transforms/itransform.h>
#include <epoch_metadata/transforms/registry.h>
#include <epoch_metadata/transforms/transform_configuration.h>
#include <epoch_metadata/transforms/transform_registry.h>
#include <yaml-cpp/yaml.h>

#include "epoch_protos/tearsheet.pb.h"

namespace epoch_folio {

// IReporter extends ITransform to add TearSheet generation capability
// Following the TradeExecutorTransform pattern for column mapping
class IReporter : public epoch_metadata::transform::ITransform {
public:
  explicit IReporter(epoch_metadata::transform::TransformConfiguration config)
      : ITransform(std::move(config)) {
    // Build column mapping from inputs like TradeExecutor does
    // Map {transform_id}#result columns to expected names
    BuildColumnMappings();
  }

  // TransformData normalizes column names and generates TearSheet
  epoch_frame::DataFrame TransformData(const epoch_frame::DataFrame &df) const override {
    // 1. Get expected columns from configuration inputs
    std::vector<std::string> inputColumns;
    for (const auto& [inputId, columns] : m_config.GetInputs()) {
      inputColumns.insert(inputColumns.end(), columns.begin(), columns.end());
    }

    if (inputColumns.empty()) {
      // No inputs configured, return empty DataFrame
      return epoch_frame::DataFrame(
          df.index(),
          arrow::Table::MakeEmpty(arrow::schema(arrow::FieldVector{}))
              .MoveValueUnsafe());
    }

    // 2. Rename columns to canonical names (e.g., "gap_classifier#result" -> "gap")
    // This follows TradeExecutorTransform pattern
    auto normalizedDf = df[inputColumns].rename(m_columnMappings);

    // 3. Child classes implement generateTearsheet() to fill m_tearsheet
    generateTearsheet(normalizedDf);

    // 4. Return normalized DataFrame (computation graph expects DataFrame output)
    return normalizedDf;
  }

  // Public getter for the generated TearSheet
  epoch_proto::TearSheet GetTearSheet() const {
    return m_tearsheet;
  }

  virtual ~IReporter() = default;

protected:
  // Child classes only need to implement this to fill m_tearsheet
  virtual void generateTearsheet(const epoch_frame::DataFrame &normalizedDf) const = 0;

  void BuildColumnMappings() {
    // Similar to TradeExecutorTransform constructor
    // Map input columns like "gap_classifier#result" to expected names like "gap"
    const auto inputs = m_config.GetInputs();
    for (const auto& [inputId, inputColumns] : inputs) {
      if (!inputColumns.empty()) {
        // Map actual column name to expected name
        // e.g., "gap_classifier#result" -> "gap"
        m_columnMappings[inputColumns.front()] = inputId;
      }
    }
  }

  mutable epoch_proto::TearSheet m_tearsheet;  // Mutable since generateTearsheet is const
  std::unordered_map<std::string, std::string> m_columnMappings;
};

// Template specialization pattern for report metadata
// Each report implementation should specialize this
template<typename ReportClass>
struct ReportMetadata {
  static epoch_metadata::transforms::TransformsMetaData Get() {
    // Child classes specialize this to provide their metadata
    static_assert(sizeof(ReportClass) == 0,
                  "Report class must specialize ReportMetadata<T>::Get()");
    return {};
  }
};

// Unified registration function that handles both metadata and transform
// No need to pass id - it comes from metadata
template<typename ReportClass>
void RegisterReport() {
  // 1. Get and register the metadata
  auto metadata = ReportMetadata<ReportClass>::Get();
  epoch_metadata::transforms::ITransformRegistry::GetInstance().Register(metadata);

  // 2. Register the transform factory using metadata.id
  epoch_metadata::transform::Register<ReportClass>(metadata.id);
}

} // namespace epoch_folio
