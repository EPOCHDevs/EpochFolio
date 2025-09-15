# EpochFolio TODO

## IReporter Integration with TransformConfiguration

### Overview
Integrate IReporter as special output nodes in the computation graph that generate TearSheet objects for dashboards. This eliminates the need for separate ReportMetadata and ReportRegistry by unifying everything under the TransformConfiguration system.

### Revised Approach
- Use existing `TransformNodeRenderKind::Standard` for rendering
- Use existing `TransformCategory::Executor` for categorization
- Add only `isReporter` flag to identify reporter transforms
- Keep `IODataType` as-is (TearSheet handled at interface level, not data type level)
- Use dynamic_cast to access IReport interface when needed

### Changes Required in EpochMetadata

#### 1. Extend TransformsMetaData Structure
**File**: `include/epoch_metadata/transforms/metadata.h`
- [x] ~~Add `bool isReporter` flag to identify reporter transforms~~ (DONE)
- [x] ~~Optionally add `std::vector<std::string> tags` for discovery/AI hints~~ (DONE)

### Changes Required in EpochFolio

#### 1. Create IReport Interface (extends existing ITransform)
**File**: Update `src/reports/ireport.h`
- [x] ~~Remove `ReportMetadata` struct~~ (DONE)
- [x] ~~Remove `ReportRegistry` class~~ (DONE)
- [x] ~~Create IReporter extending ITransform from EpochMetadata~~ (DONE)
  ```cpp
  // ITransform already exists in epoch_metadata/transforms/itransform.h
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

      // 2. Rename columns to canonical names (e.g., "gap_classifier#result" -> "gap")
      // This follows TradeExecutorTransform pattern (line 113)
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

  protected:
    // Child classes only need to implement this to fill m_tearsheet
    virtual void generateTearsheet(const epoch_frame::DataFrame &normalizedDf) const = 0;

    void BuildColumnMappings() {
      // Similar to TradeExecutorTransform constructor (lines 63-95)
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
  ```

#### 2. Update Registration System
- [x] ~~Create unified registration pattern with ReportRegistrar template~~ (DONE)
- [x] ~~RegisterReport function handles both metadata and transform registration~~ (DONE)
- [x] ~~EPOCH_REGISTER_REPORT macro for simple registration~~ (DONE)
  ```cpp
  // In report implementation file
  template<>
  struct epoch_folio::ReportRegistrar<GapReport> {
    using Type = GapReport;

    static epoch_metadata::transforms::TransformsMetaData GetMetadata() {
      return {
        .id = "gap_report",
        .category = epoch_core::TransformCategory::Executor,
        .renderKind = epoch_core::TransformNodeRenderKind::Standard,
        .name = "Gap Report",
        .isReporter = true,
        // ... other metadata fields
      };
    }
  };

  EPOCH_REGISTER_REPORT(GapReport)
  ```
- [ ] Discovery via:
  ```cpp
  auto transform = epoch_metadata::transform::TransformRegistry::GetInstance().Get(config);
  if (config.GetTransformDefinition().GetMetadata().isReporter) {
    // Run transform to generate TearSheet
    auto resultDf = transform->TransformData(inputDf);

    // Cast to IReporter to get the TearSheet
    if (auto* reporter = dynamic_cast<IReporter*>(transform.get())) {
      auto tearsheet = reporter->GetTearSheet();
      // Use tearsheet for dashboard/visualization
    }
  }
  ```

#### 3. Update Existing Reports
**File**: `src/reports/gap_report.h` and `src/reports/gap_report.cpp`
- [ ] Inherit from IReporter instead of old IReport
- [ ] Implement `generateTearsheet()` to fill `m_tearsheet` member
- [ ] Remove old `generate()` method that returned TearSheet
- [ ] Register as transforms with isReporter flag

### Integration Architecture

#### Computation Graph Integration
```
DataSource → Transform → Transform → IReporter → TearSheet
                ↓           ↓            ↓
            DataFrame   DataFrame   Dashboard
```

#### Key Benefits
1. **Unified Registry**: Single registry for all transforms including reports
2. **Graph Integration**: Reports become first-class computation graph nodes
3. **Composability**: Can chain reports after other transforms
4. **Consistency**: Same metadata and configuration system for all nodes
5. **Less Duplication**: Remove redundant metadata structures

### Migration Steps

1. **Phase 1: EpochMetadata Updates**
   - [x] ~~Add `isReporter` flag to TransformsMetaData~~ (DONE)
   - [x] ~~Add `tags` vector to TransformsMetaData~~ (DONE)

2. **Phase 2: EpochFolio Refactoring**
   - [x] ~~Create IReporter interface extending ITransform~~ (DONE)
   - [x] ~~Implement column mapping pattern like TradeExecutor~~ (DONE)
   - [x] ~~Create unified registration system~~ (DONE)
   - [x] ~~Remove old ReportMetadata/ReportRegistry~~ (DONE)
   - [ ] Migrate existing reports to new IReporter interface

3. **Phase 3: Testing & Validation**
   - [ ] Test dynamic_cast approach for report identification
   - [ ] Test report generation through computation graph
   - [ ] Validate dashboard widget output

### Technical Details

#### Leveraging Existing Infrastructure
- **ITransformBase** (`epoch_metadata/transforms/itransform.h:12`) - Abstract interface
- **ITransform** (`epoch_metadata/transforms/itransform.h:46`) - Concrete implementation with TransformConfiguration
- **TransformRegistry** (`epoch_metadata/transforms/transform_registry.h:18`) - Singleton registry
- **Registration** - Uses `Register<T>()` template function or `REGISTER_TRANSFORM` macro

#### Report Identification
```cpp
// In computation graph executor
if (node->metadata().isReporter) {
  if (auto* report = dynamic_cast<IReport*>(node.get())) {
    TearSheet tearsheet = report->generate(dataframe);
    // Handle tearsheet output
  }
}
```

#### Shared Registry Benefits
- Single registration system for all transforms
- Reports discovered by filtering: `metadata.isReporter == true`
- No duplicate registry maintenance
- Consistent lifecycle management

### Open Questions
- [ ] Should reports support streaming/incremental updates?
- [ ] How to handle report-specific validation rules?
- [ ] Should we support report composition (reports using other reports)?
- [ ] Version compatibility strategy for existing report configurations?

### Dependencies
- EpochMetadata library must be updated first
- Protobuf definitions for TearSheet must be stable
- Computation graph executor needs to handle TearSheet outputs