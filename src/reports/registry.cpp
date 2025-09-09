#include "gap_report.h"
#include "ireport.h"

namespace epoch_folio {

ReportRegistry &ReportRegistry::instance() {
  static ReportRegistry reg;
  return reg;
}

void ReportRegistry::register_report(const ReportMetadata &meta,
                                     ReportCreator creator) {
  std::scoped_lock lock{m_mutex};
  m_reports[meta.id] = std::make_pair(meta, std::move(creator));
}

std::vector<ReportMetadata> ReportRegistry::list_reports() const {
  std::scoped_lock lock{m_mutex};
  std::vector<ReportMetadata> result;
  result.reserve(m_reports.size());
  for (auto const &kv : m_reports) {
    result.emplace_back(kv.second.first);
  }
  return result;
}

std::unique_ptr<IReport> ReportRegistry::create(
    const ReportId &id,
    const epoch_metadata::transform::TransformConfiguration *config) const {
  std::scoped_lock lock{m_mutex};
  auto it = m_reports.find(id);
  if (it == m_reports.end()) {
    return nullptr;
  }
  return it->second.second(config);
}

void initialize_all_reports() {
  // Explicitly register all reports to ensure they're available
  // GapReport::register_report();
}

} // namespace epoch_folio
