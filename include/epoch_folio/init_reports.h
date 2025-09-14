#pragma once

namespace epoch_folio {

// Initialize all reports - must be called from main()
// This registers both metadata and transform factories
void InitializeReports();

} // namespace epoch_folio