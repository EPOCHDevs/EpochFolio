#include "epoch_folio/init_reports.h"
#include "gap_report.h"
// Include other report headers here as they are created

namespace epoch_folio {

// Initialize all reports - call this from main()
void InitializeReports() {
  // Register each report
  // The RegisterReport function gets the id from metadata, no need to pass it

  // Register GapReport with both metadata and transform factory
  RegisterReport<GapReport>();

  // Add more reports here as they are implemented
  // RegisterReport<VolumeReport>();
  // RegisterReport<PriceActionReport>();
  // etc.
}

} // namespace epoch_folio