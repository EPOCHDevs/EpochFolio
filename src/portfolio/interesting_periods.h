//
// Created by adesola on 1/9/25.
//

#pragma once
#include "model.h"


namespace epoch_folio {
    using std::chrono_literals::operator""y;
    using std::chrono_literals::operator""d;
    using std::chrono::January;
    using std::chrono::February;
    using std::chrono::March;
    using std::chrono::April;
    using std::chrono::May;
    using std::chrono::June;
    using std::chrono::July;
    using std::chrono::August;
    using std::chrono::September;
    using std::chrono::October;
    using std::chrono::November;
    using std::chrono::December;

    const InterestingDateRanges PERIODS = {
            {"Dotcom", epoch_frame::Date(2000y, March, 10d), epoch_frame::Date(2000y, September, 10d)},
            {"Lehman", epoch_frame::Date(2008y, August, 1d), epoch_frame::Date(2008y, October, 1d)},
            {"9/11", epoch_frame::Date(2001y, September, 11d), epoch_frame::Date(2001y, October, 11d)},
            {"US downgrade/European Debt Crisis", epoch_frame::Date(2011y, August, 5d), epoch_frame::Date(2011y, September, 5d)},
            {"Fukushima", epoch_frame::Date(2011y, March, 16d), epoch_frame::Date(2011y, April, 16d)},
            {"US Housing", epoch_frame::Date(2003y, January, 8d), epoch_frame::Date(2003y, February, 8d)},
            {"EZB IR Event", epoch_frame::Date(2012y, September, 10d), epoch_frame::Date(2012y, October, 10d)},
            {"Aug07", epoch_frame::Date(2007y, August, 1d), epoch_frame::Date(2007y, September, 1d)},
            {"Mar08", epoch_frame::Date(2008y, March, 1d), epoch_frame::Date(2008y, April, 1d)},
            {"Sept08", epoch_frame::Date(2008y, September, 1d), epoch_frame::Date(2008y, October, 1d)},
            {"2009Q1", epoch_frame::Date(2009y, January, 1d), epoch_frame::Date(2009y, March, 1d)},
            {"2009Q2", epoch_frame::Date(2009y, March, 1d), epoch_frame::Date(2009y, June, 1d)},
            {"Flash Crash", epoch_frame::Date(2010y, May, 5d), epoch_frame::Date(2010y, May, 10d)},
            {"Apr14", epoch_frame::Date(2014y, April, 1d), epoch_frame::Date(2014y, May, 1d)},
            {"Oct14", epoch_frame::Date(2014y, October, 1d), epoch_frame::Date(2014y, November, 1d)},
            {"Fall2015", epoch_frame::Date(2015y, August, 15d), epoch_frame::Date(2015y, September, 30d)},
            {"Low Volatility Bull Market", epoch_frame::Date(2005y, January, 1d), epoch_frame::Date(2007y, August, 1d)},
            {"GFC Crash", epoch_frame::Date(2007y, August, 1d), epoch_frame::Date(2009y, April, 1d)},
            {"Recovery", epoch_frame::Date(2009y, April, 1d), epoch_frame::Date(2013y, January, 1d)},
            {"New Normal", epoch_frame::Date(2013y, January, 1d), epoch_frame::Date(2018y, September, 21d)},
            {"Covid", epoch_frame::Date(2020y, February, 11d), epoch_frame::Date(2022y, December, 31d)}  // Updated end date
    };
}