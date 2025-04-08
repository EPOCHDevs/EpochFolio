//
// Created by adesola on 3/29/25.
//

#pragma once
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <chrono>

// Check if S3 testing is available
constexpr bool s3_testing_available() {
#ifdef EPOCH_FOLIO_S3_TEST_BUCKET
    return true;
#else
    return false;
#endif
}

// Get S3 path for testing
constexpr auto get_s3_test_path(const char* path) {
#ifdef EPOCH_FOLIO_S3_TEST_BUCKET
    return std::format("s3://{}/{}", EPOCH_FOLIO_S3_TEST_BUCKET, path);
#else
    return "";
#endif
}