//
// Created by adesola on 1/13/25.
//

#include <catch2/catch_session.hpp>
#include <epoch_frame/serialization.h>

int main(int argc, char *argv[]) {
  auto arrowComputeStatus = arrow::compute::Initialize();
  if (!arrowComputeStatus.ok()) {
    std::stringstream errorMsg;
    errorMsg << "arrow compute initialized failed: " << arrowComputeStatus
             << std::endl;
    throw std::runtime_error(errorMsg.str());
  }

  epoch_frame::ScopedS3 scoped_s3;

  // your setup ...
  int result = Catch::Session().run(argc, argv);

  // your clean-up...

  return result;
}
