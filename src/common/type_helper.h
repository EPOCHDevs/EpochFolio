#include <string>
#include <vector>

#include <arrow/api.h>

namespace epoch_folio {
constexpr auto string_field = [](std::string const &s) {
  return arrow::field(s, arrow::utf8());
};
constexpr auto datetime_field = [](std::string const &s) {
  return arrow::field(s, arrow::timestamp(arrow::TimeUnit::NANO));
};
constexpr auto bool_field = [](std::string const &s) {
  return arrow::field(s, arrow::boolean());
};
constexpr auto int64_field = [](std::string const &s) {
  return arrow::field(s, arrow::int64());
};
constexpr auto uint64_field = [](std::string const &s) {
  return arrow::field(s, arrow::uint64());
};
constexpr auto day_time_interval_field = [](std::string const &s) {
  return arrow::field(s, arrow::day_time_interval());
};
constexpr auto float64_field = [](std::string const &s) {
  return arrow::field(s, arrow::float64());
};
} // namespace epoch_folio