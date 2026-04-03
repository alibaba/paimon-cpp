#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace lumina::api {

struct Query {
    Query(const float* data_, uint64_t size_) : data(data_), size(size_) {}
    const float* data = nullptr;
    const uint64_t size = 0;
};
// using Query = std::span<const float>;

} // namespace lumina::api
