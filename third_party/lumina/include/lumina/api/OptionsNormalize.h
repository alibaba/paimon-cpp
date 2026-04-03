#pragma once

#include <lumina/api/Options.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <string>
#include <unordered_map>

namespace lumina::api {

// Weakly-typed C++ entry: normalize string->string maps into typed Options.
core::Result<BuilderOptions> NormalizeBuilderOptions(const std::unordered_map<std::string, std::string>& raw);
core::Result<SearcherOptions> NormalizeSearcherOptions(const std::unordered_map<std::string, std::string>& raw);
core::Result<SearchOptions> NormalizeSearchOptions(const std::string& indexType,
    const std::unordered_map<std::string, std::string>& raw);
// check global search options
core::Status ValidateSearchOptions(const api::SearchOptions& options);

} // namespace lumina::api
