/*
 * Copyright 2025-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

// SearchWithTagExtension - Search extension for tag-based filtering.
// Supports SearchWithTag and SearchWithTagAndFilter methods.

#include <functional>
#include <lumina/api/Extension.h>
#include <lumina/api/LuminaSearcher.h>
#include <lumina/api/Options.h>
#include <lumina/core/Constants.h>
#include <lumina/core/Result.h>
#include <lumina/core/Types.h>
#include <lumina/extensions/experimental/TagFilter.h>
#include <memory>
#include <memory_resource>
#include <string_view>

namespace lumina::extensions { inline namespace experimental {

// SearchWithTagExtension provides tag-based filtering for vector search.
//
// Example usage:
// @code
// auto tagExt = SearchWithTagExtension();
// searcher.Attach(tagExt);
//
// auto tagFilter = TagFilter::And(
//     TagFilter::Eq("color", std::string("blue")),
//     TagFilter::In("category", std::vector<std::string>{"shoes", "bags"})
// );
// auto result = tagExt.SearchWithTag(query, tagFilter, options, pool);
// @endcode
class SearchWithTagExtension final : public api::ISearchExtension
{
public:
    // Forward-declared implementation; defined in SearchWithTagExtensionImpl.h.
    class Impl;

    SearchWithTagExtension();
    ~SearchWithTagExtension() override;

    constexpr static std::string_view ExtensionName() { return core::kExtensionSearchWithTag; }

    std::string_view Name() const noexcept override { return ExtensionName(); }

    // User-provided filter function type (vector_id -> bool, true = keep).
    using Filter = std::function<bool(core::vector_id_t)>;

    // Search with tag filter only.
    core::Result<api::LuminaSearcher::SearchResult> SearchWithTag(const api::Query& query, const TagFilter& tagFilter,
                                                                  const api::SearchOptions& options,
                                                                  std::pmr::memory_resource& sessionPool);

    // Search with both tag filter and custom filter (combined with AND logic).
    core::Result<api::LuminaSearcher::SearchResult>
    SearchWithTagAndFilter(const api::Query& query, const TagFilter& tagFilter, Filter userFilter,
                           const api::SearchOptions& options, std::pmr::memory_resource& sessionPool);

private:
    // Friend injection: backend code calls GetImpl(ext) via the
    // internal header (SearchWithTagExtensionImpl.h) to access _impl.
    friend Impl& GetImpl(SearchWithTagExtension& ext) noexcept;

    std::unique_ptr<Impl> _impl;
};

}} // namespace lumina::extensions::experimental
