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


#include <cstddef>
#include <cstdint>
#include <lumina/api/Extension.h>
#include <lumina/api/LuminaSearcher.h>
#include <lumina/api/Options.h>
#include <lumina/core/Constants.h>
namespace lumina::extensions {

class SearchWithFilterExtension final : public api::ISearchExtension
{
public:
    SearchWithFilterExtension() = default;
    ~SearchWithFilterExtension() = default;

    SearchWithFilterExtension(const SearchWithFilterExtension&) = delete;
    SearchWithFilterExtension& operator=(const SearchWithFilterExtension&) = delete;
    SearchWithFilterExtension(SearchWithFilterExtension&&) noexcept = delete;
    SearchWithFilterExtension& operator=(SearchWithFilterExtension&&) noexcept = delete;

    constexpr static std::string_view ExtensionName() { return core::kExtensionSearchWithFilter; }

    std::string_view Name() const noexcept override { return ExtensionName(); }
    using Filter = std::function<bool(core::VectorId)>;
    using SearchFn = core::Result<api::LuminaSearcher::SearchResult>(const api::Query& q, Filter filter,
                                                                     const api::SearchOptions& options,
                                                                     std::pmr::memory_resource& sessionPool);
    core::Result<api::LuminaSearcher::SearchResult> SearchWithFilter(const api::Query& q, Filter filter,
                                                                     const api::SearchOptions& options,
                                                                     std::pmr::memory_resource& sessionPool)
    {
        return _callee(q, std::move(filter), options, sessionPool);
    }

    void SetFunc(std::function<SearchFn> func) noexcept { _callee = std::move(func); }

private:
    std::function<SearchFn> _callee;
};

}
