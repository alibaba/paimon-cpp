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
#include <functional>
#include <memory_resource>
#include <string_view>
#include <utility>

#include <lumina/api/Extension.h>
#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/api/SearchResult.h>
#include <lumina/core/Constants.h>
#include <lumina/core/Result.h>

namespace lumina::extensions {

template <typename ExtBase>
class BasicSearchWithFilter : public ExtBase
{
public:
    BasicSearchWithFilter() = default;
    ~BasicSearchWithFilter() override = default;

    constexpr static std::string_view ExtensionName() { return core::kExtensionSearchWithFilter; }

    std::string_view Name() const noexcept override { return ExtensionName(); }
    using Filter = std::function<bool(core::vector_id_t)>;
    using SearchFn = core::Result<api::SearchResult>(const api::Query& q, Filter filter,
                                                     const api::SearchOptions& options,
                                                     std::pmr::memory_resource& sessionPool);
    core::Result<api::SearchResult> SearchWithFilter(const api::Query& q, Filter filter,
                                                     const api::SearchOptions& options,
                                                     std::pmr::memory_resource& sessionPool)
    {
        if (!_callee) {
            return core::Result<api::SearchResult>::Err(
                core::Status {core::ErrorCode::FailedPrecondition, "extension not attached"});
        }
        return _callee(q, std::move(filter), options, sessionPool);
    }

    // -- Internal: called by backend wiring, not for direct use --
    void SetFunc(std::function<SearchFn> func) noexcept { _callee = std::move(func); }

private:
    std::function<SearchFn> _callee;
};

using SearchWithFilterExtension = BasicSearchWithFilter<api::ISearchExtension>;
using StreamWithFilterExtension = BasicSearchWithFilter<api::IStreamExtension>;

} // namespace lumina::extensions
