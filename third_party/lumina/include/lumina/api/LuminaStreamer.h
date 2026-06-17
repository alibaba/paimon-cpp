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
#include <lumina/api/Options.h>
#include <lumina/api/Query.h>
#include <lumina/api/SearchResult.h>
#include <lumina/core/MemoryResource.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace lumina::api {

// Real-time, in-memory vector indexing. Single-writer Insert, concurrent Search.
class LuminaStreamer final : public core::NoCopyable
{
public:
    class Impl;

    LuminaStreamer(LuminaStreamer&&) noexcept;
    LuminaStreamer& operator=(LuminaStreamer&&) noexcept;
    explicit LuminaStreamer(std::unique_ptr<Impl> impl) noexcept;
    ~LuminaStreamer() noexcept;

    static core::Result<LuminaStreamer> Create(const StreamerOptions& options) noexcept;
    static core::Result<LuminaStreamer> Create(const StreamerOptions& options,
                                               const core::MemoryResourceConfig& memoryConfig) noexcept;

    /** Insert a single vector.
     *  @param data  Non-null pointer to exactly dim floats; returns InvalidArgument if null.
     *  @param id    Caller-assigned identifier. Uniqueness is NOT enforced. */
    core::Status Insert(const float* data, core::vector_id_t id) noexcept;

    using SearchHit = api::SearchHit;
    using SearchResult = api::SearchResult;

    // Search is `const`: it does not mutate any observable state of this
    // instance. Safe to call concurrently with other Search/GetMeta calls and
    // with a single in-flight Insert (see class-level thread-safety note).
    core::Result<SearchResult> Search(const Query& q, const SearchOptions& options) const noexcept;
    core::Result<SearchResult> Search(const Query& q, const SearchOptions& options,
                                      std::pmr::memory_resource& sessionPool) const noexcept;

    struct IndexInfo {
        uint64_t count {0};        // Total vectors currently visible to Search.
        core::dimension_t dim {0}; // Vector dimension
    };

    IndexInfo GetMeta() const noexcept;

    // -- Extension attach (per instance) --
    core::Status Attach(IStreamExtension& ext) noexcept;

private:
    std::unique_ptr<Impl> _p;
};

} // namespace lumina::api
