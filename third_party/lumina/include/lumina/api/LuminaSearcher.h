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
#include <lumina/core/MemoryResource.h>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Status.h>
#include <lumina/core/Types.h>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace lumina::io {
class FileReader;
}

namespace lumina::api {

// External "narrow waist" searcher
class LuminaSearcher final : public core::NoCopyable
{
public:
    class Impl;
    LuminaSearcher(std::unique_ptr<Impl> impl);
    // -- Semantics: movable, not copyable --
    LuminaSearcher(LuminaSearcher&&) noexcept;
    ~LuminaSearcher();

    static core::Result<LuminaSearcher> Create(const SearcherOptions& options);
    static core::Result<LuminaSearcher> Create(const SearcherOptions& options,
                                               const core::MemoryResourceConfig& memoryConfig);

    core::Status Open(const IOOptions& ioOptions);
    core::Status Open(std::unique_ptr<io::FileReader> reader, const IOOptions& ioOptions);

    struct SearchHit {
        core::vector_id_t id {0};
        float distance {0.0f};
    };

    struct SearchResult {
        std::vector<SearchHit> topk;
        std::unordered_map<std::string, std::string> searchStats;
    };

    // Index info: basic searcher metadata
    struct IndexInfo {
        uint64_t count {0}; // Total vectors
        uint32_t dim {0};   // Vector dimension
    };

    core::Result<SearchResult> Search(const Query& q, const SearchOptions& options);
    core::Result<SearchResult> Search(const Query& q, const SearchOptions& options,
                                      std::pmr::memory_resource& sessionPool);
    // -- Metadata --
    IndexInfo GetMeta() const noexcept;

    // -- Lifecycle --
    core::Status Close() noexcept;

    // -- Extension attach (per instance) --
    core::Status Attach(ISearchExtension& ext);

private:
    // pImpl: internal orchestration and backend selection live in the implementation
    std::unique_ptr<Impl> _p;
};

} // namespace lumina::api
