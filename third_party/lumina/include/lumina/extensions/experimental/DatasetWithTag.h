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

#include <cstdint>
#include <lumina/core/NoCopyable.h>
#include <lumina/core/Result.h>
#include <lumina/core/Types.h>
#include <string>
#include <variant>
#include <vector>

namespace lumina::extensions { inline namespace experimental {

/**
 * Type-safe tag dimension values: all tag values within a single dimension share the
 * same type. The outer vector is indexed by vectorIdx, the inner vector holds
 * multi-value tags for that vector.
 *
 * Example: for a "color" dimension with 3 vectors:
 *   TagDimensionValues colors = std::vector<std::vector<std::string>>{
 *       {"red", "blue"},   // vector 0 has two colors
 *       {"green"},         // vector 1 has one color
 *       {}                 // vector 2 has no color
 *   };
 */
using TagDimensionValues = std::variant<std::vector<std::vector<int64_t>>, std::vector<std::vector<double>>,
                                        std::vector<std::vector<std::string>>>;

/** Returns the number of vectors (outer vector size) in a TagDimensionValues. */
inline uint64_t GetVectorCount(const TagDimensionValues& values) noexcept
{
    return std::visit([](const auto& vec) -> uint64_t { return vec.size(); }, values);
}

/**
 * TagDimensionData holds the tag data for a single tagk dimension across all vectors in a batch.
 *
 * Layout:
 *   - tagkName: the tagk name string for this dimension.
 *   - values[i]: the tag values for vector i (may contain multiple values for multi-value tags).
 *   - GetVectorCount(values) == vectorCount.
 *
 * The variant alternative determines the value type for the entire dimension
 * (all int64_t, all double, or all string). This is enforced at compile time.
 */
struct TagDimensionData {
    std::string tagkName;
    TagDimensionValues values;
};

/**
 * DatasetWithTag is the streaming data source for tag-aware index building.
 * It extends the concept of api::Dataset by carrying tag information alongside
 * vectors and IDs.
 *
 * Tag data is organized per tagk dimension: each TagDimensionData holds the tag
 * values for all vectors in the batch. Different tag dimensions can have
 * different value types (string, int64, double).
 */
class DatasetWithTag : public core::NoCopyable
{
public:
    virtual ~DatasetWithTag() noexcept = default;

    /** Vector dimension. Caller must keep it consistent with the Builder dimension. */
    virtual uint32_t Dim() const noexcept = 0;
    /** Total data size (optional, for pre-allocation). Return 0 if unknown. */
    virtual uint64_t TotalSize() const noexcept = 0;

    /**
     * Fetch the next batch of vectors, IDs, and tags into the provided buffers.
     * Implementations should clear and fill the buffers (not append).
     * Return value: number of vectors in this batch; return 0 at end; return Status on error.
     *
     * @param vectorBuffer Output buffer for vector data.
     * @param idBuffer Output buffer for vector IDs.
     * @param tagDimensionsData Output buffer for per-dimension tag data. The vector
     *        is resized to tag dimensions by the implementation. Each element holds
     *        the tag values for one tagk dimension across all vectors.
     */
    virtual core::Result<uint64_t> GetNextBatch(std::vector<float>& vectorBuffer,
                                                std::vector<core::vector_id_t>& idBuffer,
                                                std::vector<TagDimensionData>& tagDimensionsData) noexcept = 0;
};

}} // namespace lumina::extensions::experimental
