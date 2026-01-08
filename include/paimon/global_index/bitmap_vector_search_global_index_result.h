/*
 * Copyright 2024-present Alibaba Inc.
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

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "paimon/global_index/global_index_result.h"
#include "paimon/utils/roaring_bitmap64.h"
#include "paimon/visibility.h"

namespace paimon {
/// Represents a vector search global index result that combines a Roaring bitmap of candidate row
/// ids with an array of associated relevance scores.
///
/// **Important Ordering Note**: Despite inheriting from VectorSearchGlobalIndexResult, the results
/// are
/// **NOT sorted by score**. Instead, both the bitmap and the score vector are ordered by
/// **ascending row id**. This design enables efficient merging and set operations while preserving
/// row id-to-score mapping.
class PAIMON_EXPORT BitmapVectorSearchGlobalIndexResult : public VectorSearchGlobalIndexResult {
 public:
    BitmapVectorSearchGlobalIndexResult(RoaringBitmap64&& bitmap, std::vector<float>&& scores)
        : bitmap_(std::move(bitmap)), scores_(std::move(scores)) {
        assert(static_cast<size_t>(bitmap_.Cardinality()) == scores_.size());
    }

    class VectorSearchIterator : public VectorSearchGlobalIndexResult::VectorSearchIterator {
     public:
        VectorSearchIterator(const RoaringBitmap64* bitmap, RoaringBitmap64::Iterator&& iter,
                             const float* scores)
            : bitmap_(bitmap), iter_(std::move(iter)), scores_(scores) {}

        bool HasNext() const override {
            return iter_ != bitmap_->End();
        }

        std::pair<int64_t, float> NextWithScore() override {
            uint64_t value = *iter_;
            ++iter_;
            return {value, scores_[cursor_++]};
        }

     private:
        size_t cursor_ = 0;
        const RoaringBitmap64* bitmap_;
        RoaringBitmap64::Iterator iter_;
        const float* scores_;
    };

    Result<std::unique_ptr<GlobalIndexResult::Iterator>> CreateIterator() const override;

    Result<std::unique_ptr<VectorSearchGlobalIndexResult::VectorSearchIterator>>
    CreateVectorSearchIterator() const override;

    Result<std::shared_ptr<GlobalIndexResult>> And(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<std::shared_ptr<GlobalIndexResult>> Or(
        const std::shared_ptr<GlobalIndexResult>& other) override;

    Result<std::shared_ptr<GlobalIndexResult>> AddOffset(int64_t offset) override;

    Result<bool> IsEmpty() const override;

    std::string ToString() const override;

    /// @return A non-owning, const pointer to the bitmap. The row ids in the bitmap are stored in
    ///         ascending order (as guaranteed by Roaring64 iteration).
    Result<const RoaringBitmap64*> GetBitmap() const;

    /// @return A const reference to a vector of float scores, where the i-th element corresponds to
    ///         the i-th row id when iterating the bitmap in **ascending row id order**.
    const std::vector<float>& GetScores() const;

 private:
    // TODO(xinyu.lxy): may use pair<int64_t, float>
    RoaringBitmap64 bitmap_;
    // ordered by row id
    std::vector<float> scores_;
};
}  // namespace paimon
