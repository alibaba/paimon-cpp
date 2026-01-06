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

#include "paimon/global_index/bitmap_vector_search_global_index_result.h"

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "paimon/global_index/bitmap_global_index_result.h"

namespace paimon {
namespace {
std::map<int64_t, float> CreateIdToScoreMap(const RoaringBitmap64& bitmap,
                                            const std::vector<float>& scores) {
    std::map<int64_t, float> id_to_score;
    size_t idx = 0;
    for (auto iter = bitmap.Begin(); iter != bitmap.End(); ++iter, ++idx) {
        id_to_score[*iter] = scores[idx];
    }
    return id_to_score;
}
std::vector<float> GetScoresFromMap(const RoaringBitmap64& bitmap,
                                    std::map<int64_t, float>& id_to_score) {
    std::vector<float> scores;
    scores.reserve(bitmap.Cardinality());
    for (auto iter = bitmap.Begin(); iter != bitmap.End(); ++iter) {
        scores.push_back(id_to_score[*iter]);
    }
    return scores;
}
}  // namespace
Result<std::unique_ptr<GlobalIndexResult::Iterator>>
BitmapVectorSearchGlobalIndexResult::CreateIterator() const {
    return std::make_unique<BitmapGlobalIndexResult::Iterator>(&bitmap_, bitmap_.Begin());
}

Result<std::unique_ptr<VectorSearchGlobalIndexResult::VectorSearchIterator>>
BitmapVectorSearchGlobalIndexResult::CreateVectorSearchIterator() const {
    return std::make_unique<BitmapVectorSearchGlobalIndexResult::VectorSearchIterator>(
        &bitmap_, bitmap_.Begin(), scores_.data());
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapVectorSearchGlobalIndexResult::And(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto vector_search_other =
        std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(other);
    if (vector_search_other) {
        // If current and other result are both BitmapVectorSearchGlobalIndexResult, return
        // BitmapGlobalIndexResult. Erase scores to prevent the same row id with different
        // scores in current and other results.
        auto supplier = [vector_search_other,
                         result = std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(
                             shared_from_this())]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r1, vector_search_other->GetBitmap());
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r2, result->GetBitmap());
            return RoaringBitmap64::And(*r1, *r2);
        };
        return std::make_shared<BitmapGlobalIndexResult>(supplier);
    }
    auto bitmap_other = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (bitmap_other) {
        // If other bitmap is BitmapGlobalIndexResult, return BitmapVectorSearchGlobalIndexResult as
        // score must exist in current vector search result.
        std::map<int64_t, float> id_to_score = CreateIdToScoreMap(bitmap_, scores_);
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* other_bitmap, bitmap_other->GetBitmap());
        auto and_bitmap = RoaringBitmap64::And(bitmap_, *other_bitmap);
        std::vector<float> and_scores = GetScoresFromMap(and_bitmap, id_to_score);
        return std::make_shared<BitmapVectorSearchGlobalIndexResult>(std::move(and_bitmap),
                                                                     std::move(and_scores));
    }
    return GlobalIndexResult::And(other);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapVectorSearchGlobalIndexResult::Or(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto vector_search_other =
        std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(other);
    if (vector_search_other) {
        // If current and other result are both BitmapVectorSearchGlobalIndexResult, return
        // BitmapVectorSearchGlobalIndexResult when current and other have has no intersection row
        // id.
        std::map<int64_t, float> id_to_score = CreateIdToScoreMap(bitmap_, scores_);
        size_t idx = 0;
        for (auto iter = vector_search_other->bitmap_.Begin();
             iter != vector_search_other->bitmap_.End(); ++iter, ++idx) {
            if (id_to_score.find(*iter) != id_to_score.end()) {
                return Status::Invalid(
                    "not support two BitmapVectorSearchGlobalIndexResult or with same row id");
            }
            id_to_score[*iter] = vector_search_other->scores_[idx];
        }
        auto or_bitmap = RoaringBitmap64::Or(bitmap_, vector_search_other->bitmap_);
        std::vector<float> or_scores = GetScoresFromMap(or_bitmap, id_to_score);
        return std::make_shared<BitmapVectorSearchGlobalIndexResult>(std::move(or_bitmap),
                                                                     std::move(or_scores));
    }

    auto bitmap_other = std::dynamic_pointer_cast<BitmapGlobalIndexResult>(other);
    if (bitmap_other) {
        // If other bitmap is BitmapGlobalIndexResult, return BitmapGlobalIndexResult as
        // score for union row id is unknown.
        auto supplier = [bitmap_other,
                         result = std::dynamic_pointer_cast<BitmapVectorSearchGlobalIndexResult>(
                             shared_from_this())]() -> Result<RoaringBitmap64> {
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r1, bitmap_other->GetBitmap());
            PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* r2, result->GetBitmap());
            return RoaringBitmap64::Or(*r1, *r2);
        };
        return std::make_shared<BitmapGlobalIndexResult>(supplier);
    }
    return GlobalIndexResult::Or(other);
}

Result<std::shared_ptr<GlobalIndexResult>> BitmapVectorSearchGlobalIndexResult::AddOffset(
    int64_t offset) {
    PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, GetBitmap());
    RoaringBitmap64 bitmap64;
    for (auto iter = bitmap->Begin(); iter != bitmap->End(); ++iter) {
        bitmap64.Add(offset + (*iter));
    }
    auto scores = GetScores();
    return std::make_shared<BitmapVectorSearchGlobalIndexResult>(std::move(bitmap64),
                                                                 std::move(scores));
}

Result<bool> BitmapVectorSearchGlobalIndexResult::IsEmpty() const {
    return bitmap_.IsEmpty();
}

Result<const RoaringBitmap64*> BitmapVectorSearchGlobalIndexResult::GetBitmap() const {
    return &bitmap_;
}

const std::vector<float>& BitmapVectorSearchGlobalIndexResult::GetScores() const {
    return scores_;
}

std::string BitmapVectorSearchGlobalIndexResult::ToString() const {
    std::vector<std::string> formatted_scores;
    formatted_scores.reserve(scores_.size());
    for (const auto& score : scores_) {
        formatted_scores.push_back(fmt::format("{:.2f}", score));
    }
    return fmt::format("row ids: {}, scores: {{{}}}", bitmap_.ToString(),
                       fmt::join(formatted_scores, ","));
}

}  // namespace paimon
