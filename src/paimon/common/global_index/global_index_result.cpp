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

#include "paimon/global_index/global_index_result.h"

#include "fmt/format.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/global_index/bitmap_global_index_result.h"
#include "paimon/global_index/bitmap_topk_global_index_result.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
namespace paimon {
namespace {
void WriteBitmapAndScores(const RoaringBitmap64* bitmap, const std::vector<float>& scores,
                          MemorySegmentOutputStream* out, MemoryPool* pool) {
    std::shared_ptr<Bytes> bitmap_bytes = bitmap->Serialize(pool);
    out->WriteValue<int32_t>(bitmap_bytes->size());
    out->WriteBytes(bitmap_bytes);

    out->WriteValue<int32_t>(scores.size());
    for (auto score : scores) {
        out->WriteValue<float>(score);
    }
}

}  // namespace
Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexResult::And(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto supplier = [other, result = shared_from_this()]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter1,
                               result->CreateIterator());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter2,
                               other->CreateIterator());
        RoaringBitmap64 bitmap1;
        while (iter1->HasNext()) {
            bitmap1.Add(iter1->Next());
        }
        RoaringBitmap64 bitmap2;
        while (iter2->HasNext()) {
            bitmap2.Add(iter2->Next());
        }
        bitmap1 &= bitmap2;
        return bitmap1;
    };
    return std::make_shared<BitmapGlobalIndexResult>(supplier);
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexResult::Or(
    const std::shared_ptr<GlobalIndexResult>& other) {
    auto supplier = [other, result = shared_from_this()]() -> Result<RoaringBitmap64> {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter1,
                               result->CreateIterator());
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<GlobalIndexResult::Iterator> iter2,
                               other->CreateIterator());
        RoaringBitmap64 bitmap;
        while (iter1->HasNext()) {
            bitmap.Add(iter1->Next());
        }
        while (iter2->HasNext()) {
            bitmap.Add(iter2->Next());
        }
        return bitmap;
    };
    return std::make_shared<BitmapGlobalIndexResult>(supplier);
}

Result<PAIMON_UNIQUE_PTR<Bytes>> GlobalIndexResult::Serialize(
    const std::shared_ptr<GlobalIndexResult>& global_index_result,
    const std::shared_ptr<MemoryPool>& pool) {
    MemorySegmentOutputStream out(MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    out.WriteValue<int32_t>(VERSION);
    if (auto bitmap_result =
            std::dynamic_pointer_cast<BitmapGlobalIndexResult>(global_index_result)) {
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, bitmap_result->GetBitmap());
        WriteBitmapAndScores(bitmap, {}, &out, pool.get());
    } else if (auto bitmap_topk_result =
                   std::dynamic_pointer_cast<BitmapTopKGlobalIndexResult>(global_index_result)) {
        PAIMON_ASSIGN_OR_RAISE(const RoaringBitmap64* bitmap, bitmap_topk_result->GetBitmap());
        const auto& scores = bitmap_topk_result->GetScores();
        WriteBitmapAndScores(bitmap, scores, &out, pool.get());
    } else {
        return Status::Invalid(
            "invalid GlobalIndexResult, must be BitmapGlobalIndexResult or "
            "BitmapTopKGlobalIndexResult");
    }
    return MemorySegmentUtils::CopyToBytes(out.Segments(), 0, out.CurrentSize(), pool.get());
}

Result<std::shared_ptr<GlobalIndexResult>> GlobalIndexResult::Deserialize(
    const char* buffer, size_t length, const std::shared_ptr<MemoryPool>& pool) {
    auto input_stream = std::make_shared<ByteArrayInputStream>(buffer, length);
    DataInputStream in(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t version, in.ReadValue<int32_t>());
    if (version != VERSION) {
        return Status::Invalid(fmt::format("invalid version {} for GlobalIndexResult", version));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t bitmap_bytes_len, in.ReadValue<int32_t>());
    auto bitmap_bytes = Bytes::AllocateBytes(bitmap_bytes_len, pool.get());
    PAIMON_RETURN_NOT_OK(in.ReadBytes(bitmap_bytes.get()));
    RoaringBitmap64 bitmap;
    PAIMON_RETURN_NOT_OK(bitmap.Deserialize(bitmap_bytes->data(), bitmap_bytes->size()));

    PAIMON_ASSIGN_OR_RAISE(int32_t score_len, in.ReadValue<int32_t>());
    if (score_len == 0) {
        return std::make_shared<BitmapGlobalIndexResult>(
            [bitmap]() -> Result<RoaringBitmap64> { return bitmap; });
    }
    if (score_len != bitmap.Cardinality()) {
        return Status::Invalid("row id count mismatches score count");
    }
    std::vector<float> scores;
    scores.reserve(score_len);
    for (int32_t i = 0; i < score_len; i++) {
        PAIMON_ASSIGN_OR_RAISE(float score, in.ReadValue<float>());
        scores.push_back(score);
    }
    return std::make_shared<BitmapTopKGlobalIndexResult>(std::move(bitmap), std::move(scores));
}

Result<std::vector<Range>> GlobalIndexResult::ToRanges() const {
    std::vector<Range> ranges;
    PAIMON_ASSIGN_OR_RAISE(bool empty, IsEmpty());
    if (empty) {
        return ranges;
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<Iterator> iter, CreateIterator());
    int64_t range_start = iter->Next();
    int64_t range_end = range_start;
    while (iter->HasNext()) {
        int64_t current = iter->Next();
        if (current == range_end + 1) {
            // Extend the current range
            range_end = current;
        } else {
            ranges.emplace_back(range_start, range_end);
            range_start = current;
            range_end = current;
        }
    }
    // Add the last range
    ranges.emplace_back(range_start, range_end);
    return ranges;
}

}  // namespace paimon
