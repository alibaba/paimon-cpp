/*
 * Copyright 2026-present Alibaba Inc.
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

#include <map>
#include <memory>
#include <vector>

#include "paimon/common/file_index/rangebitmap/bit_slice_index_bitmap.h"
#include "paimon/common/file_index/rangebitmap/dictionary/dictionary.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

class InputStream;
class MemoryPool;

class RangeBitmap {
 public:
    static constexpr int8_t VERSION = 1;

    static Result<std::unique_ptr<RangeBitmap>> Create(
        const std::shared_ptr<InputStream>& input_stream, int64_t offset, FieldType field_type,
        const std::shared_ptr<MemoryPool>& pool);

    Result<RoaringBitmap32> Eq(const Literal& key);
    Result<RoaringBitmap32> Neq(const Literal& key);
    Result<RoaringBitmap32> Lt(const Literal& key);
    Result<RoaringBitmap32> Lte(const Literal& key);
    Result<RoaringBitmap32> Gt(const Literal& key);
    Result<RoaringBitmap32> Gte(const Literal& key);
    Result<RoaringBitmap32> In(const std::vector<Literal>& keys);
    Result<RoaringBitmap32> NotIn(const std::vector<Literal>& keys);
    Result<RoaringBitmap32> IsNull();
    Result<RoaringBitmap32> IsNotNull();

 private:
    Result<RoaringBitmap32> Not(RoaringBitmap32& bitmap);

    RangeBitmap(const std::shared_ptr<MemoryPool>& pool, int32_t rid, int32_t cardinality,
                int32_t dictionary_offset, int32_t bsi_offset, const Literal& min,
                const Literal& max, const std::shared_ptr<KeyFactory>& key_factory,
                const std::shared_ptr<InputStream>& input_stream);
    Result<BitSliceIndexBitmap* const> GetBitSliceIndex();
    Result<Dictionary* const> GetDictionary();

 private:
    std::shared_ptr<MemoryPool> pool_;
    int32_t rid_;
    int32_t cardinality_;
    int32_t bsi_offset_;
    int32_t dictionary_offset_;
    Literal min_;
    Literal max_;
    std::shared_ptr<KeyFactory> key_factory_;
    std::shared_ptr<InputStream> input_stream_;

    // For lazy loading
    std::unique_ptr<BitSliceIndexBitmap> bsi_;
    std::unique_ptr<Dictionary> dictionary_;

 public:
    class Appender {
     public:
        Appender(const std::shared_ptr<MemoryPool>& pool,
                 const std::shared_ptr<KeyFactory>& factory,
                 int64_t limited_serialized_size_in_bytes);
        void Append(const Literal& key);
        Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize() const;

     private:
        struct LiteralComparator {
            bool operator()(const Literal& lhs, const Literal& rhs) const {
                const auto result = lhs.CompareTo(rhs);
                return result.ok() && result.value() < 0;
            }
        };
        std::shared_ptr<MemoryPool> pool_;
        int32_t rid_;
        std::map<Literal, RoaringBitmap32, LiteralComparator> bitmaps_;
        std::shared_ptr<KeyFactory> factory_;
        int64_t limited_serialized_size_in_bytes_;
    };
};

}  // namespace paimon
