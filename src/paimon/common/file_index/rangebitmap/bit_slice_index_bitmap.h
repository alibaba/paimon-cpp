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

#include <cstdint>
#include <memory>
#include <optional>

#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/bytes.h"
#include "paimon/result.h"
#include "paimon/status.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon {

class BitSliceIndexBitmap {
 public:
    static constexpr int CURRENT_VERSION = 1;
    static Result<std::unique_ptr<BitSliceIndexBitmap>> Create(
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<InputStream>& input_stream,
        int32_t offset);

    BitSliceIndexBitmap(const std::shared_ptr<MemoryPool>& pool, int32_t indexes_length,
                        PAIMON_UNIQUE_PTR<Bytes> indexes, int32_t ebm_length, int32_t slices_size,
                        const std::shared_ptr<InputStream>& input_stream, int32_t body_offset);

    Result<const RoaringBitmap32*> GetEmptyBitmap();

    Result<const RoaringBitmap32*> GetSliceBitmap(int32_t idx);

    Status LoadSlices(int32_t start, int32_t end);

    Result<RoaringBitmap32> Eq(int32_t code);

    Result<RoaringBitmap32> Gt(int32_t code);

    Result<RoaringBitmap32> Gte(int32_t code);

    Result<RoaringBitmap32> IsNotNull(const RoaringBitmap32& found_set);

    class Appender {
     public:
        Appender(const std::shared_ptr<MemoryPool>& pool, int32_t min, int32_t max);
        Status Append(int32_t key, int32_t value);
        Result<PAIMON_UNIQUE_PTR<Bytes>> Serialize() const;

     private:
        std::shared_ptr<MemoryPool> pool_;
        int32_t min_;
        int32_t max_;
        RoaringBitmap32 ebm_;
        std::vector<RoaringBitmap32> slices_;
    };

 private:
    std::shared_ptr<MemoryPool> pool_;
    bool initialized_;
    std::vector<std::optional<RoaringBitmap32>> bit_slices_;
    std::optional<RoaringBitmap32> ebm;
    std::shared_ptr<InputStream> input_stream_;
    int32_t body_offset_;
    PAIMON_UNIQUE_PTR<Bytes> indexes_;
    int32_t ebm_length_;
    int32_t indexes_length_;
};

}  // namespace paimon
