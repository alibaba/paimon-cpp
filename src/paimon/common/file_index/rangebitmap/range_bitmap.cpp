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

#include "paimon/common/file_index/rangebitmap/range_bitmap.h"

#include <algorithm>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"
#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/common/io/data_output_stream.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/io/data_input_stream.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Result<std::unique_ptr<RangeBitmap>> RangeBitmap::Create(
    const std::shared_ptr<InputStream>& input_stream, const int64_t offset,
    const FieldType field_type, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(offset, SeekOrigin::FS_SEEK_SET));
    const auto data_in = std::make_shared<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int32_t header_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
    if (version != VERSION) {
        return Status::Invalid(
            fmt::format("RangeBitmap unsupported version {} (expected {})", version, VERSION));
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t rid, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t cardinality, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<KeyFactory> shared_key_factory,
                           KeyFactory::Create(field_type));
    PAIMON_ASSIGN_OR_RAISE(LiteralSerDeUtils::Deserializer key_deserializer,
                           LiteralSerDeUtils::CreateValueReader(field_type));
    PAIMON_ASSIGN_OR_RAISE(Literal min, key_deserializer(data_in, pool.get()));
    PAIMON_ASSIGN_OR_RAISE(Literal max, key_deserializer(data_in, pool.get()));
    PAIMON_ASSIGN_OR_RAISE(int32_t dictionary_length, data_in->ReadValue<int32_t>());
    const auto dictionary_offset = static_cast<int32_t>(offset + sizeof(int32_t) + header_length);
    const auto bsi_offset = dictionary_offset + dictionary_length;
    return std::unique_ptr<RangeBitmap>(new RangeBitmap(pool, rid, cardinality, dictionary_offset,
                                                        bsi_offset, min, max, shared_key_factory,
                                                        input_stream));
}

Result<RoaringBitmap32> RangeBitmap::Not(RoaringBitmap32& bitmap) {
    bitmap.Flip(0, rid_);
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 is_not_null, this->IsNotNull());
    return bitmap &= is_not_null;
}

Result<RoaringBitmap32> RangeBitmap::Eq(const Literal& key) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t min_compare, key.CompareTo(min_));
    PAIMON_ASSIGN_OR_RAISE(int32_t max_compare, key.CompareTo(max_));
    PAIMON_ASSIGN_OR_RAISE(BitSliceIndexBitmap* const bit_slice_ptr, this->GetBitSliceIndex());
    if (min_compare == 0 && max_compare == 0) {
        return bit_slice_ptr->IsNotNull({});
    }
    if (min_compare < 0 || max_compare > 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(Dictionary* const dictionary, this->GetDictionary());
    PAIMON_ASSIGN_OR_RAISE(int32_t code, dictionary->Find(key));
    if (code < 0) {
        return RoaringBitmap32();
    }
    return bit_slice_ptr->Eq(code);
}

Result<RoaringBitmap32> RangeBitmap::Neq(const Literal& key) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 eq_result, Eq(key));
    return Not(eq_result);
}

Result<RoaringBitmap32> RangeBitmap::Lt(const Literal& key) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t min_compare, key.CompareTo(min_));
    PAIMON_ASSIGN_OR_RAISE(int32_t max_compare, key.CompareTo(max_));
    if (max_compare > 0) {
        return IsNotNull();
    }
    if (min_compare <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 gte_result, Gte(key));
    return Not(gte_result);
}

Result<RoaringBitmap32> RangeBitmap::Lte(const Literal& key) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t min_compare, key.CompareTo(min_));
    PAIMON_ASSIGN_OR_RAISE(int32_t max_compare, key.CompareTo(max_));
    if (max_compare >= 0) {
        return IsNotNull();
    }
    if (min_compare < 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 gt_result, Gt(key));
    return Not(gt_result);
}

Result<RoaringBitmap32> RangeBitmap::Gt(const Literal& key) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t max_compare, key.CompareTo(max_));
    if (max_compare >= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t min_compare, key.CompareTo(min_));
    if (min_compare < 0) {
        return IsNotNull();
    }
    PAIMON_ASSIGN_OR_RAISE(Dictionary* const dictionary, this->GetDictionary());
    PAIMON_ASSIGN_OR_RAISE(int32_t code, dictionary->Find(key));
    PAIMON_ASSIGN_OR_RAISE(BitSliceIndexBitmap* const bit_slice_ptr, this->GetBitSliceIndex());
    if (code >= 0) {
        return bit_slice_ptr->Gt(code);
    }
    return bit_slice_ptr->Gte(-code - 1);
}

Result<RoaringBitmap32> RangeBitmap::Gte(const Literal& key) {
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 gt_result, Gt(key));
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 eq_result, Eq(key));
    gt_result |= eq_result;
    return gt_result;
}

Result<RoaringBitmap32> RangeBitmap::In(const std::vector<Literal>& keys) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    RoaringBitmap32 result{};
    for (const auto& key : keys) {
        PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 bitmap, Eq(key));
        result |= bitmap;
    }
    return result;
}

Result<RoaringBitmap32> RangeBitmap::NotIn(const std::vector<Literal>& keys) {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 in_result, In(keys));
    return Not(in_result);
}

Result<RoaringBitmap32> RangeBitmap::IsNull() {
    if (cardinality_ <= 0) {
        if (rid_ > 0) {
            RoaringBitmap32 result;
            result.AddRange(0, rid_);
            return result;
        }
        return RoaringBitmap32();
    }

    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 non_null_bitmap, IsNotNull());
    non_null_bitmap.Flip(0, rid_);
    return non_null_bitmap;
}

Result<RoaringBitmap32> RangeBitmap::IsNotNull() {
    if (cardinality_ <= 0) {
        return RoaringBitmap32();
    }

    PAIMON_ASSIGN_OR_RAISE(BitSliceIndexBitmap* const bit_slice_ptr, this->GetBitSliceIndex());
    PAIMON_ASSIGN_OR_RAISE(RoaringBitmap32 result, bit_slice_ptr->IsNotNull({}));
    return result;
}

RangeBitmap::RangeBitmap(const std::shared_ptr<MemoryPool>& pool, const int32_t rid,
                         const int32_t cardinality, const int32_t dictionary_offset,
                         const int32_t bsi_offset, const Literal& min, const Literal& max,
                         const std::shared_ptr<KeyFactory>& key_factory,
                         const std::shared_ptr<InputStream>& input_stream)
    : pool_(pool),
      rid_(rid),
      cardinality_(cardinality),
      bsi_offset_(bsi_offset),
      dictionary_offset_(dictionary_offset),
      min_(min),
      max_(max),
      key_factory_(key_factory),
      input_stream_(input_stream),
      bsi_(nullptr),
      dictionary_(nullptr) {}

Result<BitSliceIndexBitmap* const> RangeBitmap::GetBitSliceIndex() {
    if (bsi_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(bsi_,
                               BitSliceIndexBitmap::Create(pool_, input_stream_, bsi_offset_));
    }
    return bsi_.get();
}

Result<Dictionary* const> RangeBitmap::GetDictionary() {
    if (dictionary_ == nullptr) {
        PAIMON_ASSIGN_OR_RAISE(dictionary_,
                               ChunkedDictionary::Create(pool_, key_factory_->GetFieldType(),
                                                         input_stream_, dictionary_offset_));
    }
    return dictionary_.get();
}

RangeBitmap::Appender::Appender(const std::shared_ptr<MemoryPool>& pool,
                                const std::shared_ptr<KeyFactory>& factory,
                                const int64_t limited_serialized_size_in_bytes)
    : pool_(pool),
      rid_(0),
      factory_(factory),
      limited_serialized_size_in_bytes_(limited_serialized_size_in_bytes) {}

void RangeBitmap::Appender::Append(const Literal& key) {
    if (!key.IsNull()) {
        bitmaps_[key].Add(rid_);
    }
    rid_++;
}

Result<PAIMON_UNIQUE_PTR<Bytes>> RangeBitmap::Appender::Serialize() const {
    int32_t code = 0;
    auto bsi = BitSliceIndexBitmap::Appender(pool_, 0, static_cast<int32_t>(bitmaps_.size() - 1));
    auto dictionary = ChunkedDictionary::Appender(
        pool_, factory_, static_cast<int32_t>(limited_serialized_size_in_bytes_));
    for (const auto& [key, bitmap] : bitmaps_) {
        PAIMON_RETURN_NOT_OK(dictionary.AppendSorted(key, code));
        for (auto it = bitmap.Begin(); it != bitmap.End(); ++it) {
            PAIMON_RETURN_NOT_OK(bsi.Append(*it, code));
        }
        code++;
    }
    PAIMON_ASSIGN_OR_RAISE(LiteralSerDeUtils::Serializer serializer,
                           LiteralSerDeUtils::CreateValueWriter(factory_->GetFieldType()));
    auto min = Literal{factory_->GetFieldType()};
    auto max = Literal{factory_->GetFieldType()};
    if (!bitmaps_.empty()) {
        min = bitmaps_.begin()->first;
        max = bitmaps_.rbegin()->first;
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t min_size, LiteralSerDeUtils::GetSerializedSizeInBytes(min));
    PAIMON_ASSIGN_OR_RAISE(int32_t max_size, LiteralSerDeUtils::GetSerializedSizeInBytes(max));
    int32_t header_size = 0;
    header_size += sizeof(int8_t);               // version
    header_size += sizeof(int32_t);              // rid
    header_size += sizeof(int32_t);              // cardinality
    header_size += min.IsNull() ? 0 : min_size;  // min literal size
    header_size += max.IsNull() ? 0 : max_size;  // max literal size
    header_size += sizeof(int32_t);              // dictionary length
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> dictionary_bytes, dictionary.Serialize());
    const auto dictionary_length = static_cast<int32_t>(dictionary_bytes->size());
    PAIMON_ASSIGN_OR_RAISE(PAIMON_UNIQUE_PTR<Bytes> bsi_bytes, bsi.Serialize());
    const auto bsi_length = bsi_bytes->size();
    const auto data_output_stream = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool_);
    data_output_stream->WriteValue<int32_t>(header_size);
    data_output_stream->WriteValue<int8_t>(VERSION);
    data_output_stream->WriteValue<int32_t>(rid_);
    data_output_stream->WriteValue<int32_t>(static_cast<int32_t>(bitmaps_.size()));
    if (!min.IsNull()) {
        PAIMON_RETURN_NOT_OK(serializer(data_output_stream, min));
    }
    if (!max.IsNull()) {
        PAIMON_RETURN_NOT_OK(serializer(data_output_stream, max));
    }
    data_output_stream->WriteValue<int32_t>(dictionary_length);
    data_output_stream->Write(dictionary_bytes->data(), dictionary_length);
    data_output_stream->Write(bsi_bytes->data(), bsi_length);
    return MemorySegmentUtils::CopyToBytes(data_output_stream->Segments(), 0,
                                           static_cast<int32_t>(data_output_stream->CurrentSize()),
                                           pool_.get());
}
}  // namespace paimon
