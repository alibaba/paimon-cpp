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

#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"

#include <cmath>

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"
#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/core/utils/fields_comparator.h"

namespace paimon {

Result<std::shared_ptr<KeyFactory>> KeyFactory::Create(FieldType field_type) {
    // todo: support timestamp
    switch (field_type) {
        case FieldType::BOOLEAN:
            return std::make_shared<BooleanKeyFactory>();
        case FieldType::TINYINT:
            return std::make_shared<TinyIntKeyFactory>();
        case FieldType::SMALLINT:
            return std::make_shared<SmallIntKeyFactory>();
        case FieldType::DATE:
            return std::make_shared<DateKeyFactory>();
        case FieldType::INT:
            return std::make_shared<IntKeyFactory>();
        case FieldType::BIGINT:
            return std::make_shared<BigIntKeyFactory>();
        case FieldType::FLOAT:
            return std::make_shared<FloatKeyFactory>();
        case FieldType::DOUBLE:
            return std::make_shared<DoubleKeyFactory>();
        default:
            return Status::Invalid(fmt::format("Unsupported field type for KeyFactory: {}",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

Result<std::unique_ptr<Chunk>> FixedLengthKeyFactory::CreateChunk(
    const Literal& key, int32_t code, int32_t keys_length_limit,
    const std::shared_ptr<MemoryPool>& pool) {
    auto keys_output_stream = std::make_shared<MemorySegmentOutputStream>(
        MemorySegmentOutputStream::DEFAULT_SEGMENT_SIZE, pool);
    PAIMON_ASSIGN_OR_RAISE(auto serializer,
                           LiteralSerDeUtils::CreateValueWriter(this->GetFieldType()));
    return std::make_unique<FixedLengthChunk>(key, code, keys_length_limit,
                                              this->shared_from_this(), this->GetFieldSize(),
                                              keys_output_stream, serializer, pool);
}

Result<std::unique_ptr<Chunk>> FixedLengthKeyFactory::MmapChunk(
    const std::shared_ptr<InputStream>& input_stream, int32_t chunk_offset,
    int32_t keys_base_offset, const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(chunk_offset, FS_SEEK_SET));
    PAIMON_ASSIGN_OR_RAISE(LiteralSerDeUtils::Deserializer deserializer,
                           LiteralSerDeUtils::CreateValueReader(GetFieldType()));
    const auto data_in = std::make_shared<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(int8_t version, data_in->ReadValue<int8_t>());
    if (version != FixedLengthChunk::kCurrentVersion) {
        return Status::Invalid(fmt::format("Unsupported version for KeyFactory: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(Literal key_literal, deserializer(data_in, pool.get()));
    PAIMON_ASSIGN_OR_RAISE(int32_t code, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t offset, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t keys_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(int32_t fixed_length, data_in->ReadValue<int32_t>());
    return std::make_unique<FixedLengthChunk>(key_literal, code, offset, size,
                                              this->shared_from_this(), input_stream,
                                              keys_base_offset, keys_length, fixed_length, pool);
}

Result<std::unique_ptr<Chunk>> VariableLengthKeyFactory::CreateChunk(
    const Literal& key, int32_t code, int32_t keys_length_limit,
    const std::shared_ptr<MemoryPool>& pool) {
    return Status::NotImplemented("VariableLengthKeyFactory::CreateChunk not implemented");
}
Result<std::unique_ptr<Chunk>> VariableLengthKeyFactory::MmapChunk(
    const std::shared_ptr<InputStream>& input_stream, int32_t chunk_offset,
    int32_t keys_base_offset, const std::shared_ptr<MemoryPool>& pool) {
    return Status::NotImplemented("VariableLengthKeyFactory::MmapChunk not implemented");
}

/// Java-compatible ordering for floats
Result<int32_t> FloatKeyFactory::CompareLiteral(const Literal& lhs, const Literal& rhs) const {
    const auto a = lhs.GetValue<float>();
    const auto b = rhs.GetValue<float>();
    return FieldsComparator::CompareFloatingPoint(a, b);
}

/// Java-compatible ordering for doubles
Result<int32_t> DoubleKeyFactory::CompareLiteral(const Literal& lhs, const Literal& rhs) const {
    const auto a = lhs.GetValue<double>();
    const auto b = rhs.GetValue<double>();
    return FieldsComparator::CompareFloatingPoint(a, b);
}

}  // namespace paimon
