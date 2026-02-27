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

#include "paimon/common/file_index/rangebitmap/dictionary/key_factory.h"

#include "fmt/format.h"
#include "paimon/common/file_index/rangebitmap/dictionary/chunked_dictionary.h"
#include "paimon/common/file_index/rangebitmap/dictionary/fixed_length_chunk.h"
#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"
#include "paimon/common/utils/field_type_utils.h"

namespace paimon {

Result<std::unique_ptr<KeyFactory>> KeyFactory::Create(const FieldType field_type) {
    // todo: support timestamp
    switch (field_type) {
        case FieldType::BOOLEAN:
            return std::make_unique<BooleanKeyFactory>();
        case FieldType::TINYINT:
            return std::make_unique<TinyIntKeyFactory>();
        case FieldType::SMALLINT:
            return std::make_unique<SmallIntKeyFactory>();
        case FieldType::DATE:
            return std::make_unique<DateKeyFactory>();
        case FieldType::INT:
            return std::make_unique<IntKeyFactory>();
        case FieldType::BIGINT:
            return std::make_unique<BigIntKeyFactory>();
        case FieldType::FLOAT:
            return std::make_unique<FloatKeyFactory>();
        case FieldType::DOUBLE:
            return std::make_unique<DoubleKeyFactory>();
        default:
            return Status::Invalid(fmt::format("Unsupported field type for KeyFactory: {}",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

const std::string& KeyFactory::GetDefaultChunkSize() {
    static const std::string kDefaultChunkSize = "16kb";
    return kDefaultChunkSize;
}

Result<std::unique_ptr<Chunk>> FixedLengthKeyFactory::CreateChunk(
    const std::shared_ptr<MemoryPool>& pool, const Literal& key, const int32_t code,
    const int32_t keys_length_limit) {
    return std::make_unique<FixedLengthChunk>(pool, key, code, keys_length_limit,
                                              this->shared_from_this(), this->GetFieldSize());
}

Result<std::unique_ptr<Chunk>> FixedLengthKeyFactory::MmapChunk(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<InputStream>& input_stream,
    const int32_t chunk_offest, const int32_t keys_base_offset) {
    PAIMON_RETURN_NOT_OK(input_stream->Seek(chunk_offest, FS_SEEK_SET));
    PAIMON_ASSIGN_OR_RAISE(const auto deserializer, this->CreateDeserializer());
    const auto data_in = std::make_shared<DataInputStream>(input_stream);
    PAIMON_ASSIGN_OR_RAISE(const auto version, data_in->ReadValue<int8_t>());
    if (version != ChunkedDictionary::CURRENT_VERSION) {
        return Status::Invalid(fmt::format("Unsupported version for KeyFactory: {}", version));
    }
    PAIMON_ASSIGN_OR_RAISE(const auto key_literal, deserializer(data_in, pool.get()));
    PAIMON_ASSIGN_OR_RAISE(const auto code, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto offset, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto size, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto keys_length, data_in->ReadValue<int32_t>());
    PAIMON_ASSIGN_OR_RAISE(const auto fixed_length, data_in->ReadValue<int32_t>());
    return std::make_unique<FixedLengthChunk>(pool, key_literal, code, offset, size,
                                              this->shared_from_this(), input_stream,
                                              keys_base_offset, keys_length, fixed_length);
}

Result<KeyFactory::KeySerializer> FixedLengthKeyFactory::CreateSerializer() {
    return KeySerializer([this](const std::shared_ptr<MemorySegmentOutputStream>& out,
                                const Literal& literal) -> Status {
        PAIMON_ASSIGN_OR_RAISE(const auto writer,
                               LiteralSerializationUtils::CreateValueWriter(GetFieldType(), out));
        return writer(literal);
    });
}

Result<KeyFactory::KeyDeserializer> FixedLengthKeyFactory::CreateDeserializer() {
    return KeyDeserializer([this](const std::shared_ptr<DataInputStream>& in,
                                  MemoryPool* pool) -> Result<Literal> {
        PAIMON_ASSIGN_OR_RAISE(
            auto reader, LiteralSerializationUtils::CreateValueReader(GetFieldType(), in, pool));
        return reader();
    });
}

Result<std::unique_ptr<Chunk>> VariableLengthKeyFactory::CreateChunk(
    const std::shared_ptr<MemoryPool>& pool, const Literal& key, int32_t code,
    int32_t keys_length_limit) {
    return Status::NotImplemented("VariableLengthKeyFactory::CreateChunk not implemented");
}
Result<std::unique_ptr<Chunk>> VariableLengthKeyFactory::MmapChunk(
    const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<InputStream>& input_stream,
    int32_t chunk_offest, int32_t keys_base_offset) {
    return Status::NotImplemented("VariableLengthKeyFactory::MmapChunk not implemented");
}

Result<KeyFactory::KeySerializer> VariableLengthKeyFactory::CreateSerializer() {
    return Status::NotImplemented("VariableLengthKeyFactory::CreateSerializer not implemented");
}

Result<KeyFactory::KeyDeserializer> VariableLengthKeyFactory::CreateDeserializer() {
    return Status::NotImplemented("VariableLengthKeyFactory::CreateDeserializer not implemented");
}

}  // namespace paimon
