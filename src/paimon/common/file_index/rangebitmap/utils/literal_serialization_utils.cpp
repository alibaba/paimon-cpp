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

#include "paimon/common/file_index/rangebitmap/utils/literal_serialization_utils.h"

#include "fmt/format.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/memory/bytes.h"

namespace paimon {

Result<LiteralSerDeUtils::Serializer> LiteralSerDeUtils::CreateValueWriter(
    const FieldType field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<bool>(literal.GetValue<bool>());
                    return Status::OK();
                });
        case FieldType::TINYINT:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<int8_t>(literal.GetValue<int8_t>());
                    return Status::OK();
                });
        case FieldType::SMALLINT:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<int16_t>(literal.GetValue<int16_t>());
                    return Status::OK();
                });
        case FieldType::DATE:
        case FieldType::INT:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<int32_t>(literal.GetValue<int32_t>());
                    return Status::OK();
                });
        case FieldType::BIGINT:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<int64_t>(literal.GetValue<int64_t>());
                    return Status::OK();
                });
        case FieldType::FLOAT:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<float>(literal.GetValue<float>());
                    return Status::OK();
                });
        case FieldType::DOUBLE:
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    output_stream->WriteValue<double>(literal.GetValue<double>());
                    return Status::OK();
                });
        case FieldType::STRING: {
            return LiteralSerDeUtils::Serializer(
                [](const std::shared_ptr<MemorySegmentOutputStream>& output_stream,
                   const Literal& literal) -> Status {
                    const auto value = literal.GetValue<std::string>();
                    output_stream->WriteValue<int32_t>(static_cast<int32_t>(value.size()));
                    output_stream->Write(value.data(), value.size());
                    return Status::OK();
                });
        }
        default:
            return Status::Invalid(
                fmt::format("Unsupported field type for literal serialization: {}",
                            FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

Result<LiteralSerDeUtils::Deserializer> LiteralSerDeUtils::CreateValueReader(FieldType field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(bool value, input_stream->ReadValue<bool>());
                    return Literal(value);
                });
        }
        case FieldType::TINYINT: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int8_t value, input_stream->ReadValue<int8_t>());
                    return Literal(value);
                });
        }
        case FieldType::SMALLINT: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int16_t value, input_stream->ReadValue<int16_t>());
                    return Literal(value);
                });
        }
        case FieldType::DATE: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int32_t value, input_stream->ReadValue<int32_t>());
                    return Literal(FieldType::DATE, value);
                });
        }
        case FieldType::INT: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int32_t value, input_stream->ReadValue<int32_t>());
                    return Literal(value);
                });
        }
        case FieldType::BIGINT: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int64_t value, input_stream->ReadValue<int64_t>());
                    return Literal(value);
                });
        }
        case FieldType::FLOAT: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(float value, input_stream->ReadValue<float>());
                    return Literal(value);
                });
        }
        case FieldType::DOUBLE: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(double value, input_stream->ReadValue<double>());
                    return Literal(value);
                });
        }
        case FieldType::STRING: {
            return LiteralSerDeUtils::Deserializer(
                [](const std::shared_ptr<DataInputStream>& input_stream,
                   MemoryPool* pool) -> Result<Literal> {
                    PAIMON_ASSIGN_OR_RAISE(int32_t length, input_stream->ReadValue<int32_t>());
                    auto bytes = Bytes::AllocateBytes(length, pool);
                    PAIMON_RETURN_NOT_OK(input_stream->ReadBytes(bytes.get()));
                    return Literal(FieldType::STRING, bytes->data(), bytes->size());
                });
        }
        default:
            return Status::Invalid(
                fmt::format("Unsupported field type for literal deserialization: {}",
                            FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

Result<int32_t> LiteralSerDeUtils::GetFixedFieldSize(const FieldType& field_type) {
    switch (field_type) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
            return sizeof(int8_t);
        case FieldType::SMALLINT:
            return sizeof(int16_t);
        case FieldType::DATE:
        case FieldType::INT:
            return sizeof(int32_t);
        case FieldType::BIGINT:
            return sizeof(int64_t);
        case FieldType::FLOAT:
            return sizeof(float);
        case FieldType::DOUBLE:
            return sizeof(double);
        default:
            return Status::Invalid(fmt::format("Unsupported field type for GetFixedFieldSize: {}",
                                               FieldTypeUtils::FieldTypeToString(field_type)));
    }
}

Result<int32_t> LiteralSerDeUtils::GetSerializedSizeInBytes(const Literal& literal) {
    switch (literal.GetType()) {
        case FieldType::BOOLEAN:
        case FieldType::TINYINT:
        case FieldType::SMALLINT:
        case FieldType::DATE:
        case FieldType::INT:
        case FieldType::BIGINT:
        case FieldType::DOUBLE:
        case FieldType::FLOAT:
            return GetFixedFieldSize(literal.GetType());
        case FieldType::STRING:
            return static_cast<int32_t>(sizeof(int32_t) + literal.GetValue<std::string>().size());
        default:
            return Status::Invalid(
                fmt::format("Unsupported field type for GetSerializedSizeInBytes: {}",
                            FieldTypeUtils::FieldTypeToString(literal.GetType())));
    }
}

}  // namespace paimon
