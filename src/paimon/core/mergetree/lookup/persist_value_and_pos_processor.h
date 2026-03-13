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
#include "paimon/core/mergetree/lookup/lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_processor.h"
#include "paimon/core/mergetree/lookup/persist_value_processor.h"
#include "paimon/core/mergetree/lookup/positioned_key_value.h"
namespace paimon {
/// A `PersistProcessor` to return `PositionedKeyValue`.
class PersistValueAndPosProcessor : public PersistProcessor<PositionedKeyValue> {
 public:
    static constexpr size_t kMetaLen = sizeof(int64_t) + sizeof(int64_t) + sizeof(int8_t);

    bool WithPosition() const override {
        return true;
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv) const override {
        return Status::Invalid(
            "invalid operation, do not support persist to disk without position in "
            "PersistValueAndPosProcessor");
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv,
                                                 int64_t row_position) const override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> vbytes, serializer_(*(kv.value)));
        auto bytes = std::make_shared<Bytes>(vbytes->size() + kMetaLen, pool_.get());
        auto segment = MemorySegment::Wrap(bytes);
        segment.Put(0, *vbytes);
        segment.PutValue<int64_t>(bytes->size() - kMetaLen, row_position);
        segment.PutValue<int64_t>(bytes->size() - PersistValueProcessor::kMetaLen,
                                  kv.sequence_number);
        segment.Put(bytes->size() - 1, static_cast<char>(kv.value_kind->ToByteValue()));
        return bytes;
    }

    Result<PositionedKeyValue> ReadFromDisk(std::shared_ptr<InternalRow> key, int32_t level,
                                            const std::shared_ptr<Bytes>& value_bytes,
                                            const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InternalRow> value, deserializer_(value_bytes));
        auto memory_segment = MemorySegment::Wrap(value_bytes);
        auto row_position = memory_segment.GetValue<int64_t>(value_bytes->size() - kMetaLen);
        auto sequence_number =
            memory_segment.GetValue<int64_t>(value_bytes->size() - PersistValueProcessor::kMetaLen);
        PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                               RowKind::FromByteValue((*value_bytes)[value_bytes->size() - 1]));
        return PositionedKeyValue{
            KeyValue(row_kind, sequence_number, level, std::move(key), std::move(value)), file_name,
            row_position};
    }

    /// Factory to create `PersistProcessor`.
    class Factory : public PersistProcessor<PositionedKeyValue>::Factory {
     public:
        explicit Factory(const std::shared_ptr<arrow::Schema>& current_schema)
            : current_schema_(current_schema) {}

        std::string Identifier() const override {
            return "position-and-value";
        }

        Result<std::unique_ptr<PersistProcessor<PositionedKeyValue>>> Create(
            const std::string& file_ser_version,
            const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
            const std::shared_ptr<arrow::Schema>& file_schema,
            const std::shared_ptr<MemoryPool>& pool) const override {
            PAIMON_ASSIGN_OR_RAISE(LookupSerializerFactory::SerializeFunc serializer,
                                   serializer_factory->CreateSerializer(current_schema_, pool));
            PAIMON_ASSIGN_OR_RAISE(LookupSerializerFactory::DeserializeFunc deserializer,
                                   serializer_factory->CreateDeserializer(
                                       file_ser_version, current_schema_, file_schema, pool));
            return std::unique_ptr<PersistValueAndPosProcessor>(new PersistValueAndPosProcessor(
                std::move(serializer), std::move(deserializer), pool));
        }

     private:
        std::shared_ptr<arrow::Schema> current_schema_;
    };

 private:
    PersistValueAndPosProcessor(LookupSerializerFactory::SerializeFunc serializer,
                                LookupSerializerFactory::DeserializeFunc deserializer,
                                const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), serializer_(std::move(serializer)), deserializer_(std::move(deserializer)) {}

 private:
    std::shared_ptr<MemoryPool> pool_;
    LookupSerializerFactory::SerializeFunc serializer_;
    LookupSerializerFactory::DeserializeFunc deserializer_;
};
}  // namespace paimon
