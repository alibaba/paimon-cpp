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
namespace paimon {
/// A `PersistProcessor` to return `KeyValue`.
class PersistValueProcessor : public PersistProcessor<KeyValue> {
 public:
    static constexpr size_t kMetaLen = sizeof(int64_t) + sizeof(int8_t);

    bool WithPosition() const override {
        return false;
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv) const override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<Bytes> vbytes, serializer_(*(kv.value)));
        auto bytes = std::make_shared<Bytes>(vbytes->size() + kMetaLen, pool_.get());
        auto segment = MemorySegment::Wrap(bytes);
        segment.Put(0, *vbytes);
        segment.PutValue<int64_t>(bytes->size() - kMetaLen, kv.sequence_number);
        segment.Put(bytes->size() - 1, static_cast<char>(kv.value_kind->ToByteValue()));
        return bytes;
    }

    Result<KeyValue> ReadFromDisk(std::shared_ptr<InternalRow> key, int32_t level,
                                  const std::shared_ptr<Bytes>& value_bytes,
                                  const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<InternalRow> value, deserializer_(value_bytes));
        auto sequence_number =
            MemorySegment::Wrap(value_bytes).GetValue<int64_t>(value_bytes->size() - kMetaLen);
        PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                               RowKind::FromByteValue((*value_bytes)[value_bytes->size() - 1]));
        return KeyValue(row_kind, sequence_number, level, std::move(key), std::move(value));
    }

    /// Factory to create `PersistProcessor`.
    class Factory : public PersistProcessor<KeyValue>::Factory {
     public:
        explicit Factory(const std::shared_ptr<arrow::Schema>& current_schema)
            : current_schema_(current_schema) {}

        std::string Identifier() const override {
            return "value";
        }

        Result<std::unique_ptr<PersistProcessor<KeyValue>>> Create(
            const std::string& file_ser_version,
            const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
            const std::shared_ptr<arrow::Schema>& file_schema,
            const std::shared_ptr<MemoryPool>& pool) const override {
            PAIMON_ASSIGN_OR_RAISE(LookupSerializerFactory::SerializeFunc serializer,
                                   serializer_factory->CreateSerializer(current_schema_, pool));
            PAIMON_ASSIGN_OR_RAISE(LookupSerializerFactory::DeserializeFunc deserializer,
                                   serializer_factory->CreateDeserializer(
                                       file_ser_version, current_schema_, file_schema, pool));
            return std::unique_ptr<PersistValueProcessor>(
                new PersistValueProcessor(std::move(serializer), std::move(deserializer), pool));
        }

     private:
        std::shared_ptr<arrow::Schema> current_schema_;
    };

 private:
    PersistValueProcessor(LookupSerializerFactory::SerializeFunc serializer,
                          LookupSerializerFactory::DeserializeFunc deserializer,
                          const std::shared_ptr<MemoryPool>& pool)
        : pool_(pool), serializer_(std::move(serializer)), deserializer_(std::move(deserializer)) {}

 private:
    std::shared_ptr<MemoryPool> pool_;
    LookupSerializerFactory::SerializeFunc serializer_;
    LookupSerializerFactory::DeserializeFunc deserializer_;
};
}  // namespace paimon
