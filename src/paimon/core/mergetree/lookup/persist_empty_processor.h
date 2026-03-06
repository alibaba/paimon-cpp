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
/// A `PersistProcessor` to return `bool` only.
class PersistEmptyProcessor : public PersistProcessor<bool> {
 public:
    bool WithPosition() const override {
        return false;
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv) const override {
        return Bytes::EmptyBytes();
    }

    Result<bool> ReadFromDisk(std::shared_ptr<InternalRow> key, int32_t level,
                              const std::shared_ptr<Bytes>& value_bytes,
                              const std::string& file_name) const override {
        return true;
    }

    /// Factory to create `PersistProcessor`.
    class Factory : public PersistProcessor<bool>::Factory {
     public:
        std::string Identifier() const override {
            return "empty";
        }

        Result<std::unique_ptr<PersistProcessor<bool>>> Create(
            const std::string& file_ser_version,
            const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
            const std::shared_ptr<arrow::Schema>& file_schema,
            const std::shared_ptr<MemoryPool>& pool) const override {
            return std::unique_ptr<PersistEmptyProcessor>(new PersistEmptyProcessor());
        }
    };

 private:
    PersistEmptyProcessor() = default;
};
}  // namespace paimon
