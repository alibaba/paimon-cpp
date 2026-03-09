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
#include "paimon/core/key_value.h"
#include "paimon/memory/bytes.h"
namespace paimon {
class LookupSerializerFactory;
/// Processor to process value.
template <typename T>
class PersistProcessor {
 public:
    virtual ~PersistProcessor() = default;

    virtual bool WithPosition() const = 0;

    virtual Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv) const = 0;

    virtual Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv,
                                                         int64_t row_position) const {
        return Status::Invalid("Not support for PersistToDisk with position");
    }

    virtual Result<T> ReadFromDisk(std::shared_ptr<InternalRow> key, int32_t level,
                                   const std::shared_ptr<Bytes>& value_bytes,
                                   const std::string& file_name) const = 0;

    /// Factory to create `PersistProcessor`.
    class Factory {
     public:
        virtual ~Factory() = default;
        virtual std::string Identifier() const = 0;

        virtual Result<std::unique_ptr<PersistProcessor<T>>> Create(
            const std::string& file_ser_version,
            const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
            const std::shared_ptr<arrow::Schema>& file_schema,
            const std::shared_ptr<MemoryPool>& pool) const = 0;
    };
};
}  // namespace paimon
