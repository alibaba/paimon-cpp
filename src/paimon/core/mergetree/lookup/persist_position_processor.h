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
#include "paimon/common/utils/var_length_int_utils.h"
#include "paimon/core/mergetree/lookup/file_position.h"
#include "paimon/core/mergetree/lookup/lookup_serializer_factory.h"
#include "paimon/core/mergetree/lookup/persist_processor.h"

namespace paimon {
/// A `PersistProcessor` to return `FilePosition`.
class PersistPositionProcessor : public PersistProcessor<FilePosition> {
 public:
    bool WithPosition() const override {
        return true;
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv) const override {
        return Status::Invalid(
            "invalid operation, do not support persist to disk without position in "
            "PersistPositionProcessor");
    }

    Result<std::shared_ptr<Bytes>> PersistToDisk(const KeyValue& kv,
                                                 int64_t row_position) const override {
        auto bytes = std::make_shared<Bytes>(VarLengthIntUtils::kMaxVarLongSize, pool_.get());
        PAIMON_ASSIGN_OR_RAISE(int64_t len,
                               VarLengthIntUtils::EncodeLong(row_position, bytes.get()));
        std::shared_ptr<Bytes> copy_bytes = Bytes::CopyOf(*bytes, len, pool_.get());
        return copy_bytes;
    }

    Result<FilePosition> ReadFromDisk(std::shared_ptr<InternalRow> key, int32_t level,
                                      const std::shared_ptr<Bytes>& value_bytes,
                                      const std::string& file_name) const override {
        PAIMON_ASSIGN_OR_RAISE(int64_t row_position,
                               VarLengthIntUtils::DecodeLong(value_bytes.get(), /*index=*/0));
        return FilePosition{file_name, row_position};
    }

    /// Factory to create `PersistProcessor`.
    class Factory : public PersistProcessor<FilePosition>::Factory {
     public:
        std::string Identifier() const override {
            return "position";
        }

        Result<std::unique_ptr<PersistProcessor<FilePosition>>> Create(
            const std::string& file_ser_version,
            const std::shared_ptr<LookupSerializerFactory>& serializer_factory,
            const std::shared_ptr<arrow::Schema>& file_schema,
            const std::shared_ptr<MemoryPool>& pool) const override {
            return std::unique_ptr<PersistPositionProcessor>(new PersistPositionProcessor(pool));
        }
    };

 private:
    explicit PersistPositionProcessor(const std::shared_ptr<MemoryPool>& pool) : pool_(pool) {}

 private:
    std::shared_ptr<MemoryPool> pool_;
};
}  // namespace paimon
