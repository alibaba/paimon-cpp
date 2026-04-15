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
#include "paimon/common/data/serializer/row_compacted_serializer.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/core/mergetree/lookup/lookup_serializer_factory.h"
namespace paimon {
/// A `LookupSerializerFactory` using `RowCompactedSerializer`.
class DefaultLookupSerializerFactory : public LookupSerializerFactory {
 public:
    static constexpr char kVersion[] = "v1";

    std::string Version() const override {
        return kVersion;
    }

    Result<SerializeFunc> CreateSerializer(const std::shared_ptr<arrow::Schema>& schema,
                                           const std::shared_ptr<MemoryPool>& pool) const override {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RowCompactedSerializer> serializer,
                               RowCompactedSerializer::Create(schema, pool));
        return LookupSerializerFactory::SerializeFunc(
            [serializer =
                 std::move(serializer)](const InternalRow& row) -> Result<std::shared_ptr<Bytes>> {
                return serializer->SerializeToBytes(row);
            });
    }

    Result<DeserializeFunc> CreateDeserializer(
        const std::string& file_ser_version, const std::shared_ptr<arrow::Schema>& current_schema,
        const std::shared_ptr<arrow::Schema>& file_schema,
        const std::shared_ptr<MemoryPool>& pool) const override {
        if (Version() != file_ser_version) {
            return Status::Invalid(fmt::format(
                "file_ser_version {} mismatch DefaultLookupSerializerFactory version {}",
                file_ser_version, Version()));
        }
        if (!ArrowUtils::EqualsIgnoreNullable(arrow::struct_(file_schema->fields()),
                                              arrow::struct_(current_schema->fields()))) {
            return Status::Invalid(
                fmt::format("current_schema {} must be equal with file_schema {} in "
                            "DefaultLookupSerializerFactory",
                            current_schema->ToString(), file_schema->ToString()));
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<RowCompactedSerializer> serializer,
                               RowCompactedSerializer::Create(current_schema, pool));
        return LookupSerializerFactory::DeserializeFunc(
            [serializer = std::move(serializer)](const std::shared_ptr<Bytes>& bytes)
                -> Result<std::unique_ptr<InternalRow>> { return serializer->Deserialize(bytes); });
    }
};
}  // namespace paimon
