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
#include <functional>

#include "arrow/api.h"
#include "paimon/common/data/internal_row.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"
namespace paimon {
/// Factory to create serializer for lookup.
class LookupSerializerFactory {
 public:
    virtual ~LookupSerializerFactory() = default;

    virtual std::string Version() const = 0;

    using SerializeFunc = std::function<Result<std::shared_ptr<Bytes>>(const InternalRow&)>;
    using DeserializeFunc =
        std::function<Result<std::unique_ptr<InternalRow>>(const std::shared_ptr<Bytes>&)>;

    virtual Result<SerializeFunc> CreateSerializer(
        const std::shared_ptr<arrow::Schema>& schema,
        const std::shared_ptr<MemoryPool>& pool) const = 0;

    virtual Result<DeserializeFunc> CreateDeserializer(
        const std::string& file_ser_version, const std::shared_ptr<arrow::Schema>& current_schema,
        const std::shared_ptr<arrow::Schema>& file_schema,
        const std::shared_ptr<MemoryPool>& pool) const = 0;
};
}  // namespace paimon
