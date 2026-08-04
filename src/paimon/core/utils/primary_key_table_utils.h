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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "paimon/result.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {

class MergeFunction;
class CoreOptions;
class MemoryPool;
class FieldsComparator;
class DataField;

class PrimaryKeyTableUtils {
 public:
    PrimaryKeyTableUtils() = delete;
    ~PrimaryKeyTableUtils() = delete;

    /// Create the merge engine configured for a primary key table.
    ///
    /// @param value_schema Schema of the value part of a KeyValue.
    /// @param primary_keys Primary key field names.
    /// @param options Table options selecting the merge engine.
    /// @param pool Pool the merge engine charges its allocations to. Aggregating merge engines
    /// allocate per merged value, so the caller's pool must be threaded through.
    /// @return The merge function, or an error Status for an unsupported merge engine.
    static Result<std::unique_ptr<MergeFunction>> CreateMergeFunction(
        const std::shared_ptr<arrow::Schema>& value_schema,
        const std::vector<std::string>& primary_keys, const CoreOptions& options,
        const std::shared_ptr<MemoryPool>& pool);

    static Result<std::unique_ptr<FieldsComparator>> CreateSequenceFieldsComparator(
        const std::vector<DataField>& value_fields, const CoreOptions& options);
};

}  // namespace paimon
