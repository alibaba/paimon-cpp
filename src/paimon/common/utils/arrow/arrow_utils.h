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

#include <vector>

#include "arrow/api.h"
#include "arrow/util/type_fwd.h"
#include "fmt/format.h"
#include "paimon/result.h"

namespace paimon {

class PAIMON_EXPORT ArrowUtils {
 public:
    ArrowUtils() = delete;
    ~ArrowUtils() = delete;

    static Result<std::shared_ptr<arrow::Schema>> DataTypeToSchema(
        const std::shared_ptr<arrow::DataType>& data_type);

    static Result<std::vector<int32_t>> CreateProjection(
        const std::shared_ptr<arrow::Schema>& src_schema, const arrow::FieldVector& target_fields);

    static Status CheckNullabilityMatch(const std::shared_ptr<arrow::Schema>& schema,
                                        const std::shared_ptr<arrow::Array>& data);

    // For struct array, arrow is unsafe for fields() and field(); for dict array, arrow is unsafe
    // for dictionary(). Therefore, access array in advance before merge sort and projection to
    // avoid subsequent multi-threading problems.
    static void TraverseArray(const std::shared_ptr<arrow::Array>& array);

    static Result<std::shared_ptr<arrow::StructArray>> RemoveFieldFromStructArray(
        const std::shared_ptr<arrow::StructArray>& struct_array, const std::string& field_name);

    static bool EqualsIgnoreNullable(const std::shared_ptr<arrow::DataType>& type,
                                     const std::shared_ptr<arrow::DataType>& other_type);

    /// Normalize and resolve a compression string to an Arrow compression type.
    /// Handles "none" and empty string by mapping them to "uncompressed".
    static Result<arrow::Compression::type> GetCompressionType(const std::string& compression);

 private:
    static Status InnerCheckNullabilityMatch(const std::shared_ptr<arrow::Field>& field,
                                             const std::shared_ptr<arrow::Array>& data);
};

}  // namespace paimon
