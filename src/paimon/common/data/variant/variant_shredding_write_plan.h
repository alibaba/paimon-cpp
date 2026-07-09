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

#include <map>
#include <memory>
#include <string>

#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {

/// A physical write plan for variant shredding: maps the logical write schema (with variant
/// columns) to the physical schema where planned variant columns are replaced by their shredded
/// struct representation.
class VariantShreddingWritePlan {
 public:
    /// Creates a plan shredding the given top-level variant columns.
    ///
    /// @param logical_schema The logical write schema.
    /// @param column_shredding_types The shredding type per variant column name, e.g.
    ///        `{"v": struct{a: int32, b: string}}`. Names that are not top-level variant columns
    ///        of `logical_schema` are ignored.
    static Result<std::shared_ptr<VariantShreddingWritePlan>> Create(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<std::string, std::shared_ptr<arrow::DataType>>& column_shredding_types);

    /// Creates a plan from the `variant.shreddingSchema` option value: a ROW type JSON whose
    /// fields map variant column names to their shredding types.
    static Result<std::shared_ptr<VariantShreddingWritePlan>> FromConfiguredSchema(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::string& configured_schema_json);

    const std::shared_ptr<arrow::Schema>& LogicalSchema() const {
        return logical_schema_;
    }

    const std::shared_ptr<arrow::Schema>& PhysicalSchema() const {
        return physical_schema_;
    }

    /// The shredding schema per planned variant column name.
    const std::map<std::string, std::shared_ptr<VariantSchema>>& ColumnSchemas() const {
        return column_schemas_;
    }

    /// The physical shredded struct type per planned variant column name.
    const std::map<std::string, std::shared_ptr<arrow::DataType>>& ColumnPhysicalTypes() const {
        return column_physical_types_;
    }

 private:
    VariantShreddingWritePlan() = default;

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<arrow::Schema> physical_schema_;
    std::map<std::string, std::shared_ptr<VariantSchema>> column_schemas_;
    std::map<std::string, std::shared_ptr<arrow::DataType>> column_physical_types_;
};

}  // namespace paimon
