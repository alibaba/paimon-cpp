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
#include <vector>

#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class Schema;
}  // namespace arrow

namespace paimon {

/// A physical write plan for variant shredding: maps the logical write schema (with variant
/// columns) to the physical schema where planned variant columns are replaced by their shredded
/// struct representation. Variant columns may be at the top level or nested inside ROW (struct)
/// columns; variants nested inside arrays or maps are never shredded (as in Java).
class VariantShreddingWritePlan {
 public:
    /// One planned variant column, identified by its field-index path from the schema root,
    /// descending only through struct fields (e.g. `{1, 2}` is the third field of the second
    /// top-level column).
    struct PlannedColumn {
        std::vector<int32_t> path;
        std::shared_ptr<VariantSchema> variant_schema;
        std::shared_ptr<arrow::DataType> physical_type;
    };

    /// Creates a plan shredding the given top-level variant columns.
    ///
    /// @param logical_schema The logical write schema.
    /// @param column_shredding_types The shredding type per variant column name, e.g.
    ///        `{"v": struct{a: int32, b: string}}`. Names that are not top-level variant columns
    ///        of `logical_schema` are ignored. Returns nullptr when no name matches (the file is
    ///        written unshredded, mirroring the Java behavior).
    static Result<std::shared_ptr<VariantShreddingWritePlan>> Create(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<std::string, std::shared_ptr<arrow::DataType>>& column_shredding_types);

    /// Creates a plan shredding the variant columns at the given field-index paths (top-level or
    /// nested inside structs). Paths that do not point at a variant field are ignored. Returns
    /// nullptr when no path matches.
    static Result<std::shared_ptr<VariantShreddingWritePlan>> CreateFromPaths(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::map<std::vector<int32_t>, std::shared_ptr<arrow::DataType>>&
            path_shredding_types);

    /// Creates a plan from the `variant.shreddingSchema` option value: a ROW type JSON whose
    /// fields map top-level variant column names to their shredding types (nested variant
    /// columns cannot be configured, as in Java).
    static Result<std::shared_ptr<VariantShreddingWritePlan>> FromConfiguredSchema(
        const std::shared_ptr<arrow::Schema>& logical_schema,
        const std::string& configured_schema_json);

    const std::shared_ptr<arrow::Schema>& LogicalSchema() const {
        return logical_schema_;
    }

    const std::shared_ptr<arrow::Schema>& PhysicalSchema() const {
        return physical_schema_;
    }

    /// The planned variant columns, ordered by path.
    const std::vector<PlannedColumn>& Columns() const {
        return columns_;
    }

 private:
    VariantShreddingWritePlan(std::shared_ptr<arrow::Schema> logical_schema,
                              std::shared_ptr<arrow::Schema> physical_schema,
                              std::vector<PlannedColumn> columns)
        : logical_schema_(std::move(logical_schema)),
          physical_schema_(std::move(physical_schema)),
          columns_(std::move(columns)) {}

    std::shared_ptr<arrow::Schema> logical_schema_;
    std::shared_ptr<arrow::Schema> physical_schema_;
    std::vector<PlannedColumn> columns_;
};

}  // namespace paimon
