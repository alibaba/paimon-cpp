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

/* This file is based on source code from the Spark Project (http://spark.apache.org/), licensed
 * by the Apache Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. */

#pragma once

#include <memory>
#include <vector>

#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class ArrayBuilder;
class BinaryBuilder;
class DataType;
class ListBuilder;
class MemoryPool;
class StructBuilder;
}  // namespace arrow

namespace paimon {

/// Shreds variant values of one column into a physical shredded Arrow array, implementing the
/// `castShredded` algorithm of the parquet-format VariantShredding.md specification (mirroring
/// the Java `VariantShreddingWriter`). Decimals and integers are allowed to shred to numerically
/// equivalent values of a different scale (`allowNumericScaleChanges` in Java is always true).
class VariantShreddedColumnWriter {
 public:
    /// Creates a writer for one variant column.
    ///
    /// @param schema The shredding schema of the column.
    /// @param physical_type The physical shredded struct type
    ///        (`VariantShreddingUtils::VariantShreddingSchema` output).
    /// @param pool The Arrow memory pool used by the builders.
    static Result<std::unique_ptr<VariantShreddedColumnWriter>> Create(
        const std::shared_ptr<VariantSchema>& schema,
        const std::shared_ptr<arrow::DataType>& physical_type, arrow::MemoryPool* pool);

    /// Shreds one variant value and appends the result row.
    Status Append(const GenericVariant& variant);

    /// Appends a null variant row.
    Status AppendNull();

    /// Finishes and returns the shredded array of all appended rows.
    Result<std::shared_ptr<arrow::Array>> Finish();

 private:
    /// Builder handles of one shredding schema node (a `metadata`/`value`/`typed_value` group).
    struct Node {
        const VariantSchema* schema = nullptr;
        arrow::StructBuilder* group = nullptr;
        arrow::BinaryBuilder* metadata = nullptr;
        arrow::BinaryBuilder* value = nullptr;
        // Exactly one of the following is set when `schema->typed_idx >= 0`.
        arrow::ArrayBuilder* typed_scalar = nullptr;
        arrow::ListBuilder* typed_list = nullptr;
        arrow::StructBuilder* typed_object = nullptr;
        std::vector<Node> object_children;
        std::unique_ptr<Node> array_element;
    };

    VariantShreddedColumnWriter(const std::shared_ptr<VariantSchema>& schema,
                                std::unique_ptr<arrow::ArrayBuilder>&& root_builder);

    static Status BuildNode(const std::shared_ptr<VariantSchema>& schema,
                            arrow::StructBuilder* group, Node* node);

    Status AppendVariantNode(const GenericVariant& variant, Node* node);

    /// Appends a missing object field: the group is present with all its children set to null.
    Status AppendMissingNode(Node* node);

    /// Tries to append the variant as a typed scalar. Sets `*shredded` to whether it succeeded
    /// (on failure nothing is appended).
    Status TryTypedShred(const GenericVariant& variant, VariantValueType variant_type, Node* node,
                         bool* shredded);

    std::shared_ptr<VariantSchema> schema_;
    std::unique_ptr<arrow::ArrayBuilder> root_builder_;
    Node root_;
};

}  // namespace paimon
