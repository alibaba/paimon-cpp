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
#include <string_view>

#include "paimon/common/data/variant/variant_schema.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class MemoryPool;
class StructArray;
}  // namespace arrow

namespace paimon {

class VariantBuilder;

/// Reassembles shredded variant columns back into the unshredded
/// `struct<value: binary, metadata: binary>` representation, implementing the reconstruction
/// algorithm of the parquet-format VariantShredding.md specification (mirroring the Java
/// `ShreddingUtils.rebuild`).
class VariantReassembler {
 public:
    VariantReassembler() = delete;
    ~VariantReassembler() = delete;

    /// Reassembles a shredded variant column into a `struct<value, metadata>` array.
    ///
    /// @param shredded The physical shredded array read from the file.
    /// @param schema The shredding schema of the column
    ///        (`VariantShreddingUtils::BuildVariantSchema` of the file type).
    /// @param pool The memory pool used for intermediate variant rebuilding.
    /// @param arrow_pool The Arrow memory pool used for the output array.
    /// @return The unshredded variant array (`VariantTypeUtils::UnshreddedStructType`).
    static Result<std::shared_ptr<arrow::Array>> AssembleVariantArray(
        const std::shared_ptr<arrow::StructArray>& shredded,
        const std::shared_ptr<VariantSchema>& schema, const std::shared_ptr<MemoryPool>& pool,
        arrow::MemoryPool* arrow_pool);

    /// Rebuilds the variant value at `row` of a shredded (sub-)struct into `builder`, following
    /// the same reconstruction algorithm. `schema` describes `shredded`, which may be any level
    /// of the shredded tree; `metadata` is the column's top-level metadata binary.
    static Status RebuildValue(const arrow::StructArray& shredded, int64_t row,
                               std::string_view metadata, const VariantSchema& schema,
                               const std::shared_ptr<MemoryPool>& pool, VariantBuilder* builder);
};

}  // namespace paimon
