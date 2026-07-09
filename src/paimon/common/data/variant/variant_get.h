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

#include <memory>
#include <optional>
#include <string>

#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/data/variant.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace arrow {
class Array;
class ArrayBuilder;
class DataType;
class Field;
class MemoryPool;
}  // namespace arrow

namespace paimon {

/// Implements `variant_get` semantics: extracting a sub-variant by a JSONPath-like path and
/// casting it to a target type.
class VariantGetExecutor {
 public:
    VariantGetExecutor() = delete;
    ~VariantGetExecutor() = delete;

    /// Extracts a sub-variant value according to a path which starts with a `$`, e.g. `$.key`,
    /// `$['key']`, `$["key"]`, `$.array[0]`. Returns nullptr if the path does not match the
    /// variant structure.
    static Result<std::shared_ptr<GenericVariant>> ExtractByPath(
        const std::shared_ptr<GenericVariant>& variant, const std::string& path);

    /// Extracts a sub-variant by `path` and casts it to `target_type`. Returns nullopt for SQL
    /// NULL (unmatched path, variant null, or an invalid cast with `fail_on_error == false`).
    ///
    /// Nested target types (ROW/ARRAY/MAP/VARIANT) are not supported by the `Literal` result
    /// type; use `CastToBuilder` / `GetAsArrow` for structured results.
    static Result<std::optional<Literal>> Get(const std::shared_ptr<GenericVariant>& variant,
                                              const std::string& path,
                                              const std::shared_ptr<arrow::DataType>& target_type,
                                              const VariantCastArgs& cast_args);

    /// Casts `variant` to the type of `target_field` and appends the result to `builder`.
    ///
    /// Supported targets beyond scalars: a variant-marked struct field (the variant is deeply
    /// re-encoded), STRUCT (from a variant object, children matched by name; unmatched children
    /// are null), MAP with string keys (from a variant object), and LIST (from a variant array).
    /// A nullptr `variant`, a variant null, or an invalid cast with `fail_on_error == false`
    /// appends null.
    static Status CastToBuilder(const std::shared_ptr<GenericVariant>& variant,
                                const std::shared_ptr<arrow::Field>& target_field,
                                arrow::ArrayBuilder* builder, const VariantCastArgs& cast_args,
                                const std::shared_ptr<MemoryPool>& pool);

    /// Extracts a sub-variant by `path`, casts it to the (possibly nested) type of
    /// `target_field`, and returns a length-1 arrow array holding the result (a null slot
    /// represents SQL NULL). `arrow_pool` must outlive the returned array.
    static Result<std::shared_ptr<arrow::Array>> GetAsArrow(
        const std::shared_ptr<GenericVariant>& variant, const std::string& path,
        const std::shared_ptr<arrow::Field>& target_field, const VariantCastArgs& cast_args,
        const std::shared_ptr<MemoryPool>& pool, arrow::MemoryPool* arrow_pool);
};

}  // namespace paimon
