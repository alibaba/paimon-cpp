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
#include <string>
#include <vector>

#include "paimon/common/data/variant/variant_path_segment.h"
#include "paimon/data/variant.h"
#include "paimon/result.h"

namespace arrow {
class DataType;
class Field;
}  // namespace arrow

namespace paimon {

/// One extracted field of a variant-access projection: the extraction path, cast behavior, and
/// the target field the extracted value is cast to.
struct VariantAccessSpec {
    std::string path;
    std::vector<VariantPathSegment> segments;
    VariantCastArgs cast_args;
    std::shared_ptr<arrow::Field> target_field;
};

/// Utilities for variant-access projections: a variant column read as a struct whose children
/// each carry a `__VARIANT_METADATA<path>;<failOnError>;<timeZoneId>` description (mirroring the
/// Java `VariantMetadataUtils`). Such a projection extracts the described paths from the variant
/// column at read time, reading only the required shredded sub-columns.
class VariantAccessUtils {
 public:
    static constexpr char kMetadataKey[] = "__VARIANT_METADATA";
    static constexpr char kDelimiter = ';';

    VariantAccessUtils() = delete;
    ~VariantAccessUtils() = delete;

    /// Builds the description string encoding one access spec.
    static std::string BuildVariantMetadata(const std::string& path, bool fail_on_error,
                                            const std::string& zone_id);

    /// Whether `type` is a variant-access projection: a struct with at least one field whose
    /// children all carry a `__VARIANT_METADATA` description.
    static bool IsVariantAccessType(const std::shared_ptr<arrow::DataType>& type);

    /// Parses the access specs of a variant-access projection field.
    static Result<std::vector<VariantAccessSpec>> ParseAccessSpecs(
        const std::shared_ptr<arrow::Field>& access_field);

    /// Prunes a shredded file field down to the sub-columns required by the access specs:
    /// `metadata` is always kept, `typed_value` is narrowed to the requested top-level keys, and
    /// `value` is kept only when some requested key is not shredded. Returns `file_field`
    /// unchanged when the file is unshredded or the paths cannot be pruned (root or array-first
    /// paths).
    static Result<std::shared_ptr<arrow::Field>> ClipShreddedFileField(
        const std::vector<VariantAccessSpec>& specs,
        const std::shared_ptr<arrow::Field>& file_field);
};

}  // namespace paimon
