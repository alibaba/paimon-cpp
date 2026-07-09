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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

struct ArrowArray;
struct ArrowSchema;

namespace paimon {

/// Arguments controlling how a variant value is cast to a target type in `Variant::VariantGet`.
struct PAIMON_EXPORT VariantCastArgs {
    /// Whether an invalid cast fails the call (true) or yields SQL NULL (false).
    bool fail_on_error = true;
    /// The time zone used when rendering TIMESTAMP values. Supported forms are `UTC`/`Z`/`GMT`,
    /// fixed offsets such as `+08:00`, and IANA region ids such as `Asia/Shanghai`.
    std::string zone_id = "UTC";
};

/// A Variant represents a type that contains one of: 1) Primitive: A type and corresponding
/// value (e.g. INT, STRING); 2) Array: An ordered list of Variant values; 3) Object: An
/// unordered collection of string/Variant pairs (i.e. key/value pairs). An object may not
/// contain duplicate keys.
///
/// A Variant is encoded with 2 binaries: the value and the metadata, following the parquet
/// Variant Binary Encoding specification (compatible with the Java / Spark implementation). The
/// encoding allows representation of semi-structured data (e.g. JSON) in a form that can be
/// efficiently queried by path.
///
/// In an Arrow schema, a Variant field is represented as
/// `struct<value: binary not null, metadata: binary not null>` marked with Paimon-specific field
/// metadata; use `Variant::ArrowField` to construct such a field.
class PAIMON_EXPORT Variant {
 public:
    ~Variant();

    /// Parses a JSON string as a Variant (duplicate object keys are rejected).
    ///
    /// @param json The JSON document text.
    /// @param pool The memory pool used for the variant buffers.
    /// @return A result containing the created variant or an error.
    static Result<std::unique_ptr<Variant>> FromJson(const std::string& json,
                                                     const std::shared_ptr<MemoryPool>& pool);

    /// Creates a Variant from already-encoded value and metadata binaries (copied into `pool`).
    ///
    /// @param value The variant value binary.
    /// @param value_length The length of the value binary.
    /// @param metadata The variant metadata binary.
    /// @param metadata_length The length of the metadata binary.
    /// @param pool The memory pool used for the variant buffers.
    /// @return A result containing the created variant or an error.
    static Result<std::unique_ptr<Variant>> Create(const char* value, uint64_t value_length,
                                                   const char* metadata, uint64_t metadata_length,
                                                   const std::shared_ptr<MemoryPool>& pool);

    /// The variant value binary. The view remains valid as long as this variant exists.
    std::string_view Value() const;

    /// The variant metadata binary. The view remains valid as long as this variant exists.
    std::string_view Metadata() const;

    /// The size of the variant in bytes (value size + metadata size).
    int64_t SizeInBytes() const;

    /// Stringifies the variant in JSON format.
    ///
    /// @param zone_id The time zone used when rendering TIMESTAMP values.
    /// @return A result containing the JSON text or an error.
    Result<std::string> ToJson(const std::string& zone_id = "UTC") const;

    /// Extracts a sub-variant value according to a path which starts with a `$`, e.g. `$.key`,
    /// `$['key']`, `$["key"]`, `$.array[0]`, and casts the value to the target type.
    ///
    /// @param path The extraction path.
    /// @param target_type The target Arrow type (C data interface, consumed by the call); only
    ///        scalar types are supported. Use `VariantGetArrow` for nested targets.
    /// @param cast_args Cast behavior arguments.
    /// @return A result containing the extracted literal, or nullopt for SQL NULL (unmatched
    ///         path, variant null, or an invalid cast with `fail_on_error == false`).
    Result<std::optional<Literal>> VariantGet(const std::string& path,
                                              struct ArrowSchema* target_type,
                                              const VariantCastArgs& cast_args) const;

    /// Extracts a sub-variant value according to a path which starts with a `$` and casts the
    /// value to the (possibly nested) target field type.
    ///
    /// In addition to scalar types, the target may be a STRUCT (cast from a variant object,
    /// children matched by field name; unmatched children are null), a MAP with string keys
    /// (cast from a variant object), a LIST (cast from a variant array), or a variant-marked
    /// field created by `Variant::ArrowField` (the sub-variant is deeply re-encoded).
    ///
    /// @param path The extraction path.
    /// @param target_field The target Arrow field (C data interface, consumed by the call).
    /// @param cast_args Cast behavior arguments.
    /// @return A result containing a length-1 Arrow array of the target type whose single slot
    ///         holds the result; a null slot represents SQL NULL (unmatched path, variant null,
    ///         or an invalid cast with `fail_on_error == false`).
    Result<std::unique_ptr<struct ArrowArray>> VariantGetArrow(
        const std::string& path, struct ArrowSchema* target_field,
        const VariantCastArgs& cast_args) const;

    /// Extracts a sub-variant value according to a path which starts with a `$` and renders it
    /// as JSON text.
    ///
    /// @param path The extraction path.
    /// @param zone_id The time zone used when rendering TIMESTAMP values.
    /// @return A result containing the JSON text, or nullopt if the path does not match.
    Result<std::optional<std::string>> VariantGetJson(const std::string& path,
                                                      const std::string& zone_id = "UTC") const;

    /// Creates an Arrow field definition for the Variant type.
    ///
    /// This function constructs an Arrow Field (internally
    /// `struct<value: binary not null, metadata: binary not null>`) and exports it to the C data
    /// interface structure `::ArrowSchema`. It automatically injects Paimon-specific metadata to
    /// identify the field as a VARIANT.
    ///
    /// @param field_name The name of the Arrow field.
    /// @param nullable Whether the field is nullable.
    /// @param metadata A map of key-value metadata to be attached to the field.
    /// @return A result containing a unique pointer to the generated `::ArrowSchema` or an error.
    static Result<std::unique_ptr<struct ArrowSchema>> ArrowField(
        const std::string& field_name, bool nullable = true,
        std::unordered_map<std::string, std::string> metadata = {});

 private:
    class Impl;

    explicit Variant(std::unique_ptr<Impl>&& impl);

    std::unique_ptr<Impl> impl_;
};

/// Builds a variant-access projection field: a struct field that replaces a VARIANT column in
/// the read schema so that, instead of the full variant, only the described paths are extracted
/// at read time (reading only the required shredded sub-columns from shredded files).
///
/// Example: read `$.age` as INT64 and `$.city` as STRING from variant column `v`:
///
///     VariantAccessBuilder builder;
///     builder.AddField(age_type, "$.age");
///     builder.AddField(city_type, "$.city");
///     auto field = builder.Build("v");  // use in ReadContextBuilder::SetReadSchema
///
/// The resulting struct has one child per added field, named by its position ("0", "1", ...).
class PAIMON_EXPORT VariantAccessBuilder {
 public:
    VariantAccessBuilder();
    ~VariantAccessBuilder();

    /// Adds an extracted field.
    ///
    /// @param target_type The Arrow type the extracted value is cast to (C data interface,
    ///        consumed by the call). Nested targets follow `Variant::VariantGetArrow` semantics.
    /// @param path The extraction path, e.g. `$.a.b` or `$.array[0]`.
    /// @param fail_on_error Whether an invalid cast fails the read (true) or yields SQL NULL.
    /// @param zone_id The time zone used when rendering TIMESTAMP values.
    Status AddField(struct ArrowSchema* target_type, const std::string& path,
                    bool fail_on_error = true, const std::string& zone_id = "UTC");

    /// Builds the projection field named `field_name`, to be used in the read schema in place
    /// of the variant column with the same name.
    Result<std::unique_ptr<struct ArrowSchema>> Build(const std::string& field_name) const;

 private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace paimon
