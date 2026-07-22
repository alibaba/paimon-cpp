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
#include <optional>
#include <string>
#include <string_view>

#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {

/// A Variant represents a type that contains one of: 1) Primitive: A type and corresponding value
/// (e.g. INT, STRING); 2) Array: An ordered list of Variant values; 3) Object: An unordered
/// collection of string/Variant pairs (i.e. key/value pairs). An object may not contain duplicate
/// keys.
///
/// A Variant is encoded with 2 binaries: the value and the metadata. The Variant Binary Encoding
/// allows representation of semi-structured data (e.g. JSON) in a form that can be efficiently
/// queried by path. The design is intended to allow efficient access to nested data even in the
/// presence of very wide or deep structures.
class GenericVariant {
 public:
    /// A field of a variant object.
    struct ObjectField {
        std::string key;
        std::shared_ptr<GenericVariant> value;
    };

    /// Creates a variant taking ownership of the given buffers. Fails with `MALFORMED_VARIANT`
    /// if the metadata version is unsupported, or `VARIANT_CONSTRUCTOR_SIZE_LIMIT` if either
    /// buffer exceeds the 128MiB size limit.
    static Result<std::shared_ptr<GenericVariant>> Create(std::shared_ptr<Bytes> value,
                                                          std::shared_ptr<Bytes> metadata);

    /// Creates a variant by copying the given buffers into `pool`.
    static Result<std::shared_ptr<GenericVariant>> Create(std::string_view value,
                                                          std::string_view metadata,
                                                          const std::shared_ptr<MemoryPool>& pool);

    /// Parses a JSON string as a variant (duplicate object keys are rejected).
    static Result<std::shared_ptr<GenericVariant>> FromJson(
        std::string_view json, const std::shared_ptr<MemoryPool>& pool);

    /// The variant value binary. For a sub-variant (`Pos() != 0`), the view covers only the
    /// sub-variant slice of the underlying buffer.
    Result<std::string_view> Value() const;

    /// The whole underlying value buffer, regardless of `Pos()`.
    std::string_view RawValue() const;

    /// The variant metadata binary.
    std::string_view Metadata() const;

    /// The variant value doesn't use the whole value binary, but starts from the `Pos()` index
    /// and spans a size of `VariantBinaryUtil::ValueSize`. This design avoids frequent copies of
    /// the value binary when reading a sub-variant in an array/object element.
    int32_t Pos() const {
        return pos_;
    }

    /// The size of the variant in bytes (value size + metadata size).
    int64_t SizeInBytes() const;

    /// Stringifies the variant in JSON format. `zone_id` controls the rendering of TIMESTAMP
    /// values; supported forms are "UTC"/"Z"/"GMT" and fixed offsets such as "+08:00".
    Result<std::string> ToJson(const std::string& zone_id = "UTC") const;

    /// The value type of the variant.
    Result<VariantValueType> GetType() const;

    /// The type info bits of the variant value header.
    Result<int32_t> GetTypeInfo() const;

    Result<bool> GetBoolean() const;
    Result<int64_t> GetLong() const;
    Result<double> GetDouble() const;
    Result<VariantDecimal> GetDecimal() const;
    Result<float> GetFloat() const;
    Result<std::string_view> GetBinary() const;
    Result<std::string_view> GetString() const;
    /// The 16-byte big-endian UUID value.
    Result<std::string_view> GetUuid() const;

    /// The number of object fields in the variant. It is only legal to call it when `GetType()`
    /// is `kObject`.
    Result<int32_t> ObjectSize() const;

    /// Finds the field value whose key is equal to `key`. Returns nullptr if the key is not
    /// found. It is only legal to call it when `GetType()` is `kObject`. The returned sub-variant
    /// shares the underlying buffers with this variant.
    Result<std::shared_ptr<GenericVariant>> GetFieldByKey(std::string_view key) const;

    /// Gets the object field at the `index` slot. Returns nullopt if `index` is out of the bound
    /// of `[0, ObjectSize())`. It is only legal to call it when `GetType()` is `kObject`.
    Result<std::optional<ObjectField>> GetFieldAtIndex(int32_t index) const;

    /// Gets the metadata dictionary id for the object field at the `index` slot. It is only
    /// legal to call it when `GetType()` is `kObject`.
    Result<int32_t> GetDictionaryIdAtIndex(int32_t index) const;

    /// The number of array elements in the variant. It is only legal to call it when `GetType()`
    /// is `kArray`.
    Result<int32_t> ArraySize() const;

    /// Gets the array element at the `index` slot. Returns nullptr if `index` is out of the
    /// bound of `[0, ArraySize())`. It is only legal to call it when `GetType()` is `kArray`.
    Result<std::shared_ptr<GenericVariant>> GetElementAtIndex(int32_t index) const;

    bool operator==(const GenericVariant& other) const;

 private:
    GenericVariant(std::shared_ptr<Bytes> value, std::shared_ptr<Bytes> metadata, int32_t pos);

    std::shared_ptr<GenericVariant> SubVariant(int32_t pos) const;

    std::shared_ptr<Bytes> value_;
    std::shared_ptr<Bytes> metadata_;
    int32_t pos_;
};

}  // namespace paimon
