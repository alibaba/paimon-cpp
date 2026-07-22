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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "paimon/common/data/variant/generic_variant.h"
#include "paimon/common/data/variant/variant_binary_util.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {

/// Builds variant value and metadata binaries, either by parsing JSON values or by appending
/// values directly.
class VariantBuilder {
 public:
    /// Temporarily stores the information of a field. All fields of a JSON object are collected,
    /// sorted by their keys, and then the variant object is built in sorted order.
    struct FieldEntry {
        std::string key;
        int32_t id;
        int32_t offset;

        FieldEntry(std::string key, int32_t id, int32_t offset)
            : key(std::move(key)), id(id), offset(offset) {}
    };

    explicit VariantBuilder(bool allow_duplicate_keys)
        : allow_duplicate_keys_(allow_duplicate_keys) {}

    /// Parses a JSON string as a variant value.
    ///
    /// When `allow_duplicate_keys` is true, the last occurrence of a duplicate object key wins;
    /// otherwise duplicate keys make the parse fail.
    static Result<std::shared_ptr<GenericVariant>> ParseJson(
        std::string_view json, bool allow_duplicate_keys, const std::shared_ptr<MemoryPool>& pool);

    /// Builds the variant metadata from the collected dictionary keys and returns the variant
    /// result.
    Result<std::shared_ptr<GenericVariant>> Build(const std::shared_ptr<MemoryPool>& pool);

    /// The variant value written so far, without metadata. Used in shredding to produce a final
    /// value where all shredded values refer to a common metadata.
    std::string_view ValueWithoutMetadata() const {
        return {reinterpret_cast<const char*>(write_buffer_.data()),
                static_cast<size_t>(write_pos_)};
    }

    Status AppendString(std::string_view str);
    Status AppendNull();
    Status AppendBoolean(bool b);
    /// Appends a long value. The actual used integer type depends on the value range.
    Status AppendLong(int64_t l);
    Status AppendDouble(double d);
    /// Appends a decimal value. Its precision and scale must fit into `kMaxDecimal16Precision`.
    Status AppendDecimal(const VariantDecimal& d);
    Status AppendDate(int32_t days_since_epoch);
    Status AppendTimestamp(int64_t micros_since_epoch);
    Status AppendTimestampNtz(int64_t micros_since_epoch);
    Status AppendFloat(float f);
    Status AppendBinary(std::string_view binary);
    /// Appends a UUID value (16 bytes, big-endian).
    Status AppendUuid(std::string_view uuid_bytes);

    /// Adds a key to the variant dictionary and returns its id. If the key already exists, the
    /// dictionary is not modified.
    int32_t AddKey(std::string_view key);

    /// The current write position of the variant builder. It is used together with
    /// `FinishWritingObject` or `FinishWritingArray`.
    int32_t GetWritePos() const {
        return write_pos_;
    }

    /// Finishes writing a variant object after all of its fields have already been written. The
    /// process is as follows:
    /// 1. The caller calls `GetWritePos` before writing any fields to obtain the `start`
    ///    parameter.
    /// 2. The caller appends all the object fields to the builder. In the meantime, it should
    ///    maintain the `fields` parameter. Before appending each field, it should append an entry
    ///    to `fields` to record the offset of the field, computed as `GetWritePos() - start`.
    /// 3. The caller calls `FinishWritingObject` to finish writing a variant object.
    ///
    /// This function sorts the fields by key. If there are duplicate field keys:
    /// - when `allow_duplicate_keys` is true, the field with the greatest offset value (the last
    ///   appended one) is kept;
    /// - otherwise, the call fails.
    Status FinishWritingObject(int32_t start, std::vector<FieldEntry>* fields);

    /// Finishes writing a variant array after all of its elements have already been written. The
    /// process is similar to that of `FinishWritingObject`.
    Status FinishWritingArray(int32_t start, const std::vector<int32_t>& offsets);

    /// Appends a variant value. The keys of the input variant are inserted into the current
    /// variant dictionary and the value is rebuilt with new field ids. For scalar values, the
    /// binary slice is copied directly.
    Status AppendVariant(const GenericVariant& v);

    /// Appends the variant value without rewriting or creating any metadata. This is used when
    /// building an object during shredding, where there is a fixed pre-existing metadata that all
    /// shredded values refer to.
    Status ShallowAppendVariant(std::string_view value, int32_t pos);

 private:
    Status CheckCapacity(int32_t additional);
    Status AppendVariantImpl(std::string_view value, std::string_view metadata, int32_t pos);
    static int32_t GetIntegerSize(int32_t value);

    // The write buffer in building the variant value. Its first `write_pos_` bytes have been
    // written.
    std::vector<uint8_t> write_buffer_ = std::vector<uint8_t>(128);
    int32_t write_pos_ = 0;
    // Maps keys to a monotonically increasing id.
    std::unordered_map<std::string, int32_t> dictionary_;
    // Stores all keys in `dictionary_` in the order of id.
    std::vector<std::string> dictionary_keys_;
    const bool allow_duplicate_keys_;
};

}  // namespace paimon
