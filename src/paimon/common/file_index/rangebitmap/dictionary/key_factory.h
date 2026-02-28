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

#include "paimon/common/file_index/rangebitmap/dictionary/chunk.h"
#include "paimon/common/io/memory_segment_output_stream.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/result.h"

namespace paimon {

class InputStream;
class MemoryPool;

class KeyFactory : public std::enable_shared_from_this<KeyFactory> {
 public:
    virtual ~KeyFactory() = default;

    virtual FieldType GetFieldType() const = 0;

    /// For writing new chunk
    virtual Result<std::unique_ptr<Chunk>> CreateChunk(const std::shared_ptr<MemoryPool>& pool,
                                                       const Literal& key, int32_t code,
                                                       int32_t keys_length_limit) = 0;

    /// For reading existing chunk, lazy loading keys in the directory
    virtual Result<std::unique_ptr<Chunk>> MmapChunk(
        const std::shared_ptr<MemoryPool>& pool, const std::shared_ptr<InputStream>& input_stream,
        int32_t chunk_offest, int32_t keys_base_offset) = 0;

    static Result<std::shared_ptr<KeyFactory>> Create(FieldType field_type);
};

class FixedLengthKeyFactory : public KeyFactory {
 public:
    Result<std::unique_ptr<Chunk>> CreateChunk(const std::shared_ptr<MemoryPool>& pool,
                                               const Literal& key, int32_t code,
                                               int32_t keys_length_limit) override;
    Result<std::unique_ptr<Chunk>> MmapChunk(const std::shared_ptr<MemoryPool>& pool,
                                             const std::shared_ptr<InputStream>& input_stream,
                                             int32_t chunk_offest,
                                             int32_t keys_base_offset) override;
    virtual size_t GetFieldSize() const = 0;
};

class VariableLengthKeyFactory : public KeyFactory {
 public:
    Result<std::unique_ptr<Chunk>> CreateChunk(const std::shared_ptr<MemoryPool>& pool,
                                               const Literal& key, int32_t code,
                                               int32_t keys_length_limit) override;
    Result<std::unique_ptr<Chunk>> MmapChunk(const std::shared_ptr<MemoryPool>& pool,
                                             const std::shared_ptr<InputStream>& input_stream,
                                             int32_t chunk_offest,
                                             int32_t keys_base_offset) override;
};

class DateKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::DATE;
    }
    size_t GetFieldSize() const override {
        return sizeof(int32_t);
    }
};

class IntKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::INT;
    }
    size_t GetFieldSize() const override {
        return sizeof(int32_t);
    }
};

class BigIntKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::BIGINT;
    }
    size_t GetFieldSize() const override {
        return sizeof(int64_t);
    }
};

class BooleanKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::BOOLEAN;
    }
    size_t GetFieldSize() const override {
        return sizeof(bool);
    }
};

class TinyIntKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::TINYINT;
    }
    size_t GetFieldSize() const override {
        return sizeof(int8_t);
    }
};

class SmallIntKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::SMALLINT;
    }
    size_t GetFieldSize() const override {
        return sizeof(int16_t);
    }
};

class FloatKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::FLOAT;
    }
    size_t GetFieldSize() const override {
        return sizeof(float);
    }
};

class DoubleKeyFactory final : public FixedLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::DOUBLE;
    }
    size_t GetFieldSize() const override {
        return sizeof(double);
    }
};

class StringKeyFactory final : public VariableLengthKeyFactory {
 public:
    FieldType GetFieldType() const override {
        return FieldType::STRING;
    }
};

}  // namespace paimon
