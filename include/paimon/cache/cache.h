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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "paimon/memory/memory_segment.h"
#include "paimon/result.h"
#include "paimon/visibility.h"

namespace paimon {

class CacheValue;
enum class CacheKind {
    DEFAULT,
    MANIFEST,
    PARQUET_METADATA,
};

class PAIMON_EXPORT CacheKey {
 public:
    static std::shared_ptr<CacheKey> ForPosition(const std::string& file_path, int64_t position,
                                                 int32_t length, bool is_index);
    static std::shared_ptr<CacheKey> ForKind(const std::string& file_path, int64_t position,
                                             int32_t length, CacheKind kind);
    static std::shared_ptr<CacheKey> ForParquetMeta(const std::string& file_uri);

 public:
    virtual ~CacheKey() = default;

    virtual bool IsIndex() const = 0;

    CacheKind GetKind() const {
        return kind_;
    }

    virtual bool Equals(const CacheKey& other) const = 0;

    virtual size_t HashCode() const = 0;

 protected:
    explicit CacheKey(CacheKind kind) : kind_(kind) {}

 private:
    CacheKind kind_ = CacheKind::DEFAULT;
};

using CacheCallback = std::function<void(const std::shared_ptr<CacheKey>&)>;

class PAIMON_EXPORT Cache {
 public:
    virtual ~Cache() = default;

    virtual Result<std::shared_ptr<CacheValue>> Get(
        const std::shared_ptr<CacheKey>& key,
        std::function<Result<std::shared_ptr<CacheValue>>(const std::shared_ptr<CacheKey>&)>
            supplier) = 0;

    virtual Status Put(const std::shared_ptr<CacheKey>& key,
                       const std::shared_ptr<CacheValue>& value) = 0;

    virtual void Invalidate(const std::shared_ptr<CacheKey>& key) = 0;

    virtual void InvalidateAll() = 0;

    virtual size_t Size() const = 0;
};

class PAIMON_EXPORT CacheValue {
 public:
    CacheValue(const MemorySegment& segment, CacheCallback callback);

    ~CacheValue();

    const MemorySegment& GetSegment() const;

    void OnEvict(const std::shared_ptr<CacheKey>& key) const;

    bool operator==(const CacheValue& other) const;

 private:
    MemorySegment segment_;
    CacheCallback callback_;
};

}  // namespace paimon
