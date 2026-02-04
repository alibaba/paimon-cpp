/*
 * Copyright 2024-present Alibaba Inc.
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

#include "paimon/memory/bytes.h"

#include <cassert>
#include <cstring>
#include <memory>
#include <new>
#include <string_view>
#include <utility>

namespace paimon {

PAIMON_UNIQUE_PTR<Bytes> Bytes::AllocateBytes(int32_t length, MemoryPool* pool) {
    return pool->AllocateUnique<Bytes>(length, pool);
}

PAIMON_UNIQUE_PTR<Bytes> Bytes::AllocateBytes(const std::string& str, MemoryPool* pool) {
    return pool->AllocateUnique<Bytes>(str, pool);
}

Bytes::Bytes(size_t size, MemoryPool* pool) : pool_(pool), size_(size) {
    if (size > 0) {
        assert(pool_);
        data_ = reinterpret_cast<char*>(pool_->Malloc(size_));
        assert(data_);
        std::memset(data_, 0, size_ * sizeof(char));
    }
}

Bytes::Bytes(const std::string& str, MemoryPool* pool) : pool_(pool), size_(str.size()) {
    if (str.size() > 0) {
        assert(pool_);
        data_ = reinterpret_cast<char*>(pool_->Malloc(size_));
        assert(data_);
        std::memcpy(data_, str.data(), size_ * sizeof(char));
    }
}

Bytes::Bytes(Bytes&& other) noexcept {
    *this = std::move(other);
}

Bytes& Bytes::operator=(Bytes&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (data_ != nullptr) {
        assert(pool_);
        pool_->Free(data_, size_);
    }
    pool_ = other.pool_;
    data_ = other.data_;
    size_ = other.size_;
    other.pool_ = nullptr;
    other.data_ = nullptr;
    other.size_ = 0;
    return *this;
}

Bytes::~Bytes() {
    if (data_ != nullptr) {
        assert(pool_);
        pool_->Free(data_, size_);
        data_ = nullptr;
    }
}

bool Bytes::operator==(const Bytes& other) const {
    if (this == &other) {
        return true;
    }
    return size_ == other.size_ &&
           ((data_ == other.data_) || (std::memcmp(data_, other.data_, size_) == 0));
}

bool Bytes::operator<(const Bytes& other) const {
    std::string_view v1(data_, size_);
    std::string_view v2(other.data_, other.size_);
    return v1 < v2;
}

int32_t Bytes::compare(const Bytes& other) const {
    std::string_view v1(data_, size_);
    std::string_view v2(other.data_, other.size_);
    return v1.compare(v2);
}

char& Bytes::operator[](size_t idx) const {
    assert(idx < size());
    return data_[idx];
}

PAIMON_UNIQUE_PTR<Bytes> Bytes::CopyOf(const Bytes& other, size_t len, MemoryPool* pool) {
    assert(len >= other.size_);
    assert(pool);
    auto bytes = Bytes::AllocateBytes(len, pool);
    if (bytes && bytes->data_ && other.size_ > 0) {
        std::memcpy(bytes->data_, other.data_, other.size_);
    }
    return bytes;
}

}  // namespace paimon
