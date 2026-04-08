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

#include "paimon/common/utils/roaring_navigable_map64.h"

#include <cstring>
#include <memory>
#include <vector>

#include "paimon/memory/memory_pool.h"
#include "paimon/utils/range.h"
#include "paimon/utils/roaring_bitmap64.h"

namespace paimon {

class RoaringNavigableMap64::Impl {
 public:
    RoaringBitmap64 bitmap;
};

class RoaringNavigableMap64::Iterator::Impl {
 public:
    explicit Impl(const RoaringBitmap64& bitmap) : iterator(bitmap.Begin()) {}
    explicit Impl(const RoaringBitmap64::Iterator& iter) : iterator(iter) {}
    RoaringBitmap64::Iterator iterator;
};

RoaringNavigableMap64::RoaringNavigableMap64() : impl_(std::make_unique<Impl>()) {}

RoaringNavigableMap64::RoaringNavigableMap64(const RoaringNavigableMap64& other)
    : impl_(std::make_unique<Impl>()) {
    impl_->bitmap = other.impl_->bitmap;
}

RoaringNavigableMap64::RoaringNavigableMap64(RoaringNavigableMap64&& other) noexcept = default;

RoaringNavigableMap64& RoaringNavigableMap64::operator=(const RoaringNavigableMap64& other) {
    if (this != &other) {
        impl_->bitmap = other.impl_->bitmap;
    }
    return *this;
}

RoaringNavigableMap64& RoaringNavigableMap64::operator=(RoaringNavigableMap64&& other) noexcept =
    default;

RoaringNavigableMap64::~RoaringNavigableMap64() = default;

void RoaringNavigableMap64::AddRange(const Range& range) {
    impl_->bitmap.AddRange(range.from, range.to + 1);
}

bool RoaringNavigableMap64::Contains(int64_t x) const {
    return impl_->bitmap.Contains(x);
}

void RoaringNavigableMap64::Add(int64_t x) {
    impl_->bitmap.Add(x);
}

void RoaringNavigableMap64::Or(const RoaringNavigableMap64& other) {
    impl_->bitmap |= other.impl_->bitmap;
}

void RoaringNavigableMap64::And(const RoaringNavigableMap64& other) {
    impl_->bitmap &= other.impl_->bitmap;
}

void RoaringNavigableMap64::AndNot(const RoaringNavigableMap64& other) {
    impl_->bitmap -= other.impl_->bitmap;
}

bool RoaringNavigableMap64::IsEmpty() const {
    return impl_->bitmap.IsEmpty();
}

bool RoaringNavigableMap64::RunOptimize() {
    // Note: RoaringBitmap64 doesn't have a direct RunOptimize method
    // This is a placeholder - in practice, optimization happens automatically
    return false;
}

int64_t RoaringNavigableMap64::GetLongCardinality() const {
    return impl_->bitmap.Cardinality();
}

int32_t RoaringNavigableMap64::GetIntCardinality() const {
    return static_cast<int32_t>(impl_->bitmap.Cardinality());
}

void RoaringNavigableMap64::Clear() {
    impl_->bitmap = RoaringBitmap64();
}

std::vector<uint8_t> RoaringNavigableMap64::Serialize() const {
    // This is a simplified serialization - in practice, you might want to use
    // a more sophisticated approach
    // Use default pool when no pool is provided
    auto bytes = impl_->bitmap.Serialize(GetDefaultPool().get());
    if (!bytes) {
        return {};
    }

    std::vector<uint8_t> result(bytes->size());
    std::memcpy(result.data(), bytes->data(), bytes->size());
    return result;
}

void RoaringNavigableMap64::Deserialize(const std::vector<uint8_t>& data) {
    // This is a simplified deserialization - in practice, you might want to use
    // a more sophisticated approach
    auto status =
        impl_->bitmap.Deserialize(reinterpret_cast<const char*>(data.data()), data.size());
    if (!status.ok()) {
        // Log error or handle deserialization failure
        // For now, we'll just clear the bitmap on error
        impl_->bitmap = RoaringBitmap64();
    }
}

std::vector<Range> RoaringNavigableMap64::ToRangeList() const {
    std::vector<Range> ranges;
    if (IsEmpty()) {
        return ranges;
    }

    int64_t current_start = -1;
    int64_t current_end = -1;

    for (auto it = begin(); it != end(); ++it) {
        int64_t value = *it;
        if (current_start == -1) {
            current_start = value;
            current_end = value;
        } else if (value == current_end + 1) {
            // Continue the current range
            current_end = value;
        } else {
            // End the current range and start a new one
            ranges.emplace_back(current_start, current_end);
            current_start = value;
            current_end = value;
        }
    }

    if (current_start != -1) {
        ranges.emplace_back(current_start, current_end);
    }

    return ranges;
}

const RoaringBitmap64& RoaringNavigableMap64::GetBitmap() const {
    return impl_->bitmap;
}

RoaringNavigableMap64 RoaringNavigableMap64::BitmapOf(const std::vector<int64_t>& values) {
    RoaringNavigableMap64 result;
    for (int64_t value : values) {
        result.Add(value);
    }
    return result;
}

RoaringNavigableMap64 RoaringNavigableMap64::And(const RoaringNavigableMap64& x1,
                                                 const RoaringNavigableMap64& x2) {
    RoaringNavigableMap64 result;
    result.impl_->bitmap = RoaringBitmap64::And(x1.impl_->bitmap, x2.impl_->bitmap);
    return result;
}

RoaringNavigableMap64 RoaringNavigableMap64::Or(const RoaringNavigableMap64& x1,
                                                const RoaringNavigableMap64& x2) {
    RoaringNavigableMap64 result;
    result.impl_->bitmap = RoaringBitmap64::Or(x1.impl_->bitmap, x2.impl_->bitmap);
    return result;
}

bool RoaringNavigableMap64::operator==(const RoaringNavigableMap64& other) const {
    return impl_->bitmap == other.impl_->bitmap;
}

bool RoaringNavigableMap64::operator!=(const RoaringNavigableMap64& other) const {
    return !(*this == other);
}

// Iterator implementation
RoaringNavigableMap64::Iterator::Iterator(const RoaringNavigableMap64& bitmap)
    : impl_(std::make_unique<Impl>(bitmap.impl_->bitmap.Begin())) {}

RoaringNavigableMap64::Iterator::Iterator(const RoaringNavigableMap64& bitmap, bool is_end)
    : impl_(std::make_unique<Impl>(bitmap.impl_->bitmap.End())) {}

RoaringNavigableMap64::Iterator::Iterator(const Iterator& other)
    : impl_(std::make_unique<Impl>(other.impl_->iterator)) {}

RoaringNavigableMap64::Iterator::Iterator(Iterator&& other) noexcept = default;

RoaringNavigableMap64::Iterator& RoaringNavigableMap64::Iterator::operator=(const Iterator& other) {
    if (this != &other) {
        impl_ = std::make_unique<Impl>(other.impl_->iterator);
    }
    return *this;
}

RoaringNavigableMap64::Iterator& RoaringNavigableMap64::Iterator::operator=(
    Iterator&& other) noexcept = default;

RoaringNavigableMap64::Iterator::~Iterator() = default;

int64_t RoaringNavigableMap64::Iterator::operator*() const {
    return *impl_->iterator;
}

RoaringNavigableMap64::Iterator& RoaringNavigableMap64::Iterator::operator++() {
    ++impl_->iterator;
    return *this;
}

RoaringNavigableMap64::Iterator RoaringNavigableMap64::Iterator::operator++(int) {
    Iterator temp(*this);
    ++(*this);
    return temp;
}

bool RoaringNavigableMap64::Iterator::operator==(const Iterator& other) const {
    return impl_->iterator == other.impl_->iterator;
}

bool RoaringNavigableMap64::Iterator::operator!=(const Iterator& other) const {
    return !(*this == other);
}

RoaringNavigableMap64::Iterator RoaringNavigableMap64::begin() const {
    return Iterator(*this);
}

RoaringNavigableMap64::Iterator RoaringNavigableMap64::end() const {
    // Create an iterator that represents the end
    // For now, we'll create an iterator and set it to a special state
    // In practice, this might need a more sophisticated approach
    Iterator it(*this);
    // Move to the end by advancing past the last element
    auto underlying_end = impl_->bitmap.End();
    it.impl_->iterator = underlying_end;
    return it;
}

}  // namespace paimon
