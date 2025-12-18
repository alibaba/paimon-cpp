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

#include "paimon/utils/roaring_bitmap32.h"

#include <cassert>
#include <memory>
#include <utility>

#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/result.h"
#include "roaring.hh"  // NOLINT(build/include_subdir)

namespace paimon {
namespace {
roaring::Roaring::const_iterator& GetIterator(void* iter) {
    return *(static_cast<roaring::Roaring::const_iterator*>(iter));
}
roaring::Roaring& GetRoaringBitmap(void* bitmap) {
    return *(static_cast<roaring::Roaring*>(bitmap));
}
}  // namespace

RoaringBitmap32::Iterator::Iterator(const RoaringBitmap32& bitmap) {
    iterator_ =
        new roaring::Roaring::const_iterator(GetRoaringBitmap(bitmap.roaring_bitmap_).begin());
}

RoaringBitmap32::Iterator::~Iterator() {
    if (iterator_) {
        delete static_cast<roaring::Roaring::const_iterator*>(iterator_);
    }
}

RoaringBitmap32::Iterator::Iterator(const RoaringBitmap32::Iterator& other) noexcept {
    *this = other;
}
RoaringBitmap32::Iterator& RoaringBitmap32::Iterator::operator=(
    const RoaringBitmap32::Iterator& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (!iterator_) {
        iterator_ = new roaring::Roaring::const_iterator(GetIterator(other.iterator_));
    } else {
        GetIterator(iterator_) = GetIterator(other.iterator_);
    }
    return *this;
}

RoaringBitmap32::Iterator::Iterator(RoaringBitmap32::Iterator&& other) noexcept {
    *this = std::move(other);
}

RoaringBitmap32::Iterator& RoaringBitmap32::Iterator::operator=(
    RoaringBitmap32::Iterator&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (iterator_) {
        delete static_cast<roaring::Roaring::const_iterator*>(iterator_);
    }
    iterator_ = other.iterator_;
    other.iterator_ = nullptr;
    return *this;
}

int32_t RoaringBitmap32::Iterator::operator*() const {
    return *GetIterator(iterator_);
}
RoaringBitmap32::Iterator& RoaringBitmap32::Iterator::operator++() {
    ++GetIterator(iterator_);
    return *this;
}
bool RoaringBitmap32::Iterator::operator==(const Iterator& other) const {
    if (&other == this) {
        return true;
    }
    assert(iterator_ && other.iterator_);
    return GetIterator(iterator_) == GetIterator(other.iterator_);
}
bool RoaringBitmap32::Iterator::operator!=(const Iterator& other) const {
    return !(*this == other);
}

RoaringBitmap32::RoaringBitmap32() {
    roaring_bitmap_ = new roaring::Roaring();
}
RoaringBitmap32::~RoaringBitmap32() {
    if (roaring_bitmap_) {
        delete static_cast<roaring::Roaring*>(roaring_bitmap_);
    }
}

RoaringBitmap32::RoaringBitmap32(const RoaringBitmap32& other) noexcept {
    *this = other;
}
RoaringBitmap32& RoaringBitmap32::operator=(const RoaringBitmap32& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (!roaring_bitmap_) {
        roaring_bitmap_ = new roaring::Roaring(GetRoaringBitmap(other.roaring_bitmap_));
    } else {
        GetRoaringBitmap(roaring_bitmap_) = GetRoaringBitmap(other.roaring_bitmap_);
    }
    return *this;
}

RoaringBitmap32::RoaringBitmap32(RoaringBitmap32&& other) noexcept {
    *this = std::move(other);
}

RoaringBitmap32& RoaringBitmap32::operator=(RoaringBitmap32&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (roaring_bitmap_) {
        delete static_cast<roaring::Roaring*>(roaring_bitmap_);
    }
    roaring_bitmap_ = other.roaring_bitmap_;
    other.roaring_bitmap_ = nullptr;
    return *this;
}

RoaringBitmap32& RoaringBitmap32::operator|=(const RoaringBitmap32& other) {
    GetRoaringBitmap(roaring_bitmap_) |= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}
RoaringBitmap32& RoaringBitmap32::operator&=(const RoaringBitmap32& other) {
    GetRoaringBitmap(roaring_bitmap_) &= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}
RoaringBitmap32& RoaringBitmap32::operator-=(const RoaringBitmap32& other) {
    GetRoaringBitmap(roaring_bitmap_) -= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}

void RoaringBitmap32::Add(int32_t x) {
    GetRoaringBitmap(roaring_bitmap_).add(x);
}

void RoaringBitmap32::AddRange(int32_t min, int32_t max) {
    GetRoaringBitmap(roaring_bitmap_).addRange(min, max);
}

void RoaringBitmap32::RemoveRange(int32_t min, int32_t max) {
    GetRoaringBitmap(roaring_bitmap_).removeRange(min, max);
}

bool RoaringBitmap32::ContainsAny(int32_t min, int32_t max) const {
    auto iter = EqualOrLarger(min);
    if (iter != End() && *iter < max) {
        return true;
    }
    return false;
}

bool RoaringBitmap32::CheckedAdd(int32_t x) {
    if (Contains(x)) {
        return false;
    }
    Add(x);
    return true;
}

int32_t RoaringBitmap32::Cardinality() const {
    return GetRoaringBitmap(roaring_bitmap_).cardinality();
}

bool RoaringBitmap32::Contains(int32_t x) const {
    return GetRoaringBitmap(roaring_bitmap_).contains(x);
}

bool RoaringBitmap32::IsEmpty() const {
    return GetRoaringBitmap(roaring_bitmap_).isEmpty();
}

size_t RoaringBitmap32::GetSizeInBytes() const {
    return GetRoaringBitmap(roaring_bitmap_).getSizeInBytes();
}

void RoaringBitmap32::Flip(int32_t min, int32_t max) {
    GetRoaringBitmap(roaring_bitmap_).flip(min, max);
}

bool RoaringBitmap32::operator==(const RoaringBitmap32& other) const noexcept {
    if (this == &other) {
        return true;
    }
    assert(roaring_bitmap_ && other.roaring_bitmap_);
    return GetRoaringBitmap(roaring_bitmap_) == GetRoaringBitmap(other.roaring_bitmap_);
}

PAIMON_UNIQUE_PTR<Bytes> RoaringBitmap32::Serialize(MemoryPool* pool) const {
    GetRoaringBitmap(roaring_bitmap_).runOptimize();
    auto& bitmap = GetRoaringBitmap(roaring_bitmap_);
    auto bytes = Bytes::AllocateBytes(bitmap.getSizeInBytes(), pool);
    bitmap.write(bytes->data());
    return bytes;
}

Status RoaringBitmap32::Deserialize(ByteArrayInputStream* input_stream) {
    const char* data = input_stream->GetRawData();
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, input_stream->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t total_length, input_stream->Length());
    try {
        GetRoaringBitmap(roaring_bitmap_) =
            roaring::Roaring::readSafe(data, /*maxbytes=*/total_length - pos);
    } catch (...) {
        return Status::Invalid("catch exception in Deserialize() of RoaringBitmap32");
    }
    return input_stream->Seek(GetRoaringBitmap(roaring_bitmap_).getSizeInBytes(),
                              SeekOrigin::FS_SEEK_CUR);
}

Status RoaringBitmap32::Deserialize(const char* begin, size_t length) {
    try {
        GetRoaringBitmap(roaring_bitmap_) = roaring::Roaring::readSafe(begin, length);
    } catch (...) {
        return Status::Invalid("catch exception in Deserialize() of RoaringBitmap32");
    }
    return Status::OK();
}

std::string RoaringBitmap32::ToString() const {
    return GetRoaringBitmap(roaring_bitmap_).toString();
}

RoaringBitmap32 RoaringBitmap32::And(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs) {
    RoaringBitmap32 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) & GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap32 RoaringBitmap32::Or(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs) {
    RoaringBitmap32 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) | GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap32 RoaringBitmap32::AndNot(const RoaringBitmap32& lhs, const RoaringBitmap32& rhs) {
    RoaringBitmap32 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) - GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap32 RoaringBitmap32::From(const std::vector<int32_t>& values) {
    RoaringBitmap32 res;
    for (const auto& value : values) {
        res.Add(value);
    }
    return res;
}

RoaringBitmap32 RoaringBitmap32::FastUnion(const std::vector<const RoaringBitmap32*>& inputs) {
    std::vector<roaring::Roaring*> roaring_inputs;
    roaring_inputs.reserve(inputs.size());
    for (const auto* roaring : inputs) {
        roaring_inputs.push_back(&GetRoaringBitmap(roaring->roaring_bitmap_));
    }
    RoaringBitmap32 res;
    GetRoaringBitmap(res.roaring_bitmap_) = roaring::Roaring::fastunion(
        roaring_inputs.size(), const_cast<const roaring::Roaring**>(roaring_inputs.data()));
    return res;
}

RoaringBitmap32 RoaringBitmap32::FastUnion(const std::vector<RoaringBitmap32>& inputs) {
    std::vector<const RoaringBitmap32*> inputs_ptr;
    inputs_ptr.reserve(inputs.size());
    for (const auto& bitmap : inputs) {
        inputs_ptr.push_back(&bitmap);
    }
    return FastUnion(inputs_ptr);
}
RoaringBitmap32::Iterator RoaringBitmap32::Begin() const {
    return RoaringBitmap32::Iterator(*this);
}

RoaringBitmap32::Iterator RoaringBitmap32::End() const {
    RoaringBitmap32::Iterator iter(*this);
    GetIterator(iter.iterator_) = GetRoaringBitmap(roaring_bitmap_).end();
    return iter;
}

RoaringBitmap32::Iterator RoaringBitmap32::EqualOrLarger(int32_t key) const {
    RoaringBitmap32::Iterator iter(*this);
    GetIterator(iter.iterator_).equalorlarger(key);
    return iter;
}

}  // namespace paimon
