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

#include "paimon/utils/roaring_bitmap64.h"

#include <cassert>
#include <memory>
#include <utility>

#include "paimon/fs/file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/result.h"
#include "roaring.hh"  // NOLINT(build/include_subdir)

namespace paimon {
namespace {
roaring::Roaring64Map::const_iterator& GetIterator(void* iter) {
    return *(static_cast<roaring::Roaring64Map::const_iterator*>(iter));
}
roaring::Roaring64Map& GetRoaringBitmap(void* bitmap) {
    return *(static_cast<roaring::Roaring64Map*>(bitmap));
}
}  // namespace

RoaringBitmap64::Iterator::Iterator(const RoaringBitmap64& bitmap) {
    iterator_ =
        new roaring::Roaring64Map::const_iterator(GetRoaringBitmap(bitmap.roaring_bitmap_).begin());
}

RoaringBitmap64::Iterator::~Iterator() {
    if (iterator_) {
        delete static_cast<roaring::Roaring64Map::const_iterator*>(iterator_);
    }
}

RoaringBitmap64::Iterator::Iterator(const RoaringBitmap64::Iterator& other) noexcept {
    *this = other;
}
RoaringBitmap64::Iterator& RoaringBitmap64::Iterator::operator=(
    const RoaringBitmap64::Iterator& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (!iterator_) {
        iterator_ = new roaring::Roaring64Map::const_iterator(GetIterator(other.iterator_));
    } else {
        GetIterator(iterator_) = GetIterator(other.iterator_);
    }
    return *this;
}

RoaringBitmap64::Iterator::Iterator(RoaringBitmap64::Iterator&& other) noexcept {
    *this = std::move(other);
}

RoaringBitmap64::Iterator& RoaringBitmap64::Iterator::operator=(
    RoaringBitmap64::Iterator&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (iterator_) {
        delete static_cast<roaring::Roaring64Map::const_iterator*>(iterator_);
    }
    iterator_ = other.iterator_;
    other.iterator_ = nullptr;
    return *this;
}

int64_t RoaringBitmap64::Iterator::operator*() const {
    return *GetIterator(iterator_);
}
RoaringBitmap64::Iterator& RoaringBitmap64::Iterator::operator++() {
    ++GetIterator(iterator_);
    return *this;
}
bool RoaringBitmap64::Iterator::operator==(const Iterator& other) const {
    if (&other == this) {
        return true;
    }
    assert(iterator_ && other.iterator_);
    return GetIterator(iterator_) == GetIterator(other.iterator_);
}
bool RoaringBitmap64::Iterator::operator!=(const Iterator& other) const {
    return !(*this == other);
}

void RoaringBitmap64::Iterator::EqualOrLarger(int64_t value) {
    [[maybe_unused]] bool _ = GetIterator(iterator_).move(value);
}

RoaringBitmap64::RoaringBitmap64() {
    roaring_bitmap_ = new roaring::Roaring64Map();
}
RoaringBitmap64::~RoaringBitmap64() {
    if (roaring_bitmap_) {
        delete static_cast<roaring::Roaring64Map*>(roaring_bitmap_);
    }
}

RoaringBitmap64::RoaringBitmap64(const RoaringBitmap64& other) noexcept {
    *this = other;
}
RoaringBitmap64& RoaringBitmap64::operator=(const RoaringBitmap64& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (!roaring_bitmap_) {
        roaring_bitmap_ = new roaring::Roaring64Map(GetRoaringBitmap(other.roaring_bitmap_));
    } else {
        GetRoaringBitmap(roaring_bitmap_) = GetRoaringBitmap(other.roaring_bitmap_);
    }
    return *this;
}

RoaringBitmap64::RoaringBitmap64(const RoaringBitmap32& other) noexcept {
    *this = other;
}

RoaringBitmap64& RoaringBitmap64::operator=(const RoaringBitmap32& other) noexcept {
    auto bitmap32 = static_cast<roaring::Roaring*>(other.roaring_bitmap_);
    if (!roaring_bitmap_) {
        roaring_bitmap_ = new roaring::Roaring64Map(*bitmap32);
    } else {
        GetRoaringBitmap(roaring_bitmap_) = roaring::Roaring64Map(*bitmap32);
    }
    return *this;
}

RoaringBitmap64::RoaringBitmap64(RoaringBitmap64&& other) noexcept {
    *this = std::move(other);
}

RoaringBitmap64& RoaringBitmap64::operator=(RoaringBitmap64&& other) noexcept {
    if (&other == this) {
        return *this;
    }
    if (roaring_bitmap_) {
        delete static_cast<roaring::Roaring64Map*>(roaring_bitmap_);
    }
    roaring_bitmap_ = other.roaring_bitmap_;
    other.roaring_bitmap_ = nullptr;
    return *this;
}

RoaringBitmap64& RoaringBitmap64::operator|=(const RoaringBitmap64& other) {
    GetRoaringBitmap(roaring_bitmap_) |= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}
RoaringBitmap64& RoaringBitmap64::operator&=(const RoaringBitmap64& other) {
    GetRoaringBitmap(roaring_bitmap_) &= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}
RoaringBitmap64& RoaringBitmap64::operator-=(const RoaringBitmap64& other) {
    GetRoaringBitmap(roaring_bitmap_) -= GetRoaringBitmap(other.roaring_bitmap_);
    return *this;
}

void RoaringBitmap64::Add(int64_t x) {
    GetRoaringBitmap(roaring_bitmap_).add(static_cast<uint64_t>(x));
}

void RoaringBitmap64::AddRange(int64_t min, int64_t max) {
    GetRoaringBitmap(roaring_bitmap_).addRange(min, max);
}

void RoaringBitmap64::RemoveRange(int64_t min, int64_t max) {
    GetRoaringBitmap(roaring_bitmap_).removeRange(min, max);
}

bool RoaringBitmap64::ContainsAny(int64_t min, int64_t max) const {
    auto iter = EqualOrLarger(min);
    if (iter != End() && *iter < max) {
        return true;
    }
    return false;
}

bool RoaringBitmap64::CheckedAdd(int64_t x) {
    if (Contains(x)) {
        return false;
    }
    Add(x);
    return true;
}

int64_t RoaringBitmap64::Cardinality() const {
    return GetRoaringBitmap(roaring_bitmap_).cardinality();
}

bool RoaringBitmap64::Contains(int64_t x) const {
    return GetRoaringBitmap(roaring_bitmap_).contains(static_cast<uint64_t>(x));
}

bool RoaringBitmap64::IsEmpty() const {
    return GetRoaringBitmap(roaring_bitmap_).isEmpty();
}

size_t RoaringBitmap64::GetSizeInBytes() const {
    return GetRoaringBitmap(roaring_bitmap_).getSizeInBytes();
}

void RoaringBitmap64::Flip(int64_t min, int64_t max) {
    GetRoaringBitmap(roaring_bitmap_).flip(min, max);
}

bool RoaringBitmap64::operator==(const RoaringBitmap64& other) const noexcept {
    if (this == &other) {
        return true;
    }
    assert(roaring_bitmap_ && other.roaring_bitmap_);
    return GetRoaringBitmap(roaring_bitmap_) == GetRoaringBitmap(other.roaring_bitmap_);
}

PAIMON_UNIQUE_PTR<Bytes> RoaringBitmap64::Serialize(MemoryPool* pool) const {
    GetRoaringBitmap(roaring_bitmap_).runOptimize();
    auto& bitmap = GetRoaringBitmap(roaring_bitmap_);
    auto bytes = Bytes::AllocateBytes(bitmap.getSizeInBytes(), pool);
    bitmap.write(bytes->data());
    return bytes;
}

Status RoaringBitmap64::Deserialize(ByteArrayInputStream* input_stream) {
    const char* data = input_stream->GetRawData();
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, input_stream->GetPos());
    PAIMON_ASSIGN_OR_RAISE(int64_t total_length, input_stream->Length());
    try {
        GetRoaringBitmap(roaring_bitmap_) =
            roaring::Roaring64Map::readSafe(data, /*maxbytes*/ total_length - pos);
    } catch (...) {
        return Status::Invalid("catch exception in Deserialize() of RoaringBitmap64");
    }
    return input_stream->Seek(GetRoaringBitmap(roaring_bitmap_).getSizeInBytes(),
                              SeekOrigin::FS_SEEK_CUR);
}

Status RoaringBitmap64::Deserialize(const char* begin, size_t length) {
    try {
        GetRoaringBitmap(roaring_bitmap_) = roaring::Roaring64Map::readSafe(begin, length);
    } catch (...) {
        return Status::Invalid("catch exception in Deserialize() of RoaringBitmap64");
    }
    return Status::OK();
}

std::string RoaringBitmap64::ToString() const {
    return GetRoaringBitmap(roaring_bitmap_).toString();
}

RoaringBitmap64 RoaringBitmap64::And(const RoaringBitmap64& lhs, const RoaringBitmap64& rhs) {
    RoaringBitmap64 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) & GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap64 RoaringBitmap64::Or(const RoaringBitmap64& lhs, const RoaringBitmap64& rhs) {
    RoaringBitmap64 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) | GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap64 RoaringBitmap64::AndNot(const RoaringBitmap64& lhs, const RoaringBitmap64& rhs) {
    RoaringBitmap64 res;
    GetRoaringBitmap(res.roaring_bitmap_) =
        (GetRoaringBitmap(lhs.roaring_bitmap_) - GetRoaringBitmap(rhs.roaring_bitmap_));
    return res;
}

RoaringBitmap64 RoaringBitmap64::From(const std::vector<int64_t>& values) {
    RoaringBitmap64 res;
    for (const auto& value : values) {
        res.Add(value);
    }
    return res;
}

RoaringBitmap64 RoaringBitmap64::FastUnion(const std::vector<const RoaringBitmap64*>& inputs) {
    std::vector<roaring::Roaring64Map*> roaring_inputs;
    roaring_inputs.reserve(inputs.size());
    for (const auto* roaring : inputs) {
        roaring_inputs.push_back(&GetRoaringBitmap(roaring->roaring_bitmap_));
    }
    RoaringBitmap64 res;
    GetRoaringBitmap(res.roaring_bitmap_) = roaring::Roaring64Map::fastunion(
        roaring_inputs.size(), const_cast<const roaring::Roaring64Map**>(roaring_inputs.data()));
    return res;
}

RoaringBitmap64 RoaringBitmap64::FastUnion(const std::vector<RoaringBitmap64>& inputs) {
    std::vector<const RoaringBitmap64*> inputs_ptr;
    inputs_ptr.reserve(inputs.size());
    for (const auto& bitmap : inputs) {
        inputs_ptr.push_back(&bitmap);
    }
    return FastUnion(inputs_ptr);
}
RoaringBitmap64::Iterator RoaringBitmap64::Begin() const {
    return RoaringBitmap64::Iterator(*this);
}

RoaringBitmap64::Iterator RoaringBitmap64::End() const {
    RoaringBitmap64::Iterator iter(*this);
    GetIterator(iter.iterator_) = GetRoaringBitmap(roaring_bitmap_).end();
    return iter;
}

RoaringBitmap64::Iterator RoaringBitmap64::EqualOrLarger(int64_t key) const {
    RoaringBitmap64::Iterator iter(*this);
    bool not_end = GetIterator(iter.iterator_).move(key);
    if (!not_end) {
        return End();
    }
    return iter;
}

}  // namespace paimon
