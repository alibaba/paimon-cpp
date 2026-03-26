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

#include "paimon/common/data/binary_string.h"

#include <algorithm>
#include <cctype>

#include "paimon/common/memory/memory_segment_utils.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

const BinaryString& BinaryString::EmptyUtf8() {
    static const BinaryString empty_utf8 = BinaryString();
    return empty_utf8;
}

BinaryString::BinaryString(const MemorySegment& segment, int32_t offset, int32_t size_in_bytes)
    : BinarySection(segment, offset, size_in_bytes) {}

BinaryString::BinaryString() {
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(0, GetDefaultPool().get());
    segment_ = MemorySegment::Wrap(bytes);
    offset_ = 0;
    size_in_bytes_ = bytes->size();
}

BinaryString BinaryString::FromAddress(const MemorySegment& segment, int32_t offset,
                                       int32_t num_bytes) {
    return BinaryString(segment, offset, num_bytes);
}

BinaryString BinaryString::FromString(const std::string& str, MemoryPool* pool) {
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(str, pool);
    return FromBytes(bytes);
}

BinaryString BinaryString::FromBytes(const std::shared_ptr<Bytes>& bytes) {
    return FromBytes(bytes, 0, bytes->size());
}

BinaryString BinaryString::FromBytes(const std::shared_ptr<Bytes>& bytes, int32_t offset,
                                     int32_t num_bytes) {
    return BinaryString(MemorySegment::Wrap(bytes), offset, num_bytes);
}

BinaryString BinaryString::BlankString(int32_t length, MemoryPool* pool) {
    std::shared_ptr<Bytes> spaces = Bytes::AllocateBytes(length, pool);
    std::fill(spaces->data(), spaces->data() + length, ' ');
    return FromBytes(spaces);
}

std::string BinaryString::ToString() const {
    std::string ret(size_in_bytes_, '\0');
    MemorySegmentUtils::CopyToBytes({segment_}, offset_, &ret, 0, size_in_bytes_);
    return ret;
}

int32_t BinaryString::NumBytesForFirstByte(char b) {
    if (b >= 0) {
        // 1 byte, 7 bits: 0xxxxxxx
        return 1;
    } else if ((b >> 5) == -2 && (b & 0x1e) != 0) {
        // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
        return 2;
    } else if ((b >> 4) == -2) {
        // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
        return 3;
    } else if ((b >> 3) == -2) {
        // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        return 4;
    } else {
        // Skip the first byte disallowed in UTF-8
        // Handling errors quietly, same semantics to java String.
        return 1;
    }
}

int32_t BinaryString::CompareTo(const BinaryString& other) const {
    int32_t len = std::min(size_in_bytes_, other.size_in_bytes_);
    for (int32_t i = 0; i < len; i++) {
        int32_t res =
            (segment_.Get(offset_ + i) & 0xFF) - (other.segment_.Get(other.offset_ + i) & 0xFF);
        if (res != 0) {
            return res;
        }
    }
    return size_in_bytes_ - other.size_in_bytes_;
}

int32_t BinaryString::NumChars() const {
    int32_t len = 0;
    for (int32_t i = 0; i < size_in_bytes_; i += NumBytesForFirstByte(GetByteOneSegment(i))) {
        len++;
    }
    return len;
}

char BinaryString::ByteAt(int32_t index) const {
    return segment_.Get(offset_ + index);
}

BinaryString BinaryString::Copy(MemoryPool* pool) const {
    std::shared_ptr<Bytes> copy =
        MemorySegmentUtils::CopyToBytes({segment_}, offset_, size_in_bytes_, pool);
    return FromBytes(copy);
}

BinaryString BinaryString::Substring(int32_t begin_index, int32_t end_index,
                                     MemoryPool* pool) const {
    if (end_index <= begin_index || begin_index >= size_in_bytes_) {
        return EmptyUtf8();
    }
    int32_t i = 0;
    int32_t c = 0;
    while (i < size_in_bytes_ && c < begin_index) {
        i += NumBytesForFirstByte(segment_.Get(i + offset_));
        c += 1;
    }

    int32_t j = i;
    while (i < size_in_bytes_ && c < end_index) {
        i += NumBytesForFirstByte(segment_.Get(i + offset_));
        c += 1;
    }

    if (i > j) {
        std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(i - j, pool);
        segment_.Get(offset_ + j, bytes.get(), 0, i - j);
        return FromBytes(bytes);
    } else {
        return EmptyUtf8();
    }
}

bool BinaryString::Contains(const BinaryString& s) const {
    if (s.size_in_bytes_ == 0) {
        return true;
    }
    int32_t find = MemorySegmentUtils::Find({segment_}, offset_, size_in_bytes_, {s.segment_},
                                            s.offset_, s.size_in_bytes_);
    return find != -1;
}

bool BinaryString::StartsWith(const BinaryString& prefix) const {
    return MatchAt(prefix, 0);
}

bool BinaryString::EndsWith(const BinaryString& suffix) const {
    return MatchAt(suffix, size_in_bytes_ - suffix.size_in_bytes_);
}

int32_t BinaryString::IndexOf(const BinaryString& str, int32_t from_index) const {
    if (str.size_in_bytes_ == 0) {
        return 0;
    }
    // position in byte
    int32_t byte_idx = 0;
    // position is char
    int32_t char_idx = 0;
    while (byte_idx < size_in_bytes_ && char_idx < from_index) {
        byte_idx += NumBytesForFirstByte(GetByteOneSegment(byte_idx));
        char_idx++;
    }
    do {
        if (byte_idx + str.size_in_bytes_ > size_in_bytes_) {
            return -1;
        }
        if (MemorySegmentUtils::Equals({segment_}, offset_ + byte_idx, {str.segment_}, str.offset_,
                                       str.size_in_bytes_)) {
            return char_idx;
        }
        byte_idx += NumBytesForFirstByte(GetByteOneSegment(byte_idx));
        char_idx++;
    } while (byte_idx < size_in_bytes_);

    return -1;
}

BinaryString BinaryString::ToUpperCase(MemoryPool* pool) const {
    if (size_in_bytes_ == 0) {
        return EmptyUtf8();
    }
    int32_t size = segment_.Size();
    SegmentAndOffset segment_and_offset = StartSegmentAndOffset(size);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes_, pool);
    (*bytes)[0] = tolower(segment_and_offset.Value());
    for (int32_t i = 0; i < size_in_bytes_; i++) {
        char b = segment_and_offset.Value();
        if (NumBytesForFirstByte(b) != 1) {
            // fallback
            return CppToUpperCase(pool);
        }
        int32_t upper = toupper(static_cast<int32_t>(b));
        if (upper > 127) {
            // fallback
            return CppToUpperCase(pool);
        }
        (*bytes)[i] = static_cast<char>(upper);
        segment_and_offset.NextByte(size);
    }
    return FromBytes(bytes);
}

BinaryString BinaryString::CppToUpperCase(MemoryPool* pool) const {
    std::string str = ToString();
    std::transform(str.begin(), str.end(), str.begin(), ::toupper);
    return FromString(str, pool);
}

BinaryString BinaryString::ToLowerCase(MemoryPool* pool) const {
    if (size_in_bytes_ == 0) {
        return EmptyUtf8();
    }
    int32_t size = segment_.Size();
    SegmentAndOffset segment_and_offset = StartSegmentAndOffset(size);
    std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(size_in_bytes_, pool);
    (*bytes)[0] = tolower(segment_and_offset.Value());
    for (int32_t i = 0; i < size_in_bytes_; i++) {
        char b = segment_and_offset.Value();
        if (NumBytesForFirstByte(b) != 1) {
            // fallback
            return CppToLowerCase(pool);
        }
        int32_t lower = tolower(static_cast<int32_t>(b));
        if (lower > 127) {
            // fallback
            return CppToLowerCase(pool);
        }
        (*bytes)[i] = static_cast<char>(lower);
        segment_and_offset.NextByte(size);
    }
    return FromBytes(bytes);
}

BinaryString BinaryString::CppToLowerCase(MemoryPool* pool) const {
    std::string str = ToString();
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    return FromString(str, pool);
}

char BinaryString::GetByteOneSegment(int32_t i) const {
    return segment_.Get(offset_ + i);
}

BinaryString::SegmentAndOffset BinaryString::StartSegmentAndOffset(int32_t seg_size) const {
    return BinaryString::SegmentAndOffset(segment_, offset_);
}

BinaryString BinaryString::CopyBinaryString(int32_t start, int32_t end, MemoryPool* pool) const {
    int32_t len = end - start + 1;
    std::shared_ptr<Bytes> new_bytes = Bytes::AllocateBytes(len, pool);
    MemorySegmentUtils::CopyToBytes({segment_}, offset_ + start, new_bytes.get(), 0, len);
    return FromBytes(new_bytes);
}

bool BinaryString::MatchAt(const BinaryString& s, int32_t pos) const {
    return MatchAtOneSeg(s, pos);
}

bool BinaryString::MatchAtOneSeg(const BinaryString& s, int32_t pos) const {
    return s.size_in_bytes_ + pos <= size_in_bytes_ && pos >= 0 &&
           segment_.EqualTo(s.segment_, offset_ + pos, s.offset_, s.size_in_bytes_);
}

std::string_view BinaryString::GetStringView() const {
    const auto* bytes = segment_.GetArray();
    assert(bytes);
    return std::string_view(bytes->data() + offset_, size_in_bytes_);
}
}  // namespace paimon
