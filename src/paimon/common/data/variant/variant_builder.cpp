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

#include "paimon/common/data/variant/variant_builder.h"

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <cstring>

#include "fmt/format.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "rapidjson/error/en.h"
#include "rapidjson/memorystream.h"
#include "rapidjson/reader.h"

namespace paimon {

namespace {

// A rapidjson SAX handler that feeds parsed JSON events into a `VariantBuilder`, mirroring
// `GenericVariantBuilder.buildJson` in the Java implementation. Numbers are delivered as raw
// text (`kParseNumbersAsStringsFlag`) so that exact decimal semantics match Jackson's.
class JsonToVariantHandler
    : public rapidjson::BaseReaderHandler<rapidjson::UTF8<>, JsonToVariantHandler> {
 public:
    explicit JsonToVariantHandler(VariantBuilder* builder) : builder_(builder) {}

    bool Null() {
        return Ok(BeforeValue()) && Ok(builder_->AppendNull());
    }

    bool Bool(bool b) {
        return Ok(BeforeValue()) && Ok(builder_->AppendBoolean(b));
    }

    bool RawNumber(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        return Ok(BeforeValue()) && Ok(AppendNumber(std::string_view(str, length)));
    }

    bool String(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        return Ok(BeforeValue()) && Ok(builder_->AppendString(std::string_view(str, length)));
    }

    bool StartObject() {
        if (!Ok(BeforeValue())) {
            return false;
        }
        contexts_.emplace_back(Context{true, builder_->GetWritePos(), {}, {}, {}});
        return true;
    }

    bool Key(const char* str, rapidjson::SizeType length, bool /*copy*/) {
        contexts_.back().pending_key.assign(str, length);
        return true;
    }

    bool EndObject(rapidjson::SizeType /*member_count*/) {
        Context context = std::move(contexts_.back());
        contexts_.pop_back();
        return Ok(builder_->FinishWritingObject(context.start, &context.fields));
    }

    bool StartArray() {
        if (!Ok(BeforeValue())) {
            return false;
        }
        contexts_.emplace_back(Context{false, builder_->GetWritePos(), {}, {}, {}});
        return true;
    }

    bool EndArray(rapidjson::SizeType /*element_count*/) {
        Context context = std::move(contexts_.back());
        contexts_.pop_back();
        return Ok(builder_->FinishWritingArray(context.start, context.offsets));
    }

    const Status& status() const {
        return status_;
    }

 private:
    struct Context {
        bool is_object;
        int32_t start;
        std::vector<VariantBuilder::FieldEntry> fields;
        std::vector<int32_t> offsets;
        std::string pending_key;
    };

    bool Ok(const Status& status) {
        if (!status.ok()) {
            status_ = status;
            return false;
        }
        return true;
    }

    // Records the offset of the value that is about to be appended in the enclosing container.
    Status BeforeValue() {
        if (contexts_.empty()) {
            return Status::OK();
        }
        Context& top = contexts_.back();
        int32_t offset = builder_->GetWritePos() - top.start;
        if (top.is_object) {
            int32_t id = builder_->AddKey(top.pending_key);
            top.fields.emplace_back(top.pending_key, id, offset);
        } else {
            top.offsets.push_back(offset);
        }
        return Status::OK();
    }

    // Mirrors the Java number handling: integers that fit in a long are appended as long;
    // everything else is first tried as an exact decimal and falls back to double.
    Status AppendNumber(std::string_view text) {
        bool integral = true;
        for (char c : text) {
            if (c != '-' && !(c >= '0' && c <= '9')) {
                integral = false;
                break;
            }
        }
        if (integral) {
            int64_t long_value = 0;
            auto [ptr, ec] = std::from_chars(text.data(), text.data() + text.size(), long_value);
            if (ec == std::errc() && ptr == text.data() + text.size()) {
                return builder_->AppendLong(long_value);
            }
        }
        PAIMON_ASSIGN_OR_RAISE(bool appended, TryAppendDecimal(text));
        if (appended) {
            return Status::OK();
        }
        char* end = nullptr;
        std::string text_copy(text);
        double double_value = std::strtod(text_copy.c_str(), &end);
        if (end != text_copy.c_str() + text_copy.size()) {
            return Status::Invalid(fmt::format("Invalid JSON number: {}", text));
        }
        return builder_->AppendDouble(double_value);
    }

    // Tries to append a JSON number as an exact decimal. Returns whether it succeeded. The input
    // must only use the decimal format (an integer value with an optional '.' in it) and must not
    // use scientific notation. It also must fit into the precision limitation of decimal types.
    Result<bool> TryAppendDecimal(std::string_view text) {
        for (char c : text) {
            if (c != '-' && c != '.' && !(c >= '0' && c <= '9')) {
                return false;
            }
        }
        bool negative = false;
        size_t i = 0;
        if (i < text.size() && text[i] == '-') {
            negative = true;
            ++i;
        }
        __int128_t unscaled = 0;
        int32_t scale = 0;
        int32_t significant_digits = 0;
        bool seen_point = false;
        bool seen_nonzero = false;
        for (; i < text.size(); ++i) {
            char c = text[i];
            if (c == '.') {
                seen_point = true;
                continue;
            }
            if (seen_point) {
                ++scale;
            }
            if (c != '0' || seen_nonzero) {
                seen_nonzero = true;
                ++significant_digits;
            }
            if (significant_digits > VariantDefs::kMaxDecimal16Precision) {
                return false;
            }
            unscaled = unscaled * 10 + (c - '0');
        }
        if (scale > VariantDefs::kMaxDecimal16Precision) {
            return false;
        }
        VariantDecimal decimal;
        decimal.unscaled = negative ? -unscaled : unscaled;
        decimal.scale = scale;
        PAIMON_RETURN_NOT_OK(builder_->AppendDecimal(decimal));
        return true;
    }

    VariantBuilder* builder_;
    std::vector<Context> contexts_;
    Status status_;
};

}  // namespace

Result<std::shared_ptr<GenericVariant>> VariantBuilder::ParseJson(
    std::string_view json, bool allow_duplicate_keys, const std::shared_ptr<MemoryPool>& pool) {
    VariantBuilder builder(allow_duplicate_keys);
    JsonToVariantHandler handler(&builder);
    rapidjson::Reader reader;
    rapidjson::MemoryStream stream(json.data(), json.size());
    rapidjson::ParseResult result =
        reader.Parse<rapidjson::kParseNumbersAsStringsFlag>(stream, handler);
    if (!result) {
        if (!handler.status().ok()) {
            return handler.status();
        }
        return Status::Invalid(fmt::format("Failed to parse JSON: {} (at offset {})",
                                           rapidjson::GetParseError_En(result.Code()),
                                           result.Offset()));
    }
    return builder.Build(pool);
}

Result<std::shared_ptr<GenericVariant>> VariantBuilder::Build(
    const std::shared_ptr<MemoryPool>& pool) {
    auto num_keys = static_cast<int32_t>(dictionary_keys_.size());
    // Use int64 to avoid overflow in accumulating lengths.
    int64_t dictionary_string_size = 0;
    for (const std::string& key : dictionary_keys_) {
        dictionary_string_size += static_cast<int64_t>(key.size());
    }
    // Determine the number of bytes required per offset entry. The largest offset is the
    // one-past-the-end value, which is the total string size. It's very unlikely that the number
    // of keys could be larger, but incorporate that into the calculation in case of pathological
    // data.
    int64_t max_size = std::max(dictionary_string_size, static_cast<int64_t>(num_keys));
    if (max_size > VariantDefs::kSizeLimit) {
        return Status::Invalid("VARIANT_SIZE_LIMIT");
    }
    int32_t offset_size = GetIntegerSize(static_cast<int32_t>(max_size));

    int32_t offset_start = 1 + offset_size;
    int32_t string_start = offset_start + (num_keys + 1) * offset_size;
    int64_t metadata_size = string_start + dictionary_string_size;
    if (metadata_size > VariantDefs::kSizeLimit) {
        return Status::Invalid("VARIANT_SIZE_LIMIT");
    }

    std::shared_ptr<Bytes> metadata =
        Bytes::AllocateBytes(static_cast<size_t>(metadata_size), pool.get());
    auto* metadata_data = reinterpret_cast<uint8_t*>(metadata->data());
    int32_t header_byte = VariantDefs::kVersion | ((offset_size - 1) << 6);
    VariantBinaryUtil::WriteLong(metadata_data, 0, header_byte, 1);
    VariantBinaryUtil::WriteLong(metadata_data, 1, num_keys, offset_size);
    int32_t current_offset = 0;
    for (int32_t i = 0; i < num_keys; ++i) {
        VariantBinaryUtil::WriteLong(metadata_data, offset_start + i * offset_size, current_offset,
                                     offset_size);
        const std::string& key = dictionary_keys_[i];
        memcpy(metadata_data + string_start + current_offset, key.data(), key.size());
        current_offset += static_cast<int32_t>(key.size());
    }
    VariantBinaryUtil::WriteLong(metadata_data, offset_start + num_keys * offset_size,
                                 current_offset, offset_size);

    std::shared_ptr<Bytes> value =
        Bytes::AllocateBytes(static_cast<size_t>(write_pos_), pool.get());
    memcpy(value->data(), write_buffer_.data(), static_cast<size_t>(write_pos_));
    return GenericVariant::Create(std::move(value), std::move(metadata));
}

Status VariantBuilder::AppendString(std::string_view str) {
    bool long_str = static_cast<int32_t>(str.size()) > VariantDefs::kMaxShortStrSize;
    PAIMON_RETURN_NOT_OK(CheckCapacity((long_str ? 1 + VariantDefs::kU32Size : 1) +
                                       static_cast<int32_t>(str.size())));
    if (long_str) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kLongStr);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_,
                                     static_cast<int64_t>(str.size()), VariantDefs::kU32Size);
        write_pos_ += VariantDefs::kU32Size;
    } else {
        write_buffer_[write_pos_++] =
            VariantBinaryUtil::ShortStrHeader(static_cast<int32_t>(str.size()));
    }
    memcpy(write_buffer_.data() + write_pos_, str.data(), str.size());
    write_pos_ += static_cast<int32_t>(str.size());
    return Status::OK();
}

Status VariantBuilder::AppendNull() {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kNull);
    return Status::OK();
}

Status VariantBuilder::AppendBoolean(bool b) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1));
    write_buffer_[write_pos_++] =
        VariantBinaryUtil::PrimitiveHeader(b ? VariantDefs::kTrue : VariantDefs::kFalse);
    return Status::OK();
}

Status VariantBuilder::AppendLong(int64_t l) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 8));
    if (l == static_cast<int8_t>(l)) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kInt1);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, l, 1);
        write_pos_ += 1;
    } else if (l == static_cast<int16_t>(l)) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kInt2);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, l, 2);
        write_pos_ += 2;
    } else if (l == static_cast<int32_t>(l)) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kInt4);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, l, 4);
        write_pos_ += 4;
    } else {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kInt8);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, l, 8);
        write_pos_ += 8;
    }
    return Status::OK();
}

Status VariantBuilder::AppendDouble(double d) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 8));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kDouble);
    int64_t bits;
    memcpy(&bits, &d, sizeof(bits));
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, bits, 8);
    write_pos_ += 8;
    return Status::OK();
}

Status VariantBuilder::AppendDecimal(const VariantDecimal& d) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(2 + 16));
    int32_t precision = d.Precision();
    if (d.scale < 0 || d.scale > VariantDefs::kMaxDecimal16Precision ||
        precision > VariantDefs::kMaxDecimal16Precision) {
        return Status::Invalid(
            fmt::format("Decimal precision {} and scale {} must fit into the variant decimal "
                        "limit {}",
                        precision, d.scale, VariantDefs::kMaxDecimal16Precision));
    }
    if (d.scale <= VariantDefs::kMaxDecimal4Precision &&
        precision <= VariantDefs::kMaxDecimal4Precision) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kDecimal4);
        write_buffer_[write_pos_++] = static_cast<uint8_t>(d.scale);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_,
                                     static_cast<int64_t>(d.unscaled), 4);
        write_pos_ += 4;
    } else if (d.scale <= VariantDefs::kMaxDecimal8Precision &&
               precision <= VariantDefs::kMaxDecimal8Precision) {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kDecimal8);
        write_buffer_[write_pos_++] = static_cast<uint8_t>(d.scale);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_,
                                     static_cast<int64_t>(d.unscaled), 8);
        write_pos_ += 8;
    } else {
        write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kDecimal16);
        write_buffer_[write_pos_++] = static_cast<uint8_t>(d.scale);
        auto bits = static_cast<__uint128_t>(d.unscaled);
        for (int32_t i = 0; i < 16; ++i) {
            write_buffer_[write_pos_ + i] = static_cast<uint8_t>((bits >> (8 * i)) & 0xFF);
        }
        write_pos_ += 16;
    }
    return Status::OK();
}

Status VariantBuilder::AppendDate(int32_t days_since_epoch) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 4));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kDate);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, days_since_epoch, 4);
    write_pos_ += 4;
    return Status::OK();
}

Status VariantBuilder::AppendTimestamp(int64_t micros_since_epoch) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 8));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kTimestamp);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, micros_since_epoch, 8);
    write_pos_ += 8;
    return Status::OK();
}

Status VariantBuilder::AppendTimestampNtz(int64_t micros_since_epoch) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 8));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kTimestampNtz);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, micros_since_epoch, 8);
    write_pos_ += 8;
    return Status::OK();
}

Status VariantBuilder::AppendFloat(float f) {
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 4));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kFloat);
    int32_t bits;
    memcpy(&bits, &f, sizeof(bits));
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_, bits, 4);
    write_pos_ += 4;
    return Status::OK();
}

Status VariantBuilder::AppendBinary(std::string_view binary) {
    PAIMON_RETURN_NOT_OK(
        CheckCapacity(1 + VariantDefs::kU32Size + static_cast<int32_t>(binary.size())));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kBinary);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), write_pos_,
                                 static_cast<int64_t>(binary.size()), VariantDefs::kU32Size);
    write_pos_ += VariantDefs::kU32Size;
    memcpy(write_buffer_.data() + write_pos_, binary.data(), binary.size());
    write_pos_ += static_cast<int32_t>(binary.size());
    return Status::OK();
}

Status VariantBuilder::AppendUuid(std::string_view uuid_bytes) {
    if (uuid_bytes.size() != 16) {
        return Status::Invalid("UUID must be 16 bytes");
    }
    PAIMON_RETURN_NOT_OK(CheckCapacity(1 + 16));
    write_buffer_[write_pos_++] = VariantBinaryUtil::PrimitiveHeader(VariantDefs::kUuid);
    // UUID is stored big-endian, so don't use WriteLong.
    memcpy(write_buffer_.data() + write_pos_, uuid_bytes.data(), 16);
    write_pos_ += 16;
    return Status::OK();
}

int32_t VariantBuilder::AddKey(std::string_view key) {
    auto it = dictionary_.find(std::string(key));
    if (it != dictionary_.end()) {
        return it->second;
    }
    auto id = static_cast<int32_t>(dictionary_keys_.size());
    dictionary_.emplace(std::string(key), id);
    dictionary_keys_.emplace_back(key);
    return id;
}

Status VariantBuilder::FinishWritingObject(int32_t start, std::vector<FieldEntry>* fields) {
    auto size = static_cast<int32_t>(fields->size());
    std::sort(fields->begin(), fields->end(),
              [](const FieldEntry& a, const FieldEntry& b) { return a.key < b.key; });
    int32_t max_id = size == 0 ? 0 : (*fields)[0].id;
    if (allow_duplicate_keys_) {
        int32_t distinct_pos = 0;
        // Maintain a list of distinct keys in-place.
        for (int32_t i = 1; i < size; ++i) {
            max_id = std::max(max_id, (*fields)[i].id);
            if ((*fields)[i].id == (*fields)[i - 1].id) {
                // Found a duplicate key. Keep the field with a greater offset.
                if ((*fields)[distinct_pos].offset < (*fields)[i].offset) {
                    (*fields)[distinct_pos].offset = (*fields)[i].offset;
                }
            } else {
                // Found a distinct key. Add the field to the list.
                ++distinct_pos;
                (*fields)[distinct_pos] = (*fields)[i];
            }
        }
        if (distinct_pos + 1 < size) {
            size = distinct_pos + 1;
            fields->erase(fields->begin() + size, fields->end());
            // Sort the fields by offsets so that we can move the value data of each field to the
            // new offset without overwriting the fields after it.
            std::sort(fields->begin(), fields->end(),
                      [](const FieldEntry& a, const FieldEntry& b) { return a.offset < b.offset; });
            int32_t current_offset = 0;
            for (int32_t i = 0; i < size; ++i) {
                int32_t old_offset = (*fields)[i].offset;
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t field_size,
                    VariantBinaryUtil::ValueSize(
                        std::string_view(reinterpret_cast<const char*>(write_buffer_.data()),
                                         static_cast<size_t>(write_pos_)),
                        start + old_offset));
                memmove(write_buffer_.data() + start + current_offset,
                        write_buffer_.data() + start + old_offset, static_cast<size_t>(field_size));
                (*fields)[i].offset = current_offset;
                current_offset += field_size;
            }
            write_pos_ = start + current_offset;
            // Change back to the sort order by field keys to meet the variant spec.
            std::sort(fields->begin(), fields->end(),
                      [](const FieldEntry& a, const FieldEntry& b) { return a.key < b.key; });
        }
    } else {
        for (int32_t i = 1; i < size; ++i) {
            max_id = std::max(max_id, (*fields)[i].id);
            if ((*fields)[i].key == (*fields)[i - 1].key) {
                return Status::Invalid("VARIANT_DUPLICATE_KEY");
            }
        }
    }
    int32_t data_size = write_pos_ - start;
    bool large_size = size > VariantDefs::kU8Max;
    int32_t size_bytes = large_size ? VariantDefs::kU32Size : 1;
    int32_t id_size = GetIntegerSize(max_id);
    int32_t offset_size = GetIntegerSize(data_size);
    // The space for the header byte, object size, id list, and offset list.
    int32_t header_size = 1 + size_bytes + size * id_size + (size + 1) * offset_size;
    PAIMON_RETURN_NOT_OK(CheckCapacity(header_size));
    // Shift the just-written field data to make room for the object header section.
    memmove(write_buffer_.data() + start + header_size, write_buffer_.data() + start,
            static_cast<size_t>(data_size));
    write_pos_ += header_size;
    write_buffer_[start] = VariantBinaryUtil::ObjectHeader(large_size, id_size, offset_size);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), start + 1, size, size_bytes);
    int32_t id_start = start + 1 + size_bytes;
    int32_t offset_start = id_start + size * id_size;
    for (int32_t i = 0; i < size; ++i) {
        VariantBinaryUtil::WriteLong(write_buffer_.data(), id_start + i * id_size, (*fields)[i].id,
                                     id_size);
        VariantBinaryUtil::WriteLong(write_buffer_.data(), offset_start + i * offset_size,
                                     (*fields)[i].offset, offset_size);
    }
    VariantBinaryUtil::WriteLong(write_buffer_.data(), offset_start + size * offset_size, data_size,
                                 offset_size);
    return Status::OK();
}

Status VariantBuilder::FinishWritingArray(int32_t start, const std::vector<int32_t>& offsets) {
    int32_t data_size = write_pos_ - start;
    auto size = static_cast<int32_t>(offsets.size());
    bool large_size = size > VariantDefs::kU8Max;
    int32_t size_bytes = large_size ? VariantDefs::kU32Size : 1;
    int32_t offset_size = GetIntegerSize(data_size);
    // The space for the header byte, array size, and offset list.
    int32_t header_size = 1 + size_bytes + (size + 1) * offset_size;
    PAIMON_RETURN_NOT_OK(CheckCapacity(header_size));
    // Shift the just-written element data to make room for the header section.
    memmove(write_buffer_.data() + start + header_size, write_buffer_.data() + start,
            static_cast<size_t>(data_size));
    write_pos_ += header_size;
    write_buffer_[start] = VariantBinaryUtil::ArrayHeader(large_size, offset_size);
    VariantBinaryUtil::WriteLong(write_buffer_.data(), start + 1, size, size_bytes);
    int32_t offset_start = start + 1 + size_bytes;
    for (int32_t i = 0; i < size; ++i) {
        VariantBinaryUtil::WriteLong(write_buffer_.data(), offset_start + i * offset_size,
                                     offsets[i], offset_size);
    }
    VariantBinaryUtil::WriteLong(write_buffer_.data(), offset_start + size * offset_size, data_size,
                                 offset_size);
    return Status::OK();
}

Status VariantBuilder::AppendVariant(const GenericVariant& v) {
    return AppendVariantImpl(v.RawValue(), v.Metadata(), v.Pos());
}

Status VariantBuilder::AppendVariantImpl(std::string_view value, std::string_view metadata,
                                         int32_t pos) {
    PAIMON_RETURN_NOT_OK(VariantBinaryUtil::CheckIndex(pos, static_cast<int32_t>(value.size())));
    int32_t basic_type = static_cast<uint8_t>(value[pos]) & VariantDefs::kBasicTypeMask;
    switch (basic_type) {
        case VariantDefs::kObject: {
            PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                                   VariantBinaryUtil::GetObjectInfo(value, pos));
            std::vector<FieldEntry> fields;
            fields.reserve(info.num_elements);
            int32_t start = write_pos_;
            for (int32_t i = 0; i < info.num_elements; ++i) {
                PAIMON_ASSIGN_OR_RAISE(int32_t id,
                                       VariantBinaryUtil::ReadUnsigned(
                                           value, info.id_start + info.id_size * i, info.id_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(value, info.offset_start + info.offset_size * i,
                                                    info.offset_size));
                int32_t element_pos = info.data_start + offset;
                PAIMON_ASSIGN_OR_RAISE(std::string_view key,
                                       VariantBinaryUtil::GetMetadataKey(metadata, id));
                int32_t new_id = AddKey(key);
                fields.emplace_back(std::string(key), new_id, write_pos_ - start);
                PAIMON_RETURN_NOT_OK(AppendVariantImpl(value, metadata, element_pos));
            }
            return FinishWritingObject(start, &fields);
        }
        case VariantDefs::kArray: {
            PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ArrayInfo info,
                                   VariantBinaryUtil::GetArrayInfo(value, pos));
            std::vector<int32_t> offsets;
            offsets.reserve(info.num_elements);
            int32_t start = write_pos_;
            for (int32_t i = 0; i < info.num_elements; ++i) {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(value, info.offset_start + info.offset_size * i,
                                                    info.offset_size));
                int32_t element_pos = info.data_start + offset;
                offsets.push_back(write_pos_ - start);
                PAIMON_RETURN_NOT_OK(AppendVariantImpl(value, metadata, element_pos));
            }
            return FinishWritingArray(start, offsets);
        }
        default:
            return ShallowAppendVariant(value, pos);
    }
}

Status VariantBuilder::ShallowAppendVariant(std::string_view value, int32_t pos) {
    PAIMON_ASSIGN_OR_RAISE(int32_t size, VariantBinaryUtil::ValueSize(value, pos));
    PAIMON_RETURN_NOT_OK(
        VariantBinaryUtil::CheckIndex(pos + size - 1, static_cast<int32_t>(value.size())));
    PAIMON_RETURN_NOT_OK(CheckCapacity(size));
    memcpy(write_buffer_.data() + write_pos_, value.data() + pos, static_cast<size_t>(size));
    write_pos_ += size;
    return Status::OK();
}

Status VariantBuilder::CheckCapacity(int32_t additional) {
    int32_t required = write_pos_ + additional;
    if (required > static_cast<int32_t>(write_buffer_.size())) {
        // Allocate a new buffer with a capacity of the next power of 2 of `required`.
        auto new_capacity = static_cast<int32_t>(write_buffer_.size());
        while (new_capacity < required) {
            new_capacity *= 2;
            if (new_capacity > VariantDefs::kSizeLimit) {
                return Status::Invalid("VARIANT_SIZE_LIMIT");
            }
        }
        write_buffer_.resize(static_cast<size_t>(new_capacity));
    }
    return Status::OK();
}

int32_t VariantBuilder::GetIntegerSize(int32_t value) {
    if (value <= VariantDefs::kU8Max) {
        return 1;
    }
    if (value <= VariantDefs::kU16Max) {
        return 2;
    }
    if (value <= VariantDefs::kU24Max) {
        return 3;
    }
    return 4;
}

}  // namespace paimon
