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

#include "paimon/common/data/variant/generic_variant.h"

#include <cmath>
#include <cstring>
#include <utility>

#include "arrow/util/base64.h"
#include "paimon/common/data/variant/variant_builder.h"
#include "paimon/common/data/variant/variant_defs.h"
#include "paimon/common/data/variant/variant_json_utils.h"

namespace paimon {

namespace {

Status ToJsonImpl(std::string_view value, std::string_view metadata, int32_t pos,
                  const std::string& zone_id, std::string* out) {
    PAIMON_ASSIGN_OR_RAISE(VariantValueType type, VariantBinaryUtil::GetType(value, pos));
    switch (type) {
        case VariantValueType::kObject: {
            PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                                   VariantBinaryUtil::GetObjectInfo(value, pos));
            out->push_back('{');
            for (int32_t i = 0; i < info.num_elements; ++i) {
                PAIMON_ASSIGN_OR_RAISE(int32_t id,
                                       VariantBinaryUtil::ReadUnsigned(
                                           value, info.id_start + info.id_size * i, info.id_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(value, info.offset_start + info.offset_size * i,
                                                    info.offset_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t element_pos,
                    VariantBinaryUtil::CheckedElementPos(info.data_start, offset, value.size()));
                if (i != 0) {
                    out->push_back(',');
                }
                PAIMON_ASSIGN_OR_RAISE(std::string_view key,
                                       VariantBinaryUtil::GetMetadataKey(metadata, id));
                VariantJsonUtils::AppendEscapedJson(key, out);
                out->push_back(':');
                PAIMON_RETURN_NOT_OK(ToJsonImpl(value, metadata, element_pos, zone_id, out));
            }
            out->push_back('}');
            return Status::OK();
        }
        case VariantValueType::kArray: {
            PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ArrayInfo info,
                                   VariantBinaryUtil::GetArrayInfo(value, pos));
            out->push_back('[');
            for (int32_t i = 0; i < info.num_elements; ++i) {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(value, info.offset_start + info.offset_size * i,
                                                    info.offset_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t element_pos,
                    VariantBinaryUtil::CheckedElementPos(info.data_start, offset, value.size()));
                if (i != 0) {
                    out->push_back(',');
                }
                PAIMON_RETURN_NOT_OK(ToJsonImpl(value, metadata, element_pos, zone_id, out));
            }
            out->push_back(']');
            return Status::OK();
        }
        case VariantValueType::kNull:
            out->append("null");
            return Status::OK();
        case VariantValueType::kBoolean: {
            PAIMON_ASSIGN_OR_RAISE(bool b, VariantBinaryUtil::GetBoolean(value, pos));
            out->append(b ? "true" : "false");
            return Status::OK();
        }
        case VariantValueType::kLong: {
            PAIMON_ASSIGN_OR_RAISE(int64_t l, VariantBinaryUtil::GetLong(value, pos));
            out->append(std::to_string(l));
            return Status::OK();
        }
        case VariantValueType::kString: {
            PAIMON_ASSIGN_OR_RAISE(std::string_view s, VariantBinaryUtil::GetString(value, pos));
            VariantJsonUtils::AppendEscapedJson(s, out);
            return Status::OK();
        }
        case VariantValueType::kDouble: {
            PAIMON_ASSIGN_OR_RAISE(double d, VariantBinaryUtil::GetDouble(value, pos));
            std::string repr = VariantJsonUtils::JavaDoubleToString(d);
            if (std::isfinite(d)) {
                out->append(repr);
            } else {
                out->push_back('"');
                out->append(repr);
                out->push_back('"');
            }
            return Status::OK();
        }
        case VariantValueType::kDecimal: {
            PAIMON_ASSIGN_OR_RAISE(VariantDecimal d, VariantBinaryUtil::GetDecimal(value, pos));
            out->append(d.ToPlainString());
            return Status::OK();
        }
        case VariantValueType::kDate: {
            PAIMON_ASSIGN_OR_RAISE(int64_t days, VariantBinaryUtil::GetLong(value, pos));
            out->push_back('"');
            out->append(VariantJsonUtils::DateToString(static_cast<int32_t>(days)));
            out->push_back('"');
            return Status::OK();
        }
        case VariantValueType::kTimestamp: {
            PAIMON_ASSIGN_OR_RAISE(int64_t micros, VariantBinaryUtil::GetLong(value, pos));
            PAIMON_ASSIGN_OR_RAISE(int32_t offset_seconds,
                                   VariantJsonUtils::GetZoneOffsetSeconds(zone_id, micros));
            out->push_back('"');
            out->append(VariantJsonUtils::TimestampToString(micros, offset_seconds, true));
            out->push_back('"');
            return Status::OK();
        }
        case VariantValueType::kTimestampNtz: {
            PAIMON_ASSIGN_OR_RAISE(int64_t micros, VariantBinaryUtil::GetLong(value, pos));
            out->push_back('"');
            out->append(VariantJsonUtils::TimestampToString(micros, 0, false));
            out->push_back('"');
            return Status::OK();
        }
        case VariantValueType::kFloat: {
            PAIMON_ASSIGN_OR_RAISE(float f, VariantBinaryUtil::GetFloat(value, pos));
            std::string repr = VariantJsonUtils::JavaFloatToString(f);
            if (std::isfinite(f)) {
                out->append(repr);
            } else {
                out->push_back('"');
                out->append(repr);
                out->push_back('"');
            }
            return Status::OK();
        }
        case VariantValueType::kBinary: {
            PAIMON_ASSIGN_OR_RAISE(std::string_view binary,
                                   VariantBinaryUtil::GetBinary(value, pos));
            out->push_back('"');
            out->append(arrow::util::base64_encode(binary));
            out->push_back('"');
            return Status::OK();
        }
        case VariantValueType::kUuid: {
            PAIMON_ASSIGN_OR_RAISE(std::string_view uuid, VariantBinaryUtil::GetUuid(value, pos));
            out->push_back('"');
            out->append(VariantBinaryUtil::UuidToString(uuid));
            out->push_back('"');
            return Status::OK();
        }
    }
    return VariantBinaryUtil::MalformedVariant();
}

}  // namespace

GenericVariant::GenericVariant(std::shared_ptr<Bytes> value, std::shared_ptr<Bytes> metadata,
                               int32_t pos)
    : value_(std::move(value)), metadata_(std::move(metadata)), pos_(pos) {}

Result<std::shared_ptr<GenericVariant>> GenericVariant::Create(std::shared_ptr<Bytes> value,
                                                               std::shared_ptr<Bytes> metadata) {
    if (!value || !metadata) {
        return Status::Invalid("variant value and metadata must not be null");
    }
    // There is currently only one allowed version.
    if (metadata->size() < 1 || (static_cast<uint8_t>((*metadata)[0]) &
                                 VariantDefs::kVersionMask) != VariantDefs::kVersion) {
        return VariantBinaryUtil::MalformedVariant();
    }
    // Don't attempt to use a Variant larger than 128 MiB. We'll never produce one, and it risks
    // memory instability.
    if (metadata->size() > static_cast<size_t>(VariantDefs::kSizeLimit) ||
        value->size() > static_cast<size_t>(VariantDefs::kSizeLimit)) {
        return VariantBinaryUtil::VariantConstructorSizeLimit();
    }
    return std::shared_ptr<GenericVariant>(
        new GenericVariant(std::move(value), std::move(metadata), 0));
}

Result<std::shared_ptr<GenericVariant>> GenericVariant::Create(
    std::string_view value, std::string_view metadata, const std::shared_ptr<MemoryPool>& pool) {
    // Reject over-limit inputs before allocating and copying them.
    if (value.size() > static_cast<size_t>(VariantDefs::kSizeLimit) ||
        metadata.size() > static_cast<size_t>(VariantDefs::kSizeLimit)) {
        return VariantBinaryUtil::VariantConstructorSizeLimit();
    }
    std::shared_ptr<Bytes> value_bytes = Bytes::AllocateBytes(value.size(), pool.get());
    if (!value.empty()) {
        std::memcpy(value_bytes->data(), value.data(), value.size());
    }
    std::shared_ptr<Bytes> metadata_bytes = Bytes::AllocateBytes(metadata.size(), pool.get());
    if (!metadata.empty()) {
        // An empty (malformed) metadata view may carry a null data pointer; the size check
        // keeps memcpy away from it.
        std::memcpy(metadata_bytes->data(), metadata.data(), metadata.size());
    }
    return Create(std::move(value_bytes), std::move(metadata_bytes));
}

Result<std::shared_ptr<GenericVariant>> GenericVariant::FromJson(
    std::string_view json, const std::shared_ptr<MemoryPool>& pool) {
    return VariantBuilder::ParseJson(json, /*allow_duplicate_keys=*/false, pool);
}

Result<std::string_view> GenericVariant::Value() const {
    std::string_view raw = RawValue();
    if (pos_ == 0) {
        return raw;
    }
    PAIMON_ASSIGN_OR_RAISE(int32_t size, VariantBinaryUtil::ValueSize(raw, pos_));
    PAIMON_RETURN_NOT_OK(
        VariantBinaryUtil::CheckIndex(pos_ + size - 1, static_cast<int32_t>(raw.size())));
    return raw.substr(pos_, size);
}

std::string_view GenericVariant::RawValue() const {
    return {value_->data(), value_->size()};
}

std::string_view GenericVariant::Metadata() const {
    return {metadata_->data(), metadata_->size()};
}

int64_t GenericVariant::SizeInBytes() const {
    return static_cast<int64_t>(value_->size()) + static_cast<int64_t>(metadata_->size());
}

Result<std::string> GenericVariant::ToJson(const std::string& zone_id) const {
    std::string result;
    PAIMON_RETURN_NOT_OK(ToJsonImpl(RawValue(), Metadata(), pos_, zone_id, &result));
    return result;
}

Result<VariantValueType> GenericVariant::GetType() const {
    return VariantBinaryUtil::GetType(RawValue(), pos_);
}

Result<int32_t> GenericVariant::GetTypeInfo() const {
    return VariantBinaryUtil::GetTypeInfo(RawValue(), pos_);
}

Result<bool> GenericVariant::GetBoolean() const {
    return VariantBinaryUtil::GetBoolean(RawValue(), pos_);
}

Result<int64_t> GenericVariant::GetLong() const {
    return VariantBinaryUtil::GetLong(RawValue(), pos_);
}

Result<double> GenericVariant::GetDouble() const {
    return VariantBinaryUtil::GetDouble(RawValue(), pos_);
}

Result<VariantDecimal> GenericVariant::GetDecimal() const {
    return VariantBinaryUtil::GetDecimal(RawValue(), pos_);
}

Result<float> GenericVariant::GetFloat() const {
    return VariantBinaryUtil::GetFloat(RawValue(), pos_);
}

Result<std::string_view> GenericVariant::GetBinary() const {
    return VariantBinaryUtil::GetBinary(RawValue(), pos_);
}

Result<std::string_view> GenericVariant::GetString() const {
    return VariantBinaryUtil::GetString(RawValue(), pos_);
}

Result<std::string_view> GenericVariant::GetUuid() const {
    return VariantBinaryUtil::GetUuid(RawValue(), pos_);
}

Result<int32_t> GenericVariant::ObjectSize() const {
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                           VariantBinaryUtil::GetObjectInfo(RawValue(), pos_));
    return info.num_elements;
}

std::shared_ptr<GenericVariant> GenericVariant::SubVariant(int32_t pos) const {
    return std::shared_ptr<GenericVariant>(new GenericVariant(value_, metadata_, pos));
}

Result<std::shared_ptr<GenericVariant>> GenericVariant::GetFieldByKey(std::string_view key) const {
    std::string_view raw = RawValue();
    std::string_view metadata = Metadata();
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                           VariantBinaryUtil::GetObjectInfo(raw, pos_));
    // Use linear search for a short list. Switch to binary search when the length reaches
    // `kBinarySearchThreshold`.
    if (info.num_elements < VariantDefs::kBinarySearchThreshold) {
        for (int32_t i = 0; i < info.num_elements; ++i) {
            PAIMON_ASSIGN_OR_RAISE(
                int32_t id, VariantBinaryUtil::ReadUnsigned(raw, info.id_start + info.id_size * i,
                                                            info.id_size));
            PAIMON_ASSIGN_OR_RAISE(std::string_view field_key,
                                   VariantBinaryUtil::GetMetadataKey(metadata, id));
            if (field_key == key) {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(raw, info.offset_start + info.offset_size * i,
                                                    info.offset_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t element_pos,
                    VariantBinaryUtil::CheckedElementPos(info.data_start, offset, raw.size()));
                return SubVariant(element_pos);
            }
        }
    } else {
        int32_t low = 0;
        int32_t high = info.num_elements - 1;
        while (low <= high) {
            // Use an unsigned shift to compute the middle of `low` and `high`, which properly
            // handles the case where `low + high` overflows int32.
            auto mid = static_cast<int32_t>(
                (static_cast<uint32_t>(low) + static_cast<uint32_t>(high)) >> 1);
            PAIMON_ASSIGN_OR_RAISE(
                int32_t id, VariantBinaryUtil::ReadUnsigned(raw, info.id_start + info.id_size * mid,
                                                            info.id_size));
            PAIMON_ASSIGN_OR_RAISE(std::string_view field_key,
                                   VariantBinaryUtil::GetMetadataKey(metadata, id));
            int32_t cmp = field_key.compare(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t offset,
                    VariantBinaryUtil::ReadUnsigned(raw, info.offset_start + info.offset_size * mid,
                                                    info.offset_size));
                PAIMON_ASSIGN_OR_RAISE(
                    int32_t element_pos,
                    VariantBinaryUtil::CheckedElementPos(info.data_start, offset, raw.size()));
                return SubVariant(element_pos);
            }
        }
    }
    return std::shared_ptr<GenericVariant>(nullptr);
}

Result<std::optional<GenericVariant::ObjectField>> GenericVariant::GetFieldAtIndex(
    int32_t index) const {
    std::string_view raw = RawValue();
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                           VariantBinaryUtil::GetObjectInfo(raw, pos_));
    if (index < 0 || index >= info.num_elements) {
        return std::optional<ObjectField>(std::nullopt);
    }
    PAIMON_ASSIGN_OR_RAISE(
        int32_t id,
        VariantBinaryUtil::ReadUnsigned(raw, info.id_start + info.id_size * index, info.id_size));
    PAIMON_ASSIGN_OR_RAISE(
        int32_t offset, VariantBinaryUtil::ReadUnsigned(
                            raw, info.offset_start + info.offset_size * index, info.offset_size));
    PAIMON_ASSIGN_OR_RAISE(std::string_view key, VariantBinaryUtil::GetMetadataKey(Metadata(), id));
    ObjectField field;
    field.key = std::string(key);
    PAIMON_ASSIGN_OR_RAISE(int32_t field_pos, VariantBinaryUtil::CheckedElementPos(
                                                  info.data_start, offset, raw.size()));
    field.value = SubVariant(field_pos);
    return std::optional<ObjectField>(std::move(field));
}

Result<int32_t> GenericVariant::GetDictionaryIdAtIndex(int32_t index) const {
    std::string_view raw = RawValue();
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ObjectInfo info,
                           VariantBinaryUtil::GetObjectInfo(raw, pos_));
    if (index < 0 || index >= info.num_elements) {
        return VariantBinaryUtil::MalformedVariant();
    }
    return VariantBinaryUtil::ReadUnsigned(raw, info.id_start + info.id_size * index, info.id_size);
}

Result<int32_t> GenericVariant::ArraySize() const {
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ArrayInfo info,
                           VariantBinaryUtil::GetArrayInfo(RawValue(), pos_));
    return info.num_elements;
}

Result<std::shared_ptr<GenericVariant>> GenericVariant::GetElementAtIndex(int32_t index) const {
    std::string_view raw = RawValue();
    PAIMON_ASSIGN_OR_RAISE(VariantBinaryUtil::ArrayInfo info,
                           VariantBinaryUtil::GetArrayInfo(raw, pos_));
    if (index < 0 || index >= info.num_elements) {
        return std::shared_ptr<GenericVariant>(nullptr);
    }
    PAIMON_ASSIGN_OR_RAISE(
        int32_t offset, VariantBinaryUtil::ReadUnsigned(
                            raw, info.offset_start + info.offset_size * index, info.offset_size));
    PAIMON_ASSIGN_OR_RAISE(int32_t element_pos, VariantBinaryUtil::CheckedElementPos(
                                                    info.data_start, offset, raw.size()));
    return SubVariant(element_pos);
}

bool GenericVariant::operator==(const GenericVariant& other) const {
    return pos_ == other.pos_ && RawValue() == other.RawValue() && Metadata() == other.Metadata();
}

}  // namespace paimon
