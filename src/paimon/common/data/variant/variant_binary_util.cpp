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

#include "paimon/common/data/variant/variant_binary_util.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "fmt/format.h"
#include "paimon/common/data/variant/variant_defs.h"

namespace paimon {

int32_t VariantDecimal::Precision() const {
    __uint128_t abs_value =
        unscaled < 0 ? -static_cast<__uint128_t>(unscaled) : static_cast<__uint128_t>(unscaled);
    int32_t digits = 1;
    while (abs_value >= 10) {
        abs_value /= 10;
        ++digits;
    }
    return digits;
}

VariantDecimal VariantDecimal::StripTrailingZeros() const {
    VariantDecimal result = *this;
    if (result.unscaled == 0) {
        result.scale = 0;
        return result;
    }
    while (result.unscaled % 10 == 0) {
        result.unscaled /= 10;
        --result.scale;
    }
    return result;
}

std::string VariantDecimal::ToPlainString() const {
    __uint128_t abs_value =
        unscaled < 0 ? -static_cast<__uint128_t>(unscaled) : static_cast<__uint128_t>(unscaled);
    std::string digits;
    if (abs_value == 0) {
        digits = "0";
    } else {
        while (abs_value > 0) {
            digits.push_back(static_cast<char>('0' + static_cast<int32_t>(abs_value % 10)));
            abs_value /= 10;
        }
        std::reverse(digits.begin(), digits.end());
    }
    std::string result;
    if (unscaled < 0) {
        result.push_back('-');
    }
    if (scale <= 0) {
        result.append(digits);
        result.append(static_cast<size_t>(-scale), '0');
    } else if (static_cast<size_t>(scale) < digits.size()) {
        result.append(digits, 0, digits.size() - scale);
        result.push_back('.');
        result.append(digits, digits.size() - scale, scale);
    } else {
        result.append("0.");
        result.append(static_cast<size_t>(scale) - digits.size(), '0');
        result.append(digits);
    }
    return result;
}

Status VariantBinaryUtil::MalformedVariant(const std::string& message) {
    if (message.empty()) {
        return Status::Invalid("MALFORMED_VARIANT");
    }
    return Status::Invalid(fmt::format("MALFORMED_VARIANT: {}", message));
}

Status VariantBinaryUtil::UnknownPrimitiveTypeInVariant(int32_t id) {
    return Status::Invalid(fmt::format("UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT, id: {}", id));
}

Status VariantBinaryUtil::VariantConstructorSizeLimit() {
    return Status::Invalid("VARIANT_CONSTRUCTOR_SIZE_LIMIT");
}

Status VariantBinaryUtil::UnexpectedType(VariantValueType type) {
    static constexpr const char* kTypeNames[] = {
        "OBJECT",  "ARRAY", "NULL",      "BOOLEAN",       "LONG",  "STRING", "DOUBLE",
        "DECIMAL", "DATE",  "TIMESTAMP", "TIMESTAMP_NTZ", "FLOAT", "BINARY", "UUID"};
    return Status::Invalid(
        fmt::format("Expect type to be {}", kTypeNames[static_cast<int32_t>(type)]));
}

Status VariantBinaryUtil::CheckIndex(int32_t pos, int32_t length) {
    if (pos < 0 || pos >= length) {
        return MalformedVariant(
            fmt::format("index {} is out of bounds for a buffer of {} bytes", pos, length));
    }
    return Status::OK();
}

void VariantBinaryUtil::WriteLong(int64_t value, int32_t num_bytes, uint8_t* bytes, int32_t pos) {
    for (int32_t i = 0; i < num_bytes; ++i) {
        bytes[pos + i] = static_cast<uint8_t>((static_cast<uint64_t>(value) >> (8 * i)) & 0xFF);
    }
}

Result<int64_t> VariantBinaryUtil::ReadLong(std::string_view bytes, int32_t pos,
                                            int32_t num_bytes) {
    auto length = static_cast<int32_t>(bytes.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    PAIMON_RETURN_NOT_OK(CheckIndex(pos + num_bytes - 1, length));
    uint64_t result = 0;
    // All bytes except the most significant byte should be unsign-extended and shifted. The most
    // significant byte should be sign-extended.
    for (int32_t i = 0; i < num_bytes - 1; ++i) {
        uint64_t unsigned_byte_value = static_cast<uint8_t>(bytes[pos + i]);
        result |= unsigned_byte_value << (8 * i);
    }
    int64_t signed_byte_value = static_cast<int8_t>(bytes[pos + num_bytes - 1]);
    result |= static_cast<uint64_t>(signed_byte_value) << (8 * (num_bytes - 1));
    return static_cast<int64_t>(result);
}

Result<int32_t> VariantBinaryUtil::ReadUnsigned(std::string_view bytes, int32_t pos,
                                                int32_t num_bytes) {
    auto length = static_cast<int32_t>(bytes.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    PAIMON_RETURN_NOT_OK(CheckIndex(pos + num_bytes - 1, length));
    int64_t result = 0;
    // Similar to the `ReadLong` loop, but all bytes should be unsign-extended.
    for (int32_t i = 0; i < num_bytes; ++i) {
        int64_t unsigned_byte_value = static_cast<uint8_t>(bytes[pos + i]);
        result |= unsigned_byte_value << (8 * i);
    }
    if (result < 0 || result > std::numeric_limits<int32_t>::max()) {
        return MalformedVariant(fmt::format("unsigned value {} does not fit into int32", result));
    }
    return static_cast<int32_t>(result);
}

uint8_t VariantBinaryUtil::PrimitiveHeader(int32_t type) {
    return static_cast<uint8_t>(type << 2 | VariantDefs::kPrimitive);
}

uint8_t VariantBinaryUtil::ShortStrHeader(int32_t size) {
    return static_cast<uint8_t>(size << 2 | VariantDefs::kShortStr);
}

uint8_t VariantBinaryUtil::ObjectHeader(bool large_size, int32_t id_size, int32_t offset_size) {
    return static_cast<uint8_t>(((large_size ? 1 : 0) << (VariantDefs::kBasicTypeBits + 4)) |
                                ((id_size - 1) << (VariantDefs::kBasicTypeBits + 2)) |
                                ((offset_size - 1) << VariantDefs::kBasicTypeBits) |
                                VariantDefs::kObject);
}

uint8_t VariantBinaryUtil::ArrayHeader(bool large_size, int32_t offset_size) {
    return static_cast<uint8_t>(((large_size ? 1 : 0) << (VariantDefs::kBasicTypeBits + 2)) |
                                ((offset_size - 1) << VariantDefs::kBasicTypeBits) |
                                VariantDefs::kArray);
}

Result<int32_t> VariantBinaryUtil::GetTypeInfo(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    return (static_cast<uint8_t>(value[pos]) >> VariantDefs::kBasicTypeBits) &
           VariantDefs::kTypeInfoMask;
}

Result<VariantValueType> VariantBinaryUtil::GetType(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    switch (basic_type) {
        case VariantDefs::kShortStr:
            return VariantValueType::kString;
        case VariantDefs::kObject:
            return VariantValueType::kObject;
        case VariantDefs::kArray:
            return VariantValueType::kArray;
        default:
            switch (type_info) {
                case VariantDefs::kNull:
                    return VariantValueType::kNull;
                case VariantDefs::kTrue:
                case VariantDefs::kFalse:
                    return VariantValueType::kBoolean;
                case VariantDefs::kInt1:
                case VariantDefs::kInt2:
                case VariantDefs::kInt4:
                case VariantDefs::kInt8:
                    return VariantValueType::kLong;
                case VariantDefs::kDouble:
                    return VariantValueType::kDouble;
                case VariantDefs::kDecimal4:
                case VariantDefs::kDecimal8:
                case VariantDefs::kDecimal16:
                    return VariantValueType::kDecimal;
                case VariantDefs::kDate:
                    return VariantValueType::kDate;
                case VariantDefs::kTimestamp:
                    return VariantValueType::kTimestamp;
                case VariantDefs::kTimestampNtz:
                    return VariantValueType::kTimestampNtz;
                case VariantDefs::kFloat:
                    return VariantValueType::kFloat;
                case VariantDefs::kBinary:
                    return VariantValueType::kBinary;
                case VariantDefs::kLongStr:
                    return VariantValueType::kString;
                case VariantDefs::kUuid:
                    return VariantValueType::kUuid;
                default:
                    return UnknownPrimitiveTypeInVariant(type_info);
            }
    }
}

Result<int32_t> VariantBinaryUtil::ValueSize(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    switch (basic_type) {
        case VariantDefs::kShortStr:
            return 1 + type_info;
        case VariantDefs::kObject: {
            PAIMON_ASSIGN_OR_RAISE(ObjectInfo info, GetObjectInfo(value, pos));
            PAIMON_ASSIGN_OR_RAISE(
                int32_t data_size,
                ReadUnsigned(value, info.offset_start + info.num_elements * info.offset_size,
                             info.offset_size));
            return info.data_start - pos + data_size;
        }
        case VariantDefs::kArray: {
            PAIMON_ASSIGN_OR_RAISE(ArrayInfo info, GetArrayInfo(value, pos));
            PAIMON_ASSIGN_OR_RAISE(
                int32_t data_size,
                ReadUnsigned(value, info.offset_start + info.num_elements * info.offset_size,
                             info.offset_size));
            return info.data_start - pos + data_size;
        }
        default:
            switch (type_info) {
                case VariantDefs::kNull:
                case VariantDefs::kTrue:
                case VariantDefs::kFalse:
                    return 1;
                case VariantDefs::kInt1:
                    return 2;
                case VariantDefs::kInt2:
                    return 3;
                case VariantDefs::kInt4:
                case VariantDefs::kDate:
                case VariantDefs::kFloat:
                    return 5;
                case VariantDefs::kInt8:
                case VariantDefs::kDouble:
                case VariantDefs::kTimestamp:
                case VariantDefs::kTimestampNtz:
                    return 9;
                case VariantDefs::kDecimal4:
                    return 6;
                case VariantDefs::kDecimal8:
                    return 10;
                case VariantDefs::kDecimal16:
                    return 18;
                case VariantDefs::kBinary:
                case VariantDefs::kLongStr: {
                    PAIMON_ASSIGN_OR_RAISE(int32_t data_size,
                                           ReadUnsigned(value, pos + 1, VariantDefs::kU32Size));
                    return 1 + VariantDefs::kU32Size + data_size;
                }
                case VariantDefs::kUuid:
                    return 17;
                default:
                    return UnknownPrimitiveTypeInVariant(type_info);
            }
    }
}

Result<bool> VariantBinaryUtil::GetBoolean(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive ||
        (type_info != VariantDefs::kTrue && type_info != VariantDefs::kFalse)) {
        return UnexpectedType(VariantValueType::kBoolean);
    }
    return type_info == VariantDefs::kTrue;
}

Result<int64_t> VariantBinaryUtil::GetLong(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    constexpr const char* kExceptionMessage = "Expect type to be LONG/DATE/TIMESTAMP/TIMESTAMP_NTZ";
    if (basic_type != VariantDefs::kPrimitive) {
        return Status::Invalid(kExceptionMessage);
    }
    switch (type_info) {
        case VariantDefs::kInt1:
            return ReadLong(value, pos + 1, 1);
        case VariantDefs::kInt2:
            return ReadLong(value, pos + 1, 2);
        case VariantDefs::kInt4:
        case VariantDefs::kDate:
            return ReadLong(value, pos + 1, 4);
        case VariantDefs::kInt8:
        case VariantDefs::kTimestamp:
        case VariantDefs::kTimestampNtz:
            return ReadLong(value, pos + 1, 8);
        default:
            return Status::Invalid(kExceptionMessage);
    }
}

Result<double> VariantBinaryUtil::GetDouble(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive || type_info != VariantDefs::kDouble) {
        return UnexpectedType(VariantValueType::kDouble);
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t bits, ReadLong(value, pos + 1, 8));
    double result;
    memcpy(&result, &bits, sizeof(result));
    return result;
}

namespace {
// Checks whether the precision and scale of the decimal are within the limit.
Status CheckDecimal(const VariantDecimal& d, int32_t max_precision) {
    if (d.Precision() > max_precision || d.scale > max_precision) {
        return VariantBinaryUtil::MalformedVariant(
            fmt::format("decimal precision {} or scale {} exceeds the maximum precision {}",
                        d.Precision(), d.scale, max_precision));
    }
    return Status::OK();
}
}  // namespace

Result<VariantDecimal> VariantBinaryUtil::GetDecimalWithOriginalScale(std::string_view value,
                                                                      int32_t pos) {
    auto length = static_cast<int32_t>(value.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive) {
        return UnexpectedType(VariantValueType::kDecimal);
    }
    PAIMON_RETURN_NOT_OK(CheckIndex(pos + 1, length));
    // Interpret the scale byte as unsigned. If it is a negative byte, the unsigned value must be
    // greater than `kMaxDecimal16Precision` and will trigger an error in `CheckDecimal`.
    int32_t scale = static_cast<uint8_t>(value[pos + 1]);
    VariantDecimal result;
    result.scale = scale;
    switch (type_info) {
        case VariantDefs::kDecimal4: {
            PAIMON_ASSIGN_OR_RAISE(int64_t unscaled, ReadLong(value, pos + 2, 4));
            result.unscaled = unscaled;
            PAIMON_RETURN_NOT_OK(CheckDecimal(result, VariantDefs::kMaxDecimal4Precision));
            break;
        }
        case VariantDefs::kDecimal8: {
            PAIMON_ASSIGN_OR_RAISE(int64_t unscaled, ReadLong(value, pos + 2, 8));
            result.unscaled = unscaled;
            PAIMON_RETURN_NOT_OK(CheckDecimal(result, VariantDefs::kMaxDecimal8Precision));
            break;
        }
        case VariantDefs::kDecimal16: {
            PAIMON_RETURN_NOT_OK(CheckIndex(pos + 17, length));
            __uint128_t unscaled = 0;
            for (int32_t i = 0; i < 16; ++i) {
                unscaled |= static_cast<__uint128_t>(static_cast<uint8_t>(value[pos + 2 + i]))
                            << (8 * i);
            }
            result.unscaled = static_cast<__int128_t>(unscaled);
            PAIMON_RETURN_NOT_OK(CheckDecimal(result, VariantDefs::kMaxDecimal16Precision));
            break;
        }
        default:
            return UnexpectedType(VariantValueType::kDecimal);
    }
    return result;
}

Result<VariantDecimal> VariantBinaryUtil::GetDecimal(std::string_view value, int32_t pos) {
    PAIMON_ASSIGN_OR_RAISE(VariantDecimal decimal, GetDecimalWithOriginalScale(value, pos));
    return decimal.StripTrailingZeros();
}

Result<float> VariantBinaryUtil::GetFloat(std::string_view value, int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive || type_info != VariantDefs::kFloat) {
        return UnexpectedType(VariantValueType::kFloat);
    }
    PAIMON_ASSIGN_OR_RAISE(int64_t bits, ReadLong(value, pos + 1, 4));
    auto int_bits = static_cast<int32_t>(bits);
    float result;
    memcpy(&result, &int_bits, sizeof(result));
    return result;
}

Result<std::string_view> VariantBinaryUtil::GetBinary(std::string_view value, int32_t pos) {
    auto length = static_cast<int32_t>(value.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive || type_info != VariantDefs::kBinary) {
        return UnexpectedType(VariantValueType::kBinary);
    }
    int32_t start = pos + 1 + VariantDefs::kU32Size;
    PAIMON_ASSIGN_OR_RAISE(int32_t data_size, ReadUnsigned(value, pos + 1, VariantDefs::kU32Size));
    if (data_size > 0) {
        PAIMON_RETURN_NOT_OK(CheckIndex(start + data_size - 1, length));
    }
    return value.substr(start, data_size);
}

Result<std::string_view> VariantBinaryUtil::GetString(std::string_view value, int32_t pos) {
    auto length = static_cast<int32_t>(value.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type == VariantDefs::kShortStr ||
        (basic_type == VariantDefs::kPrimitive && type_info == VariantDefs::kLongStr)) {
        int32_t start;
        int32_t str_size;
        if (basic_type == VariantDefs::kShortStr) {
            start = pos + 1;
            str_size = type_info;
        } else {
            start = pos + 1 + VariantDefs::kU32Size;
            PAIMON_ASSIGN_OR_RAISE(str_size, ReadUnsigned(value, pos + 1, VariantDefs::kU32Size));
        }
        if (str_size > 0) {
            PAIMON_RETURN_NOT_OK(CheckIndex(start + str_size - 1, length));
        }
        return value.substr(start, str_size);
    }
    return UnexpectedType(VariantValueType::kString);
}

Result<std::string_view> VariantBinaryUtil::GetUuid(std::string_view value, int32_t pos) {
    auto length = static_cast<int32_t>(value.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, length));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kPrimitive || type_info != VariantDefs::kUuid) {
        return UnexpectedType(VariantValueType::kUuid);
    }
    int32_t start = pos + 1;
    PAIMON_RETURN_NOT_OK(CheckIndex(start + 15, length));
    return value.substr(start, 16);
}

std::string VariantBinaryUtil::UuidToString(std::string_view uuid_bytes) {
    constexpr char kHexDigits[] = "0123456789abcdef";
    std::string result;
    result.reserve(36);
    for (size_t i = 0; i < 16; ++i) {
        if (i == 4 || i == 6 || i == 8 || i == 10) {
            result.push_back('-');
        }
        auto byte = static_cast<uint8_t>(uuid_bytes[i]);
        result.push_back(kHexDigits[byte >> 4]);
        result.push_back(kHexDigits[byte & 0xF]);
    }
    return result;
}

Result<VariantBinaryUtil::ObjectInfo> VariantBinaryUtil::GetObjectInfo(std::string_view value,
                                                                       int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kObject) {
        return UnexpectedType(VariantValueType::kObject);
    }
    // Refer to the comment of the `VariantDefs::kObject` constant for the details of the object
    // header encoding. Suppose `type_info` has a bit representation of 0_b4_b3b2_b1b0, the
    // following line extracts b4 to determine whether the object uses a 1/4-byte size.
    bool large_size = ((type_info >> 4) & 0x1) != 0;
    int32_t size_bytes = large_size ? VariantDefs::kU32Size : 1;
    ObjectInfo info;
    PAIMON_ASSIGN_OR_RAISE(info.num_elements, ReadUnsigned(value, pos + 1, size_bytes));
    // Extracts b3b2 to determine the integer size of the field id list.
    info.id_size = ((type_info >> 2) & 0x3) + 1;
    // Extracts b1b0 to determine the integer size of the offset list.
    info.offset_size = (type_info & 0x3) + 1;
    info.id_start = pos + 1 + size_bytes;
    // A corrupted variant can claim a near-INT32_MAX element count; compute the layout in
    // 64-bit and bound it by the buffer before 32-bit arithmetic could overflow.
    int64_t offset_start = static_cast<int64_t>(info.id_start) +
                           static_cast<int64_t>(info.num_elements) * info.id_size;
    int64_t data_start =
        offset_start + (static_cast<int64_t>(info.num_elements) + 1) * info.offset_size;
    if (data_start > static_cast<int64_t>(value.size())) {
        return MalformedVariant("object layout exceeds the value buffer");
    }
    info.offset_start = static_cast<int32_t>(offset_start);
    info.data_start = static_cast<int32_t>(data_start);
    return info;
}

Result<VariantBinaryUtil::ArrayInfo> VariantBinaryUtil::GetArrayInfo(std::string_view value,
                                                                     int32_t pos) {
    PAIMON_RETURN_NOT_OK(CheckIndex(pos, static_cast<int32_t>(value.size())));
    auto header = static_cast<uint8_t>(value[pos]);
    int32_t basic_type = header & VariantDefs::kBasicTypeMask;
    int32_t type_info = (header >> VariantDefs::kBasicTypeBits) & VariantDefs::kTypeInfoMask;
    if (basic_type != VariantDefs::kArray) {
        return UnexpectedType(VariantValueType::kArray);
    }
    // Suppose `type_info` has a bit representation of 000_b2_b1b0, the following line extracts b2
    // to determine whether the array uses a 1/4-byte size.
    bool large_size = ((type_info >> 2) & 0x1) != 0;
    int32_t size_bytes = large_size ? VariantDefs::kU32Size : 1;
    ArrayInfo info;
    PAIMON_ASSIGN_OR_RAISE(info.num_elements, ReadUnsigned(value, pos + 1, size_bytes));
    // Extracts b1b0 to determine the integer size of the offset list.
    info.offset_size = (type_info & 0x3) + 1;
    info.offset_start = pos + 1 + size_bytes;
    // See GetObjectInfo: bound the 64-bit layout by the buffer before 32-bit overflow.
    int64_t data_start = static_cast<int64_t>(info.offset_start) +
                         (static_cast<int64_t>(info.num_elements) + 1) * info.offset_size;
    if (data_start > static_cast<int64_t>(value.size())) {
        return MalformedVariant("array layout exceeds the value buffer");
    }
    info.data_start = static_cast<int32_t>(data_start);
    return info;
}

Result<std::string_view> VariantBinaryUtil::GetMetadataKey(std::string_view metadata, int32_t id) {
    auto length = static_cast<int32_t>(metadata.size());
    PAIMON_RETURN_NOT_OK(CheckIndex(0, length));
    // Extracts the highest 2 bits in the metadata header to determine the integer size of the
    // offset list.
    int32_t offset_size = ((static_cast<uint8_t>(metadata[0]) >> 6) & 0x3) + 1;
    PAIMON_ASSIGN_OR_RAISE(int32_t dict_size, ReadUnsigned(metadata, 1, offset_size));
    if (id >= dict_size) {
        return MalformedVariant(fmt::format(
            "metadata key id {} is out of bounds for a dictionary of {} keys", id, dict_size));
    }
    // There are a header byte, a `dict_size` with `offset_size` bytes, and `(dict_size + 1)`
    // offsets before the string data.
    // Bound the offset-table layout in 64-bit before 32-bit arithmetic could overflow on a
    // corrupted dictionary size.
    int64_t string_start64 = 1 + (static_cast<int64_t>(dict_size) + 2) * offset_size;
    if (string_start64 > static_cast<int64_t>(length)) {
        return MalformedVariant("metadata dictionary layout exceeds the metadata buffer");
    }
    auto string_start = static_cast<int32_t>(string_start64);
    PAIMON_ASSIGN_OR_RAISE(int32_t offset,
                           ReadUnsigned(metadata, 1 + (id + 1) * offset_size, offset_size));
    PAIMON_ASSIGN_OR_RAISE(int32_t next_offset,
                           ReadUnsigned(metadata, 1 + (id + 2) * offset_size, offset_size));
    if (offset > next_offset) {
        return MalformedVariant("metadata key offsets are not monotonic");
    }
    if (static_cast<int64_t>(string_start) + next_offset > static_cast<int64_t>(length)) {
        return MalformedVariant("metadata key data exceeds the metadata buffer");
    }
    return metadata.substr(string_start + offset, next_offset - offset);
}

}  // namespace paimon
