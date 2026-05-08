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

#include "paimon/common/global_index/btree/key_serializer.h"

#include "fmt/format.h"
#include "paimon/common/memory/memory_slice_input.h"
#include "paimon/common/memory/memory_slice_output.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/common/utils/preconditions.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/status.h"
namespace paimon {
Result<std::shared_ptr<Bytes>> KeySerializer::SerializeKey(
    const Literal& literal, const std::shared_ptr<arrow::DataType>& type, MemoryPool* pool) {
    if (literal.IsNull()) {
        return Status::Invalid("cannot serialize null in KeySerializer");
    }
    switch (literal.GetType()) {
        case FieldType::BOOLEAN: {
            MemorySliceOutput output(1, pool);
            output.Reset();
            output.WriteValue<int8_t>(literal.GetValue<bool>() ? static_cast<int8_t>(1)
                                                               : static_cast<int8_t>(0));
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::TINYINT: {
            MemorySliceOutput output(1, pool);
            output.Reset();
            output.WriteValue<int8_t>(literal.GetValue<int8_t>());
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::SMALLINT: {
            MemorySliceOutput output(2, pool);
            output.Reset();
            output.WriteValue<int16_t>(literal.GetValue<int16_t>());
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::INT:
        case FieldType::DATE: {
            MemorySliceOutput output(4, pool);
            output.Reset();
            output.WriteValue<int32_t>(literal.GetValue<int32_t>());
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::BIGINT: {
            MemorySliceOutput output(8, pool);
            output.Reset();
            output.WriteValue<int64_t>(literal.GetValue<int64_t>());
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::FLOAT: {
            MemorySliceOutput output(4, pool);
            output.Reset();
            auto fvalue = literal.GetValue<float>();
            int32_t ivalue;
            memcpy(&ivalue, &fvalue, sizeof(float));
            output.WriteValue<int32_t>(ivalue);
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::DOUBLE: {
            MemorySliceOutput output(8, pool);
            output.Reset();
            auto dvalue = literal.GetValue<double>();
            int64_t ivalue;
            memcpy(&ivalue, &dvalue, sizeof(double));
            output.WriteValue<int64_t>(ivalue);
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::STRING: {
            auto svalue = literal.GetValue<std::string>();
            std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(svalue, pool);
            return bytes;
        }
        case FieldType::TIMESTAMP: {
            auto ts_type = std::dynamic_pointer_cast<arrow::TimestampType>(type);
            PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
                ts_type, "ts type cannot cast to arrow::TimestampType in BTreeGlobalIndex"));
            MemorySliceOutput output(8, pool);
            output.Reset();
            auto ts = literal.GetValue<Timestamp>();
            if (Timestamp::IsCompact(DateTimeUtils::GetPrecisionFromType(ts_type))) {
                output.WriteValue<int64_t>(ts.GetMillisecond());
            } else {
                output.WriteValue<int64_t>(ts.GetMillisecond());
                PAIMON_RETURN_NOT_OK(output.WriteVarLenInt(ts.GetNanoOfMillisecond()));
            }
            return output.ToSlice().CopyBytes(pool);
        }
        case FieldType::DECIMAL: {
            auto decimal_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
            PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
                decimal_type,
                "decimal type cannot cast to arrow::Decimal128Type in BTreeGlobalIndex"));

            auto decimal = literal.GetValue<Decimal>();
            if (Decimal::IsCompact(decimal_type->precision())) {
                MemorySliceOutput output(8, pool);
                output.Reset();
                output.WriteValue<int64_t>(decimal.ToUnscaledLong());
                return output.ToSlice().CopyBytes(pool);
            } else {
                std::vector<char> decimal_bytes = decimal.ToUnscaledBytes();
                std::shared_ptr<Bytes> bytes = Bytes::AllocateBytes(decimal_bytes.size(), pool);
                memcpy(bytes->data(), decimal_bytes.data(), decimal_bytes.size());
                return bytes;
            }
        }
        default:
            return Status::Invalid(
                fmt::format("Not support serialize {} type in BTreeGlobalIndex",
                            FieldTypeUtils::FieldTypeToString(literal.GetType())));
    }
}

Result<Literal> KeySerializer::DeserializeKey(const MemorySlice& slice,
                                              const std::shared_ptr<arrow::DataType>& type,
                                              MemoryPool* pool) {
    switch (type->id()) {
        case arrow::Type::type::BOOL:
            return Literal(slice.ReadByte(0) == 1 ? true : false);
        case arrow::Type::type::INT8:
            return Literal(slice.ReadByte(0));
        case arrow::Type::type::INT16:
            return Literal(slice.ReadShort(0));
        case arrow::Type::type::INT32:
            return Literal(slice.ReadInt(0));
        case arrow::Type::type::DATE32:
            return Literal(FieldType::DATE, slice.ReadInt(0));
        case arrow::Type::type::INT64:
            return Literal(slice.ReadLong(0));
        case arrow::Type::type::FLOAT: {
            int32_t ivalue = slice.ReadInt(0);
            float fvalue;
            memcpy(&fvalue, &ivalue, sizeof(fvalue));
            return Literal(fvalue);
        }
        case arrow::Type::type::DOUBLE: {
            int64_t ivalue = slice.ReadLong(0);
            double dvalue;
            memcpy(&dvalue, &ivalue, sizeof(dvalue));
            return Literal(dvalue);
        }
        case arrow::Type::type::STRING: {
            auto bytes = slice.CopyBytes(pool);
            return Literal(FieldType::STRING, bytes->data(), bytes->size());
        }
        case arrow::Type::type::TIMESTAMP: {
            auto ts_type = std::dynamic_pointer_cast<arrow::TimestampType>(type);
            PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
                ts_type, "ts type cannot cast to arrow::TimestampType in BTreeGlobalIndex"));
            if (Timestamp::IsCompact(DateTimeUtils::GetPrecisionFromType(ts_type))) {
                return Literal(Timestamp::FromEpochMillis(slice.ReadLong(0)));
            } else {
                auto input = slice.ToInput();
                int64_t millis = input.ReadLong();
                PAIMON_ASSIGN_OR_RAISE(int32_t nanos, input.ReadVarLenInt());
                return Literal(Timestamp(millis, nanos));
            }
        }
        case arrow::Type::type::DECIMAL128: {
            auto decimal_type = std::dynamic_pointer_cast<arrow::Decimal128Type>(type);
            PAIMON_RETURN_NOT_OK(Preconditions::CheckNotNull(
                decimal_type,
                "decimal type cannot cast to arrow::Decimal128Type in BTreeGlobalIndex"));
            if (Decimal::IsCompact(decimal_type->precision())) {
                return Literal(Decimal::FromUnscaledLong(
                    slice.ReadLong(0), decimal_type->precision(), decimal_type->scale()));
            } else {
                auto bytes = slice.CopyBytes(pool);
                return Literal(Decimal::FromUnscaledBytes(decimal_type->precision(),
                                                          decimal_type->scale(), bytes.get()));
            }
        }
        default:
            return Status::Invalid(fmt::format(
                "Not support deserialize {} type in BTreeGlobalIndex", type->ToString()));
    }
}

MemorySlice::SliceComparator KeySerializer::CreateComparator(
    const std::shared_ptr<arrow::DataType>& type, const std::shared_ptr<MemoryPool>& pool) {
    // Fast paths for integer and string types: direct value comparison without Literal
    // deserialization, avoiding heap allocations entirely.
    switch (type->id()) {
        case arrow::Type::type::STRING:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                std::string_view sv_a = a.ReadStringView();
                std::string_view sv_b = b.ReadStringView();
                int32_t cmp = sv_a.compare(sv_b);
                return cmp == 0 ? 0 : (cmp > 0 ? 1 : -1);
            };
        case arrow::Type::type::BOOL:
        case arrow::Type::type::INT8:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int8_t va = a.ReadByte(0);
                int8_t vb = b.ReadByte(0);
                return (va < vb) ? -1 : (va > vb ? 1 : 0);
            };
        case arrow::Type::type::INT16:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int16_t va = a.ReadShort(0);
                int16_t vb = b.ReadShort(0);
                return (va < vb) ? -1 : (va > vb ? 1 : 0);
            };
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int32_t va = a.ReadInt(0);
                int32_t vb = b.ReadInt(0);
                return (va < vb) ? -1 : (va > vb ? 1 : 0);
            };
        case arrow::Type::type::INT64:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int64_t va = a.ReadLong(0);
                int64_t vb = b.ReadLong(0);
                return (va < vb) ? -1 : (va > vb ? 1 : 0);
            };
        case arrow::Type::type::FLOAT:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int32_t ia = a.ReadInt(0);
                int32_t ib = b.ReadInt(0);
                float fa, fb;
                memcpy(&fa, &ia, sizeof(float));
                memcpy(&fb, &ib, sizeof(float));
                return FieldsComparator::CompareFloatingPoint(fa, fb);
            };
        case arrow::Type::type::DOUBLE:
            return [](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
                int64_t ia = a.ReadLong(0);
                int64_t ib = b.ReadLong(0);
                double da, db;
                memcpy(&da, &ia, sizeof(double));
                memcpy(&db, &ib, sizeof(double));
                return FieldsComparator::CompareFloatingPoint(da, db);
            };
        default:
            break;
    }

    // Fallback for complex types (TIMESTAMP, DECIMAL, etc.):
    // deserialize to Literal and compare.
    return
        [pool = pool, type = type](const MemorySlice& a, const MemorySlice& b) -> Result<int32_t> {
            PAIMON_ASSIGN_OR_RAISE(Literal la, DeserializeKey(a, type, pool.get()));
            PAIMON_ASSIGN_OR_RAISE(Literal lb, DeserializeKey(b, type, pool.get()));
            return la.CompareTo(lb);
        };
}
}  // namespace paimon
