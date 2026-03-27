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

#include "paimon/common/data/binary_row_writer.h"

#include <cassert>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/memory/memory_segment.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/bytes.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"

namespace paimon {

BinaryRowWriter::BinaryRowWriter(BinaryRow* row, int32_t initial_size, MemoryPool* pool)
    : null_bits_size_in_bytes_(BinaryRow::CalculateBitSetWidthInBytes(row->GetFieldCount())),
      fixed_size_(row->GetFixedLengthPartSize()) {
    cursor_ = fixed_size_;
    row_ = row;
    segment_ = MemorySegment::Wrap(Bytes::AllocateBytes(fixed_size_ + initial_size, pool));
    row_->PointTo(segment_, 0, segment_.Size());
    pool_ = pool;
}

Result<BinaryRowWriter::FieldSetterFunc> BinaryRowWriter::CreateFieldSetter(
    int32_t field_idx, const std::shared_ptr<arrow::DataType>& field_type) {
    arrow::Type::type type = field_type->id();
    BinaryRowWriter::FieldSetterFunc field_setter;
    switch (type) {
        case arrow::Type::type::BOOL: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteBoolean(field_idx, DataDefine::GetVariantValue<bool>(field));
            };
            break;
        }
        case arrow::Type::type::INT8: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteByte(field_idx, DataDefine::GetVariantValue<char>(field));
            };
            break;
        }
        case arrow::Type::type::INT16: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteShort(field_idx, DataDefine::GetVariantValue<int16_t>(field));
            };
            break;
        }
        case arrow::Type::type::INT32: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteInt(field_idx, DataDefine::GetVariantValue<int32_t>(field));
            };
            break;
        }
        case arrow::Type::type::INT64: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteLong(field_idx, DataDefine::GetVariantValue<int64_t>(field));
            };
            break;
        }
        case arrow::Type::type::FLOAT: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteFloat(field_idx, DataDefine::GetVariantValue<float>(field));
            };
            break;
        }
        case arrow::Type::type::DOUBLE: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteDouble(field_idx, DataDefine::GetVariantValue<double>(field));
            };
            break;
        }
        case arrow::Type::type::DATE32: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                return writer->WriteInt(field_idx, DataDefine::GetVariantValue<int32_t>(field));
            };
            break;
        }
        case arrow::Type::type::STRING: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                const auto* view = DataDefine::GetVariantPtr<std::string_view>(field);
                if (view) {
                    return writer->WriteStringView(field_idx, *view);
                }
                return writer->WriteString(field_idx,
                                           DataDefine::GetVariantValue<BinaryString>(field));
            };
            break;
        }
        case arrow::Type::type::BINARY: {
            field_setter = [field_idx](const VariantType& field, BinaryRowWriter* writer) -> void {
                const auto* view = DataDefine::GetVariantPtr<std::string_view>(field);
                if (view) {
                    return writer->WriteStringView(field_idx, *view);
                }
                return writer->WriteBinary(
                    field_idx, *DataDefine::GetVariantValue<std::shared_ptr<Bytes>>(field));
            };
            break;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            field_setter = [field_idx, precision](const VariantType& field,
                                                  BinaryRowWriter* writer) -> void {
                if (DataDefine::IsVariantNull(field)) {
                    if (!Timestamp::IsCompact(precision)) {
                        writer->WriteTimestamp(field_idx, std::nullopt, precision);
                    } else {
                        writer->SetNullAt(field_idx);
                    }
                    return;
                }
                return writer->WriteTimestamp(
                    field_idx, DataDefine::GetVariantValue<Timestamp>(field), precision);
            };
            return field_setter;
        }
        case arrow::Type::type::DECIMAL: {
            auto* decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(field_type.get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            field_setter = [field_idx, precision](const VariantType& field,
                                                  BinaryRowWriter* writer) -> void {
                if (DataDefine::IsVariantNull(field)) {
                    if (!Decimal::IsCompact(precision)) {
                        writer->WriteDecimal(field_idx, std::nullopt, precision);
                    } else {
                        writer->SetNullAt(field_idx);
                    }
                    return;
                }
                return writer->WriteDecimal(field_idx, DataDefine::GetVariantValue<Decimal>(field),
                                            precision);
            };
            return field_setter;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in data setter", field_type->ToString()));
    }
    BinaryRowWriter::FieldSetterFunc ret =
        [field_idx, field_setter](const VariantType& field, BinaryRowWriter* writer) -> void {
        if (DataDefine::IsVariantNull(field)) {
            writer->SetNullAt(field_idx);
            return;
        }
        field_setter(field, writer);
    };
    return ret;
}

}  // namespace paimon
