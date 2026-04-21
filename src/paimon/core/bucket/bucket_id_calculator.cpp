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

#include "paimon/bucket/bucket_id_calculator.h"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/bucket/bucket_function.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/hive_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/result.h"

namespace paimon {
namespace {
#define CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id) \
    if (typed_array->IsNull(row_id)) {                              \
        row_writer->SetNullAt(col_id);                              \
        return;                                                     \
    }

using WriteFunction = std::function<void(int32_t, BinaryRowWriter*)>;
static Result<WriteFunction> WriteBucketRow(int32_t col_id,
                                            const std::shared_ptr<arrow::Array>& field) {
    arrow::Type::type type = field->type()->id();
    switch (type) {
        case arrow::Type::type::BOOL: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::BooleanArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteBoolean(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::INT8: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Int8Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteByte(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::INT16: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Int16Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteShort(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::INT32: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Int32Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteInt(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::INT64: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Int64Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteLong(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::FLOAT: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::FloatArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteFloat(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::DOUBLE: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::DoubleArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteDouble(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::DATE32: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Date32Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                row_writer->WriteInt(col_id, typed_array->Value(row_id));
            };
            return writer_func;
        }
        case arrow::Type::type::STRING: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::StringArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                std::string_view value = typed_array->GetView(row_id);
                row_writer->WriteStringView(col_id, value);
            };
            return writer_func;
        }
        case arrow::Type::type::BINARY: {
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::BinaryArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array](int32_t row_id,
                                                              BinaryRowWriter* row_writer) {
                CHECK_AND_SET_NULL(typed_array, row_writer, row_id, col_id);
                std::string_view value = typed_array->GetView(row_id);
                row_writer->WriteStringView(col_id, value);
            };
            return writer_func;
        }
        case arrow::Type::type::TIMESTAMP: {
            auto timestamp_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field->type());
            assert(timestamp_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
            DateTimeUtils::TimeType time_type =
                DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::TimestampArray*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [typed_array, col_id, precision, time_type](
                                            int32_t row_id, BinaryRowWriter* row_writer) {
                if (typed_array->IsNull(row_id)) {
                    if (!Timestamp::IsCompact(precision)) {
                        row_writer->WriteTimestamp(col_id, std::nullopt, precision);
                    } else {
                        row_writer->SetNullAt(col_id);
                    }
                    return;
                }
                int64_t ts_value = typed_array->Value(row_id);
                auto [milli, nano] = DateTimeUtils::TimestampConverter(
                    ts_value, time_type, DateTimeUtils::TimeType::MILLISECOND,
                    DateTimeUtils::TimeType::NANOSECOND);
                row_writer->WriteTimestamp(col_id, Timestamp(milli, nano), precision);
            };
            return writer_func;
        }
        case arrow::Type::type::DECIMAL: {
            const auto* decimal_type =
                arrow::internal::checked_cast<const arrow::Decimal128Type*>(field->type().get());
            assert(decimal_type);
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            const auto* typed_array =
                arrow::internal::checked_cast<const arrow::Decimal128Array*>(field.get());
            assert(typed_array);
            WriteFunction writer_func = [col_id, typed_array, precision, scale](
                                            int32_t row_id, BinaryRowWriter* row_writer) {
                if (typed_array->IsNull(row_id)) {
                    if (!Decimal::IsCompact(precision)) {
                        row_writer->WriteDecimal(col_id, std::nullopt, precision);
                    } else {
                        row_writer->SetNullAt(col_id);
                    }
                    return;
                }
                arrow::Decimal128 decimal128(typed_array->GetValue(row_id));
                Decimal decimal(precision, scale,
                                static_cast<Decimal::int128_t>(decimal128.high_bits()) << 64 |
                                    decimal128.low_bits());
                row_writer->WriteDecimal(col_id, decimal, precision);
            };
            return writer_func;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in write bucket row", field->type()->ToString()));
    }
}
}  // namespace

BucketIdCalculator::BucketIdCalculator(int32_t num_buckets,
                                       std::unique_ptr<BucketFunction> bucket_function,
                                       const std::shared_ptr<MemoryPool>& pool)
    : num_buckets_(num_buckets), bucket_function_(std::move(bucket_function)), pool_(pool) {}

BucketIdCalculator::~BucketIdCalculator() = default;

Result<std::unique_ptr<BucketIdCalculator>> BucketIdCalculator::Create(
    bool is_pk_table, int32_t num_buckets, const std::shared_ptr<MemoryPool>& pool) {
    return Create(is_pk_table, num_buckets, std::make_unique<DefaultBucketFunction>(), pool);
}

Result<std::unique_ptr<BucketIdCalculator>> BucketIdCalculator::Create(
    bool is_pk_table, int32_t num_buckets, std::unique_ptr<BucketFunction> bucket_function,
    const std::shared_ptr<MemoryPool>& pool) {
    if (num_buckets == 0 || num_buckets < -2) {
        return Status::Invalid("num buckets must be -1 or -2 or greater than 0");
    }
    if (is_pk_table && num_buckets == -1) {
        return Status::Invalid(
            "DynamicBucketMode or CrossPartitionBucketMode cannot calculate bucket id in "
            "primary key table");
    }
    if (!is_pk_table && num_buckets == -2) {
        return Status::Invalid("Append table not support PostponeBucketMode");
    }
    return std::unique_ptr<BucketIdCalculator>(
        new BucketIdCalculator(num_buckets, std::move(bucket_function), pool));
}

Result<std::unique_ptr<BucketIdCalculator>> BucketIdCalculator::CreateMod(
    bool is_pk_table, int32_t num_buckets, FieldType bucket_key_type,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(auto mod_func, ModBucketFunction::Create(bucket_key_type));
    return Create(is_pk_table, num_buckets, std::move(mod_func), pool);
}

Result<std::unique_ptr<BucketIdCalculator>> BucketIdCalculator::CreateHive(
    bool is_pk_table, int32_t num_buckets, const std::vector<HiveFieldInfo>& field_infos,
    const std::shared_ptr<MemoryPool>& pool) {
    PAIMON_ASSIGN_OR_RAISE(auto hive_func, HiveBucketFunction::Create(field_infos));
    return Create(is_pk_table, num_buckets, std::move(hive_func), pool);
}

Status BucketIdCalculator::CalculateBucketIds(ArrowArray* bucket_keys, ArrowSchema* bucket_schema,
                                              int32_t* bucket_ids) const {
    ScopeGuard guard([bucket_keys, bucket_schema]() {
        ArrowArrayRelease(bucket_keys);
        ArrowSchemaRelease(bucket_schema);
    });
    if (num_buckets_ == -1 || num_buckets_ == 1) {
        memset(bucket_ids, 0, bucket_keys->length * sizeof(int32_t));
        return Status::OK();
    }
    if (num_buckets_ == -2) {
        for (int32_t i = 0; i < bucket_keys->length; i++) {
            bucket_ids[i] = -2;
        }
        return Status::OK();
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> bucket_array,
                                      arrow::ImportArray(bucket_keys, bucket_schema));
    const auto* struct_array =
        arrow::internal::checked_cast<const arrow::StructArray*>(bucket_array.get());
    if (!struct_array) {
        return Status::Invalid("bucket keys is not a struct array");
    }
    std::vector<WriteFunction> write_functions;
    int32_t num_fields = struct_array->num_fields();
    write_functions.reserve(num_fields);
    for (int32_t col = 0; col < num_fields; col++) {
        PAIMON_ASSIGN_OR_RAISE(WriteFunction write_func,
                               WriteBucketRow(col, struct_array->field(col)));
        write_functions.push_back(std::move(write_func));
    }

    BinaryRow bucket_row(num_fields);
    BinaryRowWriter row_writer(&bucket_row, /*initial_size=*/1024, pool_.get());
    for (int32_t row = 0; row < struct_array->length(); row++) {
        row_writer.Reset();
        for (int32_t col = 0; col < num_fields; col++) {
            write_functions[col](row, &row_writer);
        }
        row_writer.Complete();
        bucket_ids[row] = bucket_function_->Bucket(bucket_row, num_buckets_);
    }
    guard.Release();
    return Status::OK();
}

}  // namespace paimon
