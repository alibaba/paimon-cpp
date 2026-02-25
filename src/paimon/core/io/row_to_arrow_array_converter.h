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

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/internal_array.h"
#include "paimon/common/data/internal_map.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/key_value.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/reader/batch_reader.h"
namespace paimon {
// convert row T to output R (R maybe BatchReader::ReadBatch or KeyValueBatch)
template <typename T, typename R>
class RowToArrowArrayConverter {
 public:
    virtual ~RowToArrowArrayConverter() = default;

    virtual Result<R> NextBatch(const std::vector<T>& rows) = 0;

    void CleanUp() {
        appenders_.clear();
        array_builder_.reset();
    }

 protected:
    using AppendValueFunc =
        std::function<arrow::Status(const DataGetters& data_getter, int32_t pos)>;
    RowToArrowArrayConverter(int32_t reserve_count, std::vector<AppendValueFunc>&& appenders,
                             std::unique_ptr<arrow::StructBuilder>&& array_builder,
                             std::unique_ptr<arrow::MemoryPool>&& arrow_pool);

    static Result<AppendValueFunc> AppendField(bool use_view, arrow::ArrayBuilder* array_builder,
                                               int32_t* reserve_count);

 protected:
    Status ResetAndReserve();
    Result<BatchReader::ReadBatch> FinishAndAccumulate();
    Status Accumulate(const arrow::Array* array, int32_t* idx);
    Status Reserve(arrow::ArrayBuilder* array_builder, int32_t* idx);

 private:
    template <typename BuilderType>
    static Result<BuilderType*> CastToTypedBuilder(arrow::ArrayBuilder* array_builder);

    static inline const double INFLATION_FACTOR = 1.2;
    void UpdateAccumulatedVec(int32_t value, int32_t* idx);

 protected:
    std::vector<int32_t> reserved_sizes_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_;
    std::vector<AppendValueFunc> appenders_;
    std::unique_ptr<arrow::StructBuilder> array_builder_;
};

#define CHECK_AND_APPEND_NULL(getter, builder, pos) \
    if (getter.IsNullAt(pos)) {                     \
        return builder->AppendNull();               \
    }

template <typename T, typename R>
RowToArrowArrayConverter<T, R>::RowToArrowArrayConverter(
    int32_t reserve_count, std::vector<RowToArrowArrayConverter<T, R>::AppendValueFunc>&& appenders,
    std::unique_ptr<arrow::StructBuilder>&& array_builder,
    std::unique_ptr<arrow::MemoryPool>&& arrow_pool)
    : reserved_sizes_(reserve_count, -1),
      arrow_pool_(std::move(arrow_pool)),
      appenders_(std::move(appenders)),
      array_builder_(std::move(array_builder)) {}

template <typename T, typename R>
Status RowToArrowArrayConverter<T, R>::ResetAndReserve() {
    array_builder_->Reset();
    int32_t reserve_idx = 0;
    return Reserve(array_builder_.get(), &reserve_idx);
}

template <typename T, typename R>
Result<BatchReader::ReadBatch> RowToArrowArrayConverter<T, R>::FinishAndAccumulate() {
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(array_builder_->Finish(&array));

    int32_t reserve_idx = 0;
    PAIMON_RETURN_NOT_OK(Accumulate(array.get(), &reserve_idx));

    std::unique_ptr<ArrowArray> c_array = std::make_unique<ArrowArray>();
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, c_array.get(), c_schema.get()));
    return make_pair(std::move(c_array), std::move(c_schema));
}

template <typename T, typename R>
Status RowToArrowArrayConverter<T, R>::Reserve(arrow::ArrayBuilder* array_builder, int32_t* idx) {
    if (reserved_sizes_[*idx] == -1) {
        // first batch, reserved_sizes_ is not initialized
        return Status::OK();
    }
    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        array_builder->Reserve(INFLATION_FACTOR * reserved_sizes_[(*idx)++]));
    arrow::Type::type type = array_builder->type()->id();
    switch (type) {
        case arrow::Type::type::BOOL:
        case arrow::Type::type::INT8:
        case arrow::Type::type::INT16:
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32:
        case arrow::Type::type::INT64:
        case arrow::Type::type::FLOAT:
        case arrow::Type::type::DOUBLE:
        case arrow::Type::type::TIMESTAMP:
        case arrow::Type::type::DECIMAL:
            break;
        case arrow::Type::type::STRING: {
            // reserve string data buffer
            PAIMON_ASSIGN_OR_RAISE(auto* string_builder,
                                   CastToTypedBuilder<arrow::StringBuilder>(array_builder));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                string_builder->ReserveData(INFLATION_FACTOR * reserved_sizes_[(*idx)++]));
            break;
        }
        case arrow::Type::type::BINARY: {
            // reserve binary data buffer
            PAIMON_ASSIGN_OR_RAISE(auto* binary_builder,
                                   CastToTypedBuilder<arrow::BinaryBuilder>(array_builder));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                binary_builder->ReserveData(INFLATION_FACTOR * reserved_sizes_[(*idx)++]));
            break;
        }
        case arrow::Type::type::LIST: {
            PAIMON_ASSIGN_OR_RAISE(auto* list_builder,
                                   CastToTypedBuilder<arrow::ListBuilder>(array_builder));
            // reserve value builder in list
            PAIMON_RETURN_NOT_OK(Reserve(list_builder->value_builder(), idx));
            break;
        }
        case arrow::Type::type::MAP: {
            PAIMON_ASSIGN_OR_RAISE(auto* map_builder,
                                   CastToTypedBuilder<arrow::MapBuilder>(array_builder));
            // reserve key builder in map
            PAIMON_RETURN_NOT_OK(Reserve(map_builder->key_builder(), idx));
            // reserve item builder in map
            PAIMON_RETURN_NOT_OK(Reserve(map_builder->item_builder(), idx));
            break;
        }
        case arrow::Type::type::STRUCT: {
            PAIMON_ASSIGN_OR_RAISE(auto* struct_builder,
                                   CastToTypedBuilder<arrow::StructBuilder>(array_builder));
            for (int32_t i = 0; i < struct_builder->num_fields(); i++) {
                // reserve item builder in struct
                PAIMON_RETURN_NOT_OK(Reserve(struct_builder->field_builder(i), idx));
            }
            break;
        }
        default:
            assert(false);
            return Status::Invalid(fmt::format("Do not support type {} in RowToArrowArrayConverter",
                                               array_builder->type()->ToString()));
    }
    return Status::OK();
}

template <typename T, typename R>
void RowToArrowArrayConverter<T, R>::UpdateAccumulatedVec(int32_t value, int32_t* idx) {
    reserved_sizes_[*idx] =
        (reserved_sizes_[*idx] == -1 ? value : (reserved_sizes_[*idx] + value) / 2);
    (*idx)++;
}

template <typename T, typename R>
Status RowToArrowArrayConverter<T, R>::Accumulate(const arrow::Array* array, int32_t* idx) {
    UpdateAccumulatedVec(array->length(), idx);
    arrow::Type::type type = array->type()->id();
    switch (type) {
        case arrow::Type::type::BOOL:
        case arrow::Type::type::INT8:
        case arrow::Type::type::INT16:
        case arrow::Type::type::INT32:
        case arrow::Type::type::DATE32:
        case arrow::Type::type::INT64:
        case arrow::Type::type::FLOAT:
        case arrow::Type::type::DOUBLE:
        case arrow::Type::type::TIMESTAMP:
        case arrow::Type::type::DECIMAL:
            break;
        case arrow::Type::type::STRING: {
            auto string_array = arrow::internal::checked_cast<const arrow::StringArray*>(array);
            assert(string_array);
            // accumulate the bytes buffer size of binary
            UpdateAccumulatedVec(string_array->value_data()->size(), idx);
            break;
        }
        case arrow::Type::type::BINARY: {
            auto binary_array = arrow::internal::checked_cast<const arrow::BinaryArray*>(array);
            assert(binary_array);
            // accumulate the bytes buffer size of binary
            UpdateAccumulatedVec(binary_array->value_data()->size(), idx);
            break;
        }
        case arrow::Type::type::LIST: {
            auto list_array = arrow::internal::checked_cast<const arrow::ListArray*>(array);
            assert(list_array);
            PAIMON_RETURN_NOT_OK(Accumulate(list_array->values().get(), idx));
            break;
        }
        case arrow::Type::type::MAP: {
            auto map_array = arrow::internal::checked_cast<const arrow::MapArray*>(array);
            assert(map_array);
            PAIMON_RETURN_NOT_OK(Accumulate(map_array->keys().get(), idx));
            PAIMON_RETURN_NOT_OK(Accumulate(map_array->items().get(), idx));
            break;
        }
        case arrow::Type::type::STRUCT: {
            auto struct_array = arrow::internal::checked_cast<const arrow::StructArray*>(array);
            assert(struct_array);
            for (const auto& field : struct_array->fields()) {
                PAIMON_RETURN_NOT_OK(Accumulate(field.get(), idx));
            }
            break;
        }
        default:
            assert(false);
            return Status::Invalid(fmt::format("Do not support type {} in RowToArrowArrayConverter",
                                               array->type()->ToString()));
    }
    return Status::OK();
}

template <typename T, typename R>
template <typename BuilderType>
Result<BuilderType*> RowToArrowArrayConverter<T, R>::CastToTypedBuilder(
    arrow::ArrayBuilder* array_builder) {
    auto field_builder = arrow::internal::checked_cast<BuilderType*>(array_builder);
    if (field_builder == nullptr) {
        return Status::Invalid("field builder is nullptr");
    }
    return field_builder;
}

template <typename T, typename R>
Result<typename RowToArrowArrayConverter<T, R>::AppendValueFunc>
RowToArrowArrayConverter<T, R>::AppendField(bool use_view, arrow::ArrayBuilder* array_builder,
                                            int32_t* reserve_count) {
    arrow::Type::type type = array_builder->type()->id();
    (*reserve_count)++;
    switch (type) {
        case arrow::Type::type::BOOL: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::BooleanBuilder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    bool value = data_getter.GetBoolean(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::INT8: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Int8Builder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    int8_t value = data_getter.GetByte(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::INT16: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Int16Builder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    int16_t value = data_getter.GetShort(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::INT32: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Int32Builder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    int32_t value = data_getter.GetInt(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::DATE32: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Date32Builder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    int32_t value = data_getter.GetDate(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::INT64: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Int64Builder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    int64_t value = data_getter.GetLong(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::FLOAT: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::FloatBuilder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    float value = data_getter.GetFloat(pos);
                    return field_builder->Append(value);
                });
        }

        case arrow::Type::type::DOUBLE: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::DoubleBuilder>(array_builder));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    double value = data_getter.GetDouble(pos);
                    return field_builder->Append(value);
                });
        }
        case arrow::Type::type::STRING: {
            (*reserve_count)++;
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::BinaryBuilder>(array_builder));
            if (use_view) {
                return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                    [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                        CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                        auto view = data_getter.GetStringView(pos);
                        return field_builder->Append(view.data(), view.size());
                    });
            }
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    auto str = data_getter.GetString(pos).ToString();
                    return field_builder->Append(str.data(), str.size());
                });
        }
        case arrow::Type::type::BINARY: {
            (*reserve_count)++;
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::BinaryBuilder>(array_builder));
            if (use_view) {
                return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                    [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                        CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                        auto view = data_getter.GetStringView(pos);
                        return field_builder->Append(view.data(), view.size());
                    });
            }
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder](const DataGetters& data_getter, int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    auto bytes = data_getter.GetBinary(pos);
                    assert(bytes);
                    return field_builder->Append(bytes->data(), bytes->size());
                });
        }
        case arrow::Type::type::TIMESTAMP: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::TimestampBuilder>(array_builder));
            auto ts_type =
                arrow::internal::checked_pointer_cast<arrow::TimestampType>(field_builder->type());
            if (!ts_type) {
                return Status::Invalid("cannot cast to timestamp type");
            }
            DateTimeUtils::TimeType time_type = DateTimeUtils::GetTimeTypeFromArrowType(ts_type);
            int32_t precision = DateTimeUtils::GetPrecisionFromType(ts_type);
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder, precision, time_type](const DataGetters& data_getter,
                                                      int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    Timestamp timestamp = data_getter.GetTimestamp(pos, precision);
                    return field_builder->Append(
                        DateTimeUtils::TimestampToInteger(timestamp, time_type));
                });
        }
        case arrow::Type::type::DECIMAL: {
            PAIMON_ASSIGN_OR_RAISE(auto* field_builder,
                                   CastToTypedBuilder<arrow::Decimal128Builder>(array_builder));
            auto decimal_type =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(field_builder->type().get());
            if (!decimal_type) {
                return Status::Invalid("cannot cast to decimal type");
            }
            auto precision = decimal_type->precision();
            auto scale = decimal_type->scale();
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [field_builder, precision, scale](const DataGetters& data_getter,
                                                  int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, field_builder, pos);
                    Decimal value = data_getter.GetDecimal(pos, precision, scale);
                    return field_builder->Append(
                        arrow::Decimal128(value.HighBits(), value.LowBits()));
                });
        }
        case arrow::Type::type::LIST: {
            PAIMON_ASSIGN_OR_RAISE(auto* list_builder,
                                   CastToTypedBuilder<arrow::ListBuilder>(array_builder));
            PAIMON_ASSIGN_OR_RAISE(AppendValueFunc value_func,
                                   (RowToArrowArrayConverter<T, R>::AppendField(
                                       use_view, list_builder->value_builder(), reserve_count)));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [list_builder, value_func](const DataGetters& data_getter,
                                           int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, list_builder, pos);
                    ARROW_RETURN_NOT_OK(list_builder->Append());
                    auto sub_array = data_getter.GetArray(pos);
                    assert(sub_array);
                    for (int32_t i = 0; i < sub_array->Size(); i++) {
                        ARROW_RETURN_NOT_OK(value_func(*sub_array, i));
                    }
                    return arrow::Status::OK();
                });
        }
        case arrow::Type::type::MAP: {
            PAIMON_ASSIGN_OR_RAISE(auto* map_builder,
                                   CastToTypedBuilder<arrow::MapBuilder>(array_builder));
            PAIMON_ASSIGN_OR_RAISE(AppendValueFunc key_func,
                                   (RowToArrowArrayConverter<T, R>::AppendField(
                                       use_view, map_builder->key_builder(), reserve_count)));
            PAIMON_ASSIGN_OR_RAISE(AppendValueFunc item_func,
                                   (RowToArrowArrayConverter<T, R>::AppendField(
                                       use_view, map_builder->item_builder(), reserve_count)));
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [map_builder, key_func, item_func](const DataGetters& data_getter,
                                                   int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, map_builder, pos);
                    ARROW_RETURN_NOT_OK(map_builder->Append());
                    auto sub_map = data_getter.GetMap(pos);
                    assert(sub_map);
                    auto key_array = sub_map->KeyArray();
                    auto item_array = sub_map->ValueArray();
                    for (int32_t i = 0; i < sub_map->Size(); i++) {
                        ARROW_RETURN_NOT_OK(key_func(*key_array, i));
                        ARROW_RETURN_NOT_OK(item_func(*item_array, i));
                    }
                    return arrow::Status::OK();
                });
        }
        case arrow::Type::type::STRUCT: {
            PAIMON_ASSIGN_OR_RAISE(auto* struct_builder,
                                   CastToTypedBuilder<arrow::StructBuilder>(array_builder));
            std::vector<RowToArrowArrayConverter<T, R>::AppendValueFunc> sub_funcs;
            sub_funcs.reserve(struct_builder->num_fields());
            for (int32_t i = 0; i < struct_builder->num_fields(); i++) {
                PAIMON_ASSIGN_OR_RAISE(
                    AppendValueFunc sub_func,
                    (RowToArrowArrayConverter<T, R>::AppendField(
                        use_view, struct_builder->field_builder(i), reserve_count)));
                sub_funcs.push_back(std::move(sub_func));
            }
            return RowToArrowArrayConverter<T, R>::AppendValueFunc(
                [struct_builder, sub_funcs](const DataGetters& data_getter,
                                            int32_t pos) -> arrow::Status {
                    CHECK_AND_APPEND_NULL(data_getter, struct_builder, pos);
                    ARROW_RETURN_NOT_OK(struct_builder->Append());
                    auto sub_row = data_getter.GetRow(pos, sub_funcs.size());
                    assert(sub_row);
                    assert(sub_funcs.size() == static_cast<size_t>(struct_builder->num_fields()));
                    for (size_t i = 0; i < sub_funcs.size(); i++) {
                        ARROW_RETURN_NOT_OK(sub_funcs[i](*sub_row, i));
                    }
                    return arrow::Status::OK();
                });
        }
        default:
            return Status::Invalid(fmt::format("Do not support type {} in RowToArrowArrayConverter",
                                               array_builder->type()->ToString()));
    }
}

}  // namespace paimon
