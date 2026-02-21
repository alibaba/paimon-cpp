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

#include "paimon/core/io/key_value_data_file_record_reader.h"

#include <cassert>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/status.h"

namespace paimon {
class MemoryPool;

KeyValueDataFileRecordReader::KeyValueDataFileRecordReader(
    std::unique_ptr<BatchReader>&& reader, int32_t key_arity,
    const std::shared_ptr<arrow::Schema>& value_schema, int32_t level,
    const std::shared_ptr<MemoryPool>& pool)
    : key_arity_(key_arity),
      level_(level),
      pool_(pool),
      reader_(std::move(reader)),
      value_schema_(value_schema),
      value_names_(value_schema_->field_names()) {}

bool KeyValueDataFileRecordReader::Iterator::HasNext() const {
    int64_t array_length = reader_->row_kind_array_->length();
    const auto& selection_bitmap = reader_->selection_bitmap_;
    if (selection_bitmap.Cardinality() == array_length) {
        // all rows are selected in bitmap
        return cursor_ < array_length;
    }
    auto iter = selection_bitmap.EqualOrLarger(cursor_);
    if (iter == selection_bitmap.End()) {
        // no row are selected
        return false;
    }
    // find the first selected row
    cursor_ = *iter;
    assert(cursor_ < array_length);
    return true;
}

Result<KeyValue> KeyValueDataFileRecordReader::Iterator::Next() {
    assert(HasNext());
    // key is only used in merge sort; key context does not hold parent struct array
    auto key = std::make_unique<ColumnarRowRef>(reader_->key_ctx_, cursor_);
    // value is used in merge sort and projection (maybe async and multi-thread), so value context
    // holds parent struct array to ensure data remains valid
    auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, cursor_);
    PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                           RowKind::FromByteValue(reader_->row_kind_array_->Value(cursor_)));
    int64_t sequence_number = reader_->sequence_number_array_->Value(cursor_);
    cursor_++;
    // TODO(xinyu.lxy): reuse KeyValue and ColumnarRow to avoid construct and destruction
    return KeyValue(row_kind, sequence_number, reader_->level_, std::move(key), std::move(value));
}

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> KeyValueDataFileRecordReader::NextBatch() {
    Reset();
    PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatchWithBitmap batch_with_bitmap,
                           reader_->NextBatchWithBitmap());
    if (BatchReader::IsEofBatch(batch_with_bitmap)) {
        // reader eof, just return
        return std::unique_ptr<KeyValueRecordReader::Iterator>();
    }
    auto& [array, bitmap] = batch_with_bitmap;
    auto& [c_array, c_schema] = array;
    if (bitmap.IsEmpty()) {
        return Status::Invalid("KeyValueRecordReader should not accept empty batch");
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                      arrow::ImportArray(c_array.get(), c_schema.get()));
    auto* data_batch = arrow::internal::checked_cast<arrow::StructArray*>(arrow_array.get());
    assert(data_batch);
    // do not use arrow::checked_pointer_cast as in release compile, checked_pointer_cast is
    // static_cast without check
    sequence_number_array_ =
        std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int64Type>>(data_batch->field(0));
    if (!sequence_number_array_) {
        return Status::Invalid("cannot cast SEQUENCE_NUMBER column to int64 arrow array");
    }
    row_kind_array_ =
        std::dynamic_pointer_cast<arrow::NumericArray<arrow::Int8Type>>(data_batch->field(1));
    if (!row_kind_array_) {
        return Status::Invalid("cannot cast VALUE_KIND column to int8 arrow array");
    }

    key_fields_.reserve(key_arity_);
    for (int32_t i = 0; i < key_arity_; i++) {
        // skip special fields
        key_fields_.emplace_back(
            data_batch->field(i + SpecialFields::KEY_VALUE_SPECIAL_FIELD_COUNT));
    }
    // e.g., file schema:    seq, kind, key1, key2, s1, s2, v1, v2
    // user raw read schema: key1, v1, s1
    // format reader read schema: seq, kind, key1, key2, v1, s1, s2
    // in KeyValue object: key: key1, key2 / value: key1, v1, s1, s2
    value_fields_.reserve(value_schema_->num_fields());
    for (const auto& value_field : value_schema_->fields()) {
        auto field_array = data_batch->GetFieldByName(value_field->name());
        if (!field_array) {
            return Status::Invalid(
                fmt::format("cannot find field {} in data batch", value_field->name()));
        }
        value_fields_.emplace_back(field_array);
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(value_struct_array_,
                                      arrow::StructArray::Make(value_fields_, value_names_));
    selection_bitmap_ = std::move(bitmap);
    value_fields_ = value_struct_array_->fields();
    key_ctx_ = std::make_shared<ColumnarBatchContext>(nullptr, key_fields_, pool_);
    value_ctx_ = std::make_shared<ColumnarBatchContext>(value_struct_array_, value_fields_, pool_);
    TraverseArray(value_struct_array_);
    return std::make_unique<KeyValueDataFileRecordReader::Iterator>(this);
}

void KeyValueDataFileRecordReader::Reset() {
    selection_bitmap_ = RoaringBitmap32();
    key_ctx_.reset();
    value_ctx_.reset();
    key_fields_.clear();
    value_fields_.clear();
    value_struct_array_.reset();
    sequence_number_array_.reset();
    row_kind_array_.reset();
}

void KeyValueDataFileRecordReader::TraverseArray(const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type type = array->type()->id();
    switch (type) {
        case arrow::Type::type::DICTIONARY: {
            auto* dict_array = arrow::internal::checked_cast<arrow::DictionaryArray*>(array.get());
            [[maybe_unused]] auto dict = dict_array->dictionary();
            return;
        }
        case arrow::Type::type::STRUCT: {
            auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(array.get());
            for (const auto& field : struct_array->fields()) {
                TraverseArray(field);
            }
            return;
        }
        case arrow::Type::type::MAP: {
            auto* map_array = arrow::internal::checked_cast<arrow::MapArray*>(array.get());
            TraverseArray(map_array->keys());
            TraverseArray(map_array->items());
            return;
        }
        case arrow::Type::type::LIST: {
            auto* list_array = arrow::internal::checked_cast<arrow::ListArray*>(array.get());
            TraverseArray(list_array->values());
            return;
        }
        default:
            return;
    }
}

}  // namespace paimon
