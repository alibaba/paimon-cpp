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

#include "paimon/core/io/key_value_in_memory_record_reader.h"

#include <cassert>
#include <utility>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/ordering.h"
#include "arrow/util/checked_cast.h"
#include "fmt/format.h"
#include "paimon/common/data/columnar/columnar_row_ref.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/status.h"
namespace paimon {
class MemoryPool;

Result<KeyValue> KeyValueInMemoryRecordReader::Iterator::Next() {
    uint64_t index = reader_->sort_indices_->Value(cursor_++);
    const RowKind* row_kind = RowKind::Insert();
    if (!reader_->row_kinds_.empty()) {
        PAIMON_ASSIGN_OR_RAISE(
            row_kind, RowKind::FromByteValue(static_cast<int8_t>(reader_->row_kinds_[index])));
    }

    // key must hold value_struct_array as min/max key may be used after projection
    auto key = std::make_unique<ColumnarRowRef>(reader_->key_ctx_, index);
    auto value = std::make_unique<ColumnarRowRef>(reader_->value_ctx_, index);
    return KeyValue(row_kind, reader_->last_sequence_num_ + index,
                    /*level=*/KeyValue::UNKNOWN_LEVEL, std::move(key), std::move(value));
}

KeyValueInMemoryRecordReader::KeyValueInMemoryRecordReader(
    int64_t last_sequence_num, const std::shared_ptr<arrow::StructArray>& struct_array,
    const std::vector<RecordBatch::RowKind>& row_kinds,
    const std::vector<std::string>& primary_keys,
    const std::vector<std::string>& user_defined_sequence_fields, bool sequence_fields_ascending,
    const std::shared_ptr<FieldsComparator>& key_comparator,
    const std::shared_ptr<MemoryPool>& pool)
    : last_sequence_num_(last_sequence_num),
      primary_keys_(primary_keys),
      user_defined_sequence_fields_(user_defined_sequence_fields),
      sequence_fields_ascending_(sequence_fields_ascending),
      pool_(pool),
      value_struct_array_(struct_array),
      row_kinds_(row_kinds),
      key_comparator_(key_comparator) {
    assert(value_struct_array_);
    ArrowUtils::TraverseArray(value_struct_array_);
}

Result<std::unique_ptr<KeyValueRecordReader::Iterator>> KeyValueInMemoryRecordReader::NextBatch() {
    if (visited_) {
        return std::unique_ptr<KeyValueInMemoryRecordReader::Iterator>();
    }
    visited_ = true;
    arrow::ArrayVector key_fields;
    key_fields.reserve(primary_keys_.size());
    for (const auto& key : primary_keys_) {
        auto key_array = value_struct_array_->GetFieldByName(key);
        if (!key_array) {
            return Status::Invalid(fmt::format("cannot find field {} in data batch", key));
        }
        key_fields.emplace_back(key_array);
    }
    arrow::ArrayVector value_fields;
    value_fields.reserve(value_struct_array_->num_fields());
    for (int32_t i = 0; i < value_struct_array_->num_fields(); i++) {
        value_fields.push_back(value_struct_array_->field(i));
    }
    key_ctx_ = std::make_shared<ColumnarBatchContext>(key_fields, pool_);
    value_ctx_ = std::make_shared<ColumnarBatchContext>(value_fields, pool_);

    PAIMON_ASSIGN_OR_RAISE(sort_indices_, SortBatch());
    return std::make_unique<KeyValueInMemoryRecordReader::Iterator>(this);
}

void KeyValueInMemoryRecordReader::Close() {
    visited_ = true;
    value_struct_array_.reset();
    row_kinds_.clear();
    sort_indices_.reset();
    key_ctx_.reset();
    value_ctx_.reset();
}

Result<std::shared_ptr<arrow::NumericArray<arrow::UInt64Type>>>
KeyValueInMemoryRecordReader::SortBatch() const {
    std::vector<arrow::compute::SortKey> sort_keys;
    sort_keys.reserve(primary_keys_.size() + user_defined_sequence_fields_.size());
    for (const auto& name : primary_keys_) {
        sort_keys.emplace_back(name, arrow::compute::SortOrder::Ascending);
    }
    const auto sequence_sort_order = sequence_fields_ascending_
                                         ? arrow::compute::SortOrder::Ascending
                                         : arrow::compute::SortOrder::Descending;
    for (const auto& name : user_defined_sequence_fields_) {
        sort_keys.emplace_back(name, sequence_sort_order);
    }
    auto sort_options =
        arrow::compute::SortOptions(sort_keys, arrow::compute::NullPlacement::AtStart);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Array> sorted_indices,
        arrow::compute::SortIndices(arrow::Datum(value_struct_array_), sort_options));
    auto typed_indices =
        arrow::internal::checked_pointer_cast<arrow::NumericArray<arrow::UInt64Type>>(
            sorted_indices);
    if (!typed_indices) {
        return Status::Invalid("cannot cast sorted indices to UInt64Array");
    }
    return typed_indices;
}

}  // namespace paimon
