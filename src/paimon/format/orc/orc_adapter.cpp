// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Adapted from Apache Arrow
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/adapters/orc/util.cc

#include "paimon/format/orc/orc_adapter.h"

#include <cassert>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_decimal.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer_builder.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/range.h"
#include "arrow/visit_data_inline.h"
#include "fmt/format.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/timestamp.h"

namespace paimon::orc {
namespace {
template <typename T>
class OrcBackedArrowBuffer : public arrow::ResizableBuffer {
 public:
    explicit OrcBackedArrowBuffer(::orc::DataBuffer<T>&& buffer)
        : ResizableBuffer(reinterpret_cast<uint8_t*>(buffer.data()), buffer.size() * sizeof(T)),
          orc_buffer_(std::move(buffer)) {}

    arrow::Status Resize(const int64_t new_size, bool shrink_to_fit) override {
        try {
            // arrow buffer size is equal to orc buffer size * sizeof(T)
            orc_buffer_.resize(new_size / sizeof(T));
        } catch (const std::exception& e) {
            return arrow::Status::Invalid(
                fmt::format("OrcBackedArrowBuffer resize failed for {}", e.what()));
        }
        arrow::ResizableBuffer::data_ = reinterpret_cast<uint8_t*>(orc_buffer_.data());
        arrow::ResizableBuffer::size_ = orc_buffer_.size() * sizeof(T);
        arrow::ResizableBuffer::capacity_ = orc_buffer_.capacity() * sizeof(T);
        return arrow::Status::OK();
    }

    arrow::Status Reserve(const int64_t new_capacity) override {
        try {
            // arrow buffer capacity is equal to orc buffer capacity * sizeof(T)
            orc_buffer_.reserve(new_capacity / sizeof(T));
        } catch (const std::exception& e) {
            return arrow::Status::Invalid(
                fmt::format("OrcBackedArrowBuffer reserve failed for {}", e.what()));
        }
        arrow::ResizableBuffer::data_ = reinterpret_cast<uint8_t*>(orc_buffer_.data());
        arrow::ResizableBuffer::capacity_ = orc_buffer_.capacity() * sizeof(T);
        return arrow::Status::OK();
    }

 private:
    ::orc::DataBuffer<T> orc_buffer_;
};

class EmptyBuilder : public arrow::ArrayBuilder {
 public:
    using arrow::ArrayBuilder::SetNotNull;
    explicit EmptyBuilder(arrow::MemoryPool* pool) : arrow::ArrayBuilder(pool) {}
    arrow::Status AppendNulls(int64_t length) override {
        return arrow::Status::NotImplemented("AppendNulls is not implemented");
    }

    arrow::Status AppendNull() override {
        return arrow::Status::NotImplemented("AppendNull is not implemented");
    }

    arrow::Status AppendEmptyValue() override {
        return arrow::Status::NotImplemented("AppendEmptyValue is not implemented");
    }

    arrow::Status AppendEmptyValues(int64_t length) override {
        return arrow::Status::NotImplemented("AppendEmptyValues is not implemented");
    }

    // when null_count == 0 in array, null_bitmap buffer can be null, thus, there is no need to
    // construct null bitmap, use IncreaseLength() to increase the length_ of array
    virtual void IncreaseLength(int64_t length) {
        length_ += length;
        has_nulls_ = false;
    }

 protected:
    bool has_nulls_ = true;
};

class UnPooledBooleanBuilder : public EmptyBuilder {
 public:
    explicit UnPooledBooleanBuilder(arrow::MemoryPool* pool)
        : EmptyBuilder(pool),
          type_(arrow::boolean()),
          data_builder_(std::make_shared<arrow::TypedBufferBuilder<bool>>(pool)) {}

    std::shared_ptr<arrow::DataType> type() const override {
        return type_;
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }

    arrow::Status SetData(const uint8_t* data, int64_t length) {
        ARROW_RETURN_NOT_OK(data_builder_->Reserve(length));
        data_builder_->UnsafeAppend(data, length);
        return arrow::Status::OK();
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_ASSIGN_OR_RAISE(null_bitmap, null_bitmap_builder_.FinishWithLength(length_));
        }
        ARROW_ASSIGN_OR_RAISE(auto data, data_builder_->FinishWithLength(length_));
        *out = arrow::ArrayData::Make(type_, length_, {null_bitmap, data}, null_count_);
        Reset();
        return arrow::Status::OK();
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    const std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<arrow::TypedBufferBuilder<bool>> data_builder_;
};

template <typename Type>
class UnPooledPrimitiveBuilder : public arrow::NumericBuilder<Type> {
 public:
    UnPooledPrimitiveBuilder(const std::shared_ptr<arrow::DataType>& type, arrow::MemoryPool* pool)
        : arrow::NumericBuilder<Type>(type, pool) {}

    void SetData(const std::shared_ptr<arrow::Buffer>& data) {
        data_ = data;
    }

    void IncreaseLength(int64_t length) {
        has_nulls_ = false;
        this->length_ += length;
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(this->null_bitmap_builder_.Finish(&null_bitmap));
        }
        *out = arrow::ArrayData::Make(this->type_, this->length_, {null_bitmap, data_},
                                      this->null_count_);
        Reset();
        return arrow::Status::OK();
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    bool has_nulls_ = true;
    std::shared_ptr<arrow::Buffer> data_;
};

class UnPooledLargeBinaryBuilder : public arrow::LargeBinaryBuilder {
 public:
    using arrow::ArrayBuilder::SetNotNull;
    UnPooledLargeBinaryBuilder(const std::shared_ptr<arrow::DataType>& type,
                               arrow::MemoryPool* pool)
        : arrow::LargeBinaryBuilder(pool), type_(type) {}

    void SetOffsets(const std::shared_ptr<arrow::Buffer>& offsets) {
        offsets_ = offsets;
    }
    void SetData(const std::shared_ptr<arrow::Buffer>& data) {
        data_ = data;
    }
    void IncreaseLength(int64_t length) {
        has_nulls_ = false;
        length_ += length;
    }
    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }
    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
        }
        *out = arrow::ArrayData::Make(type_, length_, {null_bitmap, offsets_, data_}, null_count_);
        Reset();
        return arrow::Status::OK();
    }
    std::shared_ptr<arrow::DataType> type() const override {
        return type_;
    }
    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    bool has_nulls_ = true;
    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<arrow::Buffer> offsets_;
    std::shared_ptr<arrow::Buffer> data_;
};

class UnPooledBinaryBuilder : public arrow::BinaryBuilder {
 public:
    using arrow::ArrayBuilder::SetNotNull;
    UnPooledBinaryBuilder(const std::shared_ptr<arrow::DataType>& type, arrow::MemoryPool* pool)
        : arrow::BinaryBuilder(pool), type_(type) {}

    void SetOffsets(const std::shared_ptr<arrow::Buffer>& offsets) {
        offsets_ = offsets;
    }

    void SetData(const std::shared_ptr<arrow::Buffer>& data) {
        data_ = data;
    }

    void IncreaseLength(int64_t length) {
        has_nulls_ = false;
        length_ += length;
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
        }
        *out = arrow::ArrayData::Make(type_, length_, {null_bitmap, offsets_, data_}, null_count_);
        Reset();
        return arrow::Status::OK();
    }

    std::shared_ptr<arrow::DataType> type() const override {
        return type_;
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    bool has_nulls_ = true;
    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<arrow::Buffer> offsets_;
    std::shared_ptr<arrow::Buffer> data_;
};

class UnPooledListBuilder : public EmptyBuilder {
 public:
    UnPooledListBuilder(const std::shared_ptr<arrow::DataType>& type,
                        const std::shared_ptr<arrow::ArrayBuilder>& value_builder,
                        arrow::MemoryPool* pool)
        : EmptyBuilder(pool), type_(type), value_builder_(value_builder) {}

    std::shared_ptr<arrow::DataType> type() const override {
        return arrow::list(value_builder_->type());
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }

    void SetOffsets(const std::shared_ptr<arrow::Buffer>& offsets) {
        offsets_ = offsets;
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        // Offset padding zeroed by BufferBuilder
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
        }
        std::shared_ptr<arrow::ArrayData> items;
        ARROW_RETURN_NOT_OK(value_builder_->FinishInternal(&items));

        *out =
            arrow::ArrayData::Make(type(), length_, {null_bitmap, offsets_}, {items}, null_count_);
        Reset();
        return arrow::Status::OK();
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<arrow::Buffer> offsets_;
    std::shared_ptr<arrow::ArrayBuilder> value_builder_;
};
class UnPooledStructBuilder : public EmptyBuilder {
 public:
    UnPooledStructBuilder(const std::shared_ptr<arrow::DataType>& type,
                          const std::vector<std::shared_ptr<arrow::ArrayBuilder>>& field_builders,
                          arrow::MemoryPool* pool)
        : EmptyBuilder(pool), type_(type), children_(field_builders) {}

    std::shared_ptr<arrow::DataType> type() const override {
        arrow::FieldVector fields;
        fields.reserve(children_.size());
        for (size_t i = 0; i < children_.size(); i++) {
            fields.emplace_back(arrow::field(type_->field(i)->name(), children_[i]->type()));
        }
        return arrow::struct_(fields);
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
        }
        std::vector<std::shared_ptr<arrow::ArrayData>> child_data(children_.size());
        for (size_t i = 0; i < children_.size(); i++) {
            ARROW_RETURN_NOT_OK(children_[i]->FinishInternal(&child_data[i]));
        }

        *out = arrow::ArrayData::Make(type(), length_, {null_bitmap}, null_count_);
        (*out)->child_data = std::move(child_data);
        Reset();
        return arrow::Status::OK();
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    std::shared_ptr<arrow::DataType> type_;
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> children_;
};

class UnPooledMapBuilder : public EmptyBuilder {
 public:
    UnPooledMapBuilder(const std::shared_ptr<arrow::DataType>& type,
                       const std::shared_ptr<arrow::ArrayBuilder>& struct_builder,
                       arrow::MemoryPool* pool)
        : EmptyBuilder(pool), type_(type) {
        list_builder_ = std::make_shared<UnPooledListBuilder>(arrow::list(struct_builder->type()),
                                                              struct_builder, pool);
    }

    std::shared_ptr<arrow::DataType> type() const override {
        auto map_type = arrow::internal::checked_cast<arrow::MapType*>(type_.get());
        auto list_type =
            arrow::internal::checked_pointer_cast<arrow::ListType>(list_builder_->type());
        return std::make_shared<arrow::MapType>(arrow::field("entries", list_type->value_type()),
                                                map_type->keys_sorted());
    }

    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return list_builder_->SetNulls(valid_bytes, length);
    }

    void IncreaseLength(int64_t length) override {
        list_builder_->IncreaseLength(length);
    }

    void SetOffsets(const std::shared_ptr<arrow::Buffer>& offsets) {
        list_builder_->SetOffsets(offsets);
    }

    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        ARROW_RETURN_NOT_OK(list_builder_->FinishInternal(out));
        (*out)->type = type();
        Reset();
        return arrow::Status::OK();
    }

    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    std::shared_ptr<arrow::DataType> type_;
    std::shared_ptr<UnPooledListBuilder> list_builder_;
};

template <typename BuilderType>
Status BuilderSetNulls(::orc::ColumnVectorBatch* column_vector_batch, BuilderType* builder) {
    if (column_vector_batch->hasNulls) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder->SetNulls(reinterpret_cast<uint8_t*>(column_vector_batch->notNull.data()),
                              column_vector_batch->numElements));
    } else {
        builder->IncreaseLength(column_vector_batch->numElements);
    }
    return Status::OK();
}

class OrcStringDictionary : public arrow::ArrayData {
 public:
    OrcStringDictionary(const std::shared_ptr<arrow::ArrayData>& data,
                        const std::shared_ptr<::orc::StringDictionary>& orc_dictionary)
        : arrow::ArrayData(*data), orc_dictionary_(orc_dictionary) {}

 private:
    const std::shared_ptr<::orc::StringDictionary> orc_dictionary_;
};

class UnPooledStringDictionaryBuilder : public EmptyBuilder {
 public:
    explicit UnPooledStringDictionaryBuilder(arrow::MemoryPool* pool)
        : EmptyBuilder(pool), pool_(pool) {}

    arrow::Status SetDictionary(const std::shared_ptr<::orc::StringDictionary>& orc_dictionary) {
        auto dict_builder =
            std::make_shared<UnPooledLargeBinaryBuilder>(arrow::large_utf8(), pool_);
        // We do not transfer buffer ownership to arrow here. The memory still
        // owned by dictionary which is hold in a shared_ptr.
        const auto& dict_offset = orc_dictionary->dictionaryOffset;
        const auto& dict_data = orc_dictionary->dictionaryBlob;
        auto offsets =
            std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(dict_offset.data()),
                                            dict_offset.size() * sizeof(int64_t));
        auto data = std::make_shared<arrow::Buffer>(
            reinterpret_cast<const uint8_t*>(dict_data.data()), dict_data.size());
        dict_builder->SetData(data);
        dict_builder->SetOffsets(offsets);
        dict_builder->IncreaseLength(dict_offset.size() - 1);
        std::shared_ptr<arrow::Array> dictionary;
        ARROW_RETURN_NOT_OK(dict_builder->Finish(&dictionary));
        dictionary_ = std::make_shared<OrcStringDictionary>(dictionary->data(), orc_dictionary);
        return arrow::Status::OK();
    }

    void SetIndices(const std::shared_ptr<arrow::Buffer>& indices) {
        indices_ = indices;
    }
    arrow::Status SetNulls(const uint8_t* valid_bytes, int64_t length) {
        return arrow::ArrayBuilder::AppendToBitmap(valid_bytes, length);
    }
    arrow::Status FinishInternal(std::shared_ptr<arrow::ArrayData>* out) override {
        std::shared_ptr<arrow::Buffer> null_bitmap;
        if (has_nulls_) {
            ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
        }
        *out = arrow::ArrayData::Make(type(), length_, {null_bitmap, indices_}, null_count_);
        (*out)->dictionary = dictionary_;
        Reset();
        return arrow::Status::OK();
    }
    std::shared_ptr<arrow::DataType> type() const override {
        static std::shared_ptr<arrow::DataType> dict_type =
            arrow::dictionary(arrow::int64(), arrow::large_utf8());
        return dict_type;
    }
    void Reset() override {
        arrow::ArrayBuilder::Reset();
    }

 private:
    arrow::MemoryPool* pool_;
    std::shared_ptr<arrow::Buffer> indices_;
    std::shared_ptr<arrow::ArrayData> dictionary_;
};

Status CheckOutOfBounds(const ::orc::StringVectorBatch* string_vector_batch) {
    int64_t total_length = 0;
    bool has_nulls = string_vector_batch->hasNulls;
    for (uint64_t i = 0; i < string_vector_batch->numElements; ++i) {
        if (has_nulls && !string_vector_batch->notNull[i]) {
            continue;
        }
        if (string_vector_batch->length[i] > std::numeric_limits<int32_t>::max()) {
            return Status::Invalid(fmt::format("index {} with length {} is out-of-bounds", i,
                                               string_vector_batch->length[i]));
        }
        total_length += string_vector_batch->length[i];
        if (total_length > std::numeric_limits<int32_t>::max()) {
            return Status::Invalid("total length is out-of-bounds");
        }
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::Buffer>> PrepareOffsetsBufferForString(
    const ::orc::StringVectorBatch* string_vector_batch, arrow::MemoryPool* pool) {
    // TODO(liancheng.lsz): length for StringVectorBatch in orc is int64, offset for string in arrow
    // array is int32, we only support int32 for now. large_string & large_binary will be supported
    // in the future.
    PAIMON_RETURN_NOT_OK(CheckOutOfBounds(string_vector_batch));
    int64_t buffer_size = (string_vector_batch->numElements + 1) * sizeof(int32_t);
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> data_buffer,
                                      arrow::AllocateBuffer(buffer_size, pool));
    auto* offsets_data = data_buffer->mutable_data_as<int32_t>();
    int32_t cur_offset = 0;
    bool has_nulls = string_vector_batch->hasNulls;
    for (size_t i = 0; i < string_vector_batch->numElements; i++) {
        int32_t tmp_length =
            (!has_nulls) ? string_vector_batch->length[i]
                         : (string_vector_batch->notNull[i] ? string_vector_batch->length[i] : 0);
        offsets_data[i] = cur_offset;
        cur_offset += tmp_length;
    }
    offsets_data[string_vector_batch->numElements] = cur_offset;
    return data_buffer;
}

Result<std::shared_ptr<arrow::Buffer>> DeepCopyDataBufferForString(
    const ::orc::StringVectorBatch* string_vector_batch, arrow::MemoryPool* pool) {
    int64_t data_size = 0;
    bool has_nulls = string_vector_batch->hasNulls;
    for (uint64_t i = 0; i < string_vector_batch->numElements; ++i) {
        if (!has_nulls || string_vector_batch->notNull[i]) {
            data_size += string_vector_batch->length[i];
        }
    }
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Buffer> data_buffer,
                                      arrow::AllocateBuffer(data_size, pool));
    char* data = data_buffer->mutable_data_as<char>();
    int32_t dest_offset = 0;
    for (uint64_t i = 0; i < string_vector_batch->numElements; ++i) {
        if (!has_nulls || string_vector_batch->notNull[i]) {
            memcpy(data + dest_offset, string_vector_batch->data[i],
                   string_vector_batch->length[i]);
            dest_offset += string_vector_batch->length[i];
        }
    }
    return data_buffer;
}

template <typename BuilderType, typename BatchType, typename ValueType>
Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedPrimitiveBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    auto typed_batch = dynamic_cast<BatchType*>(column_vector_batch);
    assert(typed_batch);
    auto builder = std::make_shared<BuilderType>(type, pool);
    builder->SetData(
        std::make_shared<OrcBackedArrowBuffer<ValueType>>(std::move(typed_batch->data)));
    PAIMON_RETURN_NOT_OK(BuilderSetNulls<BuilderType>(column_vector_batch, builder.get()));
    return builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedBooleanBuilder(
    ::orc::ColumnVectorBatch* column_vector_batch, arrow::MemoryPool* pool) {
    auto typed_batch = dynamic_cast<::orc::ByteVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto builder = std::make_shared<UnPooledBooleanBuilder>(pool);
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->SetData(
        reinterpret_cast<uint8_t*>(typed_batch->data.data()), typed_batch->numElements));
    PAIMON_RETURN_NOT_OK(
        BuilderSetNulls<UnPooledBooleanBuilder>(column_vector_batch, builder.get()));
    return builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedDate32Builder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    auto typed_batch = dynamic_cast<::orc::LongVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto builder = std::make_shared<arrow::Date32Builder>(type, pool);

    const uint8_t* valid_bytes = typed_batch->hasNulls
                                     ? reinterpret_cast<const uint8_t*>(typed_batch->notNull.data())
                                     : nullptr;
    auto transform_iter = arrow::internal::MakeLazyRange(
        [&typed_batch](int64_t index) {
            return static_cast<int32_t>(typed_batch->data.data()[index]);
        },
        typed_batch->numElements);

    PAIMON_RETURN_NOT_OK_FROM_ARROW(
        builder->AppendValues(transform_iter.begin(), transform_iter.end(), valid_bytes));
    return builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedTimestampBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    auto typed_batch = dynamic_cast<::orc::TimestampVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto builder = std::make_shared<arrow::TimestampBuilder>(type, pool);

    const uint8_t* valid_bytes = typed_batch->hasNulls
                                     ? reinterpret_cast<const uint8_t*>(typed_batch->notNull.data())
                                     : nullptr;
    const int64_t* seconds = typed_batch->data.data();
    const int64_t* nanos = typed_batch->nanoseconds.data();
    auto timestamp_type = arrow::internal::checked_pointer_cast<arrow::TimestampType>(type);
    assert(timestamp_type);
    int32_t precision = DateTimeUtils::GetPrecisionFromType(timestamp_type);
    // TODO(lisizhuo.lsz): check nano overflow in arrow
    if (precision == Timestamp::MIN_PRECISION) {
        auto transform_iter = arrow::internal::MakeLazyRange(
            [seconds](int64_t index) { return seconds[index]; }, typed_batch->numElements);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder->AppendValues(transform_iter.begin(), transform_iter.end(), valid_bytes));
        return builder;
    } else if (precision == Timestamp::MILLIS_PRECISION) {
        auto transform_iter = arrow::internal::MakeLazyRange(
            [seconds, nanos](int64_t index) {
                return seconds[index] *
                           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::MILLISECOND] +
                       nanos[index] /
                           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::MICROSECOND];
            },
            typed_batch->numElements);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder->AppendValues(transform_iter.begin(), transform_iter.end(), valid_bytes));
        return builder;
    } else if (precision == Timestamp::DEFAULT_PRECISION) {
        auto transform_iter = arrow::internal::MakeLazyRange(
            [seconds, nanos](int64_t index) {
                return seconds[index] *
                           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::MICROSECOND] +
                       nanos[index] /
                           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::MILLISECOND];
            },
            typed_batch->numElements);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder->AppendValues(transform_iter.begin(), transform_iter.end(), valid_bytes));
        return builder;
    } else if (precision == Timestamp::MAX_PRECISION) {
        auto transform_iter = arrow::internal::MakeLazyRange(
            [seconds, nanos](int64_t index) {
                return seconds[index] *
                           DateTimeUtils::CONVERSION_FACTORS[DateTimeUtils::TimeType::NANOSECOND] +
                       nanos[index];
            },
            typed_batch->numElements);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            builder->AppendValues(transform_iter.begin(), transform_iter.end(), valid_bytes));
        return builder;
    }
    return Status::Invalid(fmt::format("invalid timestamp precision {}", precision));
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedDecimal128Builder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    auto builder = std::make_shared<arrow::Decimal128Builder>(type, pool);
    const bool has_nulls = column_vector_batch->hasNulls;
    auto decimal_type = arrow::internal::checked_cast<arrow::Decimal128Type*>(type.get());
    assert(decimal_type);
    if (decimal_type->precision() == 0 || decimal_type->precision() > 18) {
        auto typed_batch =
            arrow::internal::checked_cast<const ::orc::Decimal128VectorBatch*>(column_vector_batch);
        for (size_t i = 0; i < typed_batch->numElements; i++) {
            if (!has_nulls || typed_batch->notNull[i]) {
                int64_t high_bits = typed_batch->values[i].getHighBits();
                uint64_t low_bits = typed_batch->values[i].getLowBits();
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    builder->Append(arrow::Decimal128(high_bits, low_bits)));
            } else {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
            }
        }
    } else {
        auto typed_batch =
            arrow::internal::checked_cast<const ::orc::Decimal64VectorBatch*>(column_vector_batch);
        for (size_t i = 0; i < typed_batch->numElements; i++) {
            if (!has_nulls || typed_batch->notNull[i]) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    builder->Append(arrow::Decimal128(typed_batch->values[i])));
            } else {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->AppendNull());
            }
        }
    }
    return builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedBinaryBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    assert(column_vector_batch->numElements > 0);
    auto typed_batch = dynamic_cast<::orc::StringVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto encoded_batch = dynamic_cast<::orc::EncodedStringVectorBatch*>(typed_batch);
    if (typed_batch->blob.size() != 0) {
        assert(!typed_batch->isEncoded);
        // condition1: orc batch is normal string, return arrow::string
        auto data = std::make_shared<OrcBackedArrowBuffer<char>>(std::move(typed_batch->blob));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> offsets,
                               PrepareOffsetsBufferForString(typed_batch, pool));
        auto builder = std::make_shared<UnPooledBinaryBuilder>(type, pool);
        builder->SetData(data);
        builder->SetOffsets(offsets);
        PAIMON_RETURN_NOT_OK(
            BuilderSetNulls<UnPooledBinaryBuilder>(column_vector_batch, builder.get()));
        return builder;
    } else if (encoded_batch && encoded_batch->dictionary) {
        assert(type->id() == arrow::Type::type::STRING);
        // condition2: orc batch is dict batch, return arrow::dict
        auto builder = std::make_shared<UnPooledStringDictionaryBuilder>(pool);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->SetDictionary(encoded_batch->dictionary));
        auto indices =
            std::make_shared<OrcBackedArrowBuffer<int64_t>>(std::move(encoded_batch->index));
        builder->SetIndices(indices);
        PAIMON_RETURN_NOT_OK(
            BuilderSetNulls<UnPooledStringDictionaryBuilder>(column_vector_batch, builder.get()));
        return builder;
    } else {
        // condition3: disable dict batch in orc, while blob is also invalid, degrade to deep copy
        // version
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> data,
                               DeepCopyDataBufferForString(typed_batch, pool));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> offsets,
                               PrepareOffsetsBufferForString(typed_batch, pool));
        auto builder = std::make_shared<UnPooledBinaryBuilder>(type, pool);
        builder->SetData(data);
        builder->SetOffsets(offsets);
        PAIMON_RETURN_NOT_OK(
            BuilderSetNulls<UnPooledBinaryBuilder>(column_vector_batch, builder.get()));
        return builder;
    }
    return Status::Invalid("convert to arrow array failed, because of invalid string vector batch");
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeArrowBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool);

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedListBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    using OffsetType = arrow::ListType::offset_type;
    auto typed_batch = dynamic_cast<::orc::ListVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto list_type = arrow::internal::checked_cast<arrow::ListType*>(type.get());
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ArrayBuilder> elements_builder,
        MakeArrowBuilder(list_type->value_type(), typed_batch->elements.get(), pool));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Buffer> offsets,
        arrow::AllocateBuffer(sizeof(OffsetType) * (typed_batch->numElements + 1), pool));
    auto* typed_offsets = offsets->mutable_data_as<OffsetType>();
    for (size_t i = 0; i < typed_batch->numElements + 1; i++) {
        typed_offsets[i] = static_cast<OffsetType>(typed_batch->offsets[i]);
    }
    auto list_builder = std::make_shared<UnPooledListBuilder>(type, elements_builder, pool);
    list_builder->SetOffsets(std::move(offsets));
    PAIMON_RETURN_NOT_OK(
        BuilderSetNulls<UnPooledListBuilder>(column_vector_batch, list_builder.get()));
    return list_builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedStructBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    auto typed_batch = dynamic_cast<::orc::StructVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto struct_type = arrow::internal::checked_cast<arrow::StructType*>(type.get());
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> children_builders;
    children_builders.reserve(typed_batch->fields.size());
    for (size_t i = 0; i < typed_batch->fields.size(); i++) {
        PAIMON_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::ArrayBuilder> child_builder,
            MakeArrowBuilder(struct_type->field(i)->type(), typed_batch->fields[i], pool));
        children_builders.push_back(child_builder);
    }
    auto struct_builder = std::make_shared<UnPooledStructBuilder>(type, children_builders, pool);
    PAIMON_RETURN_NOT_OK(
        BuilderSetNulls<UnPooledStructBuilder>(column_vector_batch, struct_builder.get()));
    return struct_builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeOrcBackedMapBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    using OffsetType = arrow::ListType::offset_type;
    auto typed_batch = dynamic_cast<::orc::MapVectorBatch*>(column_vector_batch);
    assert(typed_batch);
    auto map_type = arrow::internal::checked_cast<arrow::MapType*>(type.get());
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayBuilder> key_builder,
                           MakeArrowBuilder(map_type->key_type(), typed_batch->keys.get(), pool));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<arrow::ArrayBuilder> item_builder,
        MakeArrowBuilder(map_type->item_type(), typed_batch->elements.get(), pool));
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
        std::shared_ptr<arrow::Buffer> offsets,
        arrow::AllocateBuffer(sizeof(OffsetType) * (typed_batch->numElements + 1), pool));
    auto* typed_offsets = offsets->mutable_data_as<OffsetType>();
    for (size_t i = 0; i < typed_batch->numElements + 1; i++) {
        typed_offsets[i] = static_cast<OffsetType>(typed_batch->offsets[i]);
    }

    std::vector<std::shared_ptr<arrow::ArrayBuilder>> child_builders = {key_builder, item_builder};
    auto struct_builder =
        std::make_shared<UnPooledStructBuilder>(map_type->value_type(), child_builders, pool);
    // key cannot be null
    struct_builder->IncreaseLength(key_builder->length());

    auto map_builder = std::make_shared<UnPooledMapBuilder>(type, struct_builder, pool);
    map_builder->SetOffsets(std::move(offsets));
    PAIMON_RETURN_NOT_OK(
        BuilderSetNulls<UnPooledMapBuilder>(column_vector_batch, map_builder.get()));
    return map_builder;
}

Result<std::shared_ptr<arrow::ArrayBuilder>> MakeArrowBuilder(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* column_vector_batch,
    arrow::MemoryPool* pool) {
    if (column_vector_batch->numElements == 0) {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::ArrayBuilder> builder,
                                          arrow::MakeBuilder(type, pool));
        return builder;
    }
    arrow::Type::type kind = type->id();
    switch (kind) {
        case arrow::Type::type::BOOL:
            return MakeOrcBackedBooleanBuilder(column_vector_batch, pool);
        case arrow::Type::type::INT8:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::Int8Type>,
                                                 ::orc::ByteVectorBatch, int8_t>(
                type, column_vector_batch, pool);
        case arrow::Type::type::INT16:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::Int16Type>,
                                                 ::orc::ShortVectorBatch, int16_t>(
                type, column_vector_batch, pool);
        case arrow::Type::type::INT32:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::Int32Type>,
                                                 ::orc::IntVectorBatch, int32_t>(
                type, column_vector_batch, pool);

        case arrow::Type::type::INT64:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::Int64Type>,
                                                 ::orc::LongVectorBatch, int64_t>(
                type, column_vector_batch, pool);
        case arrow::Type::type::FLOAT:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::FloatType>,
                                                 ::orc::FloatVectorBatch, float>(
                type, column_vector_batch, pool);
        case arrow::Type::type::DOUBLE:
            return MakeOrcBackedPrimitiveBuilder<UnPooledPrimitiveBuilder<arrow::DoubleType>,
                                                 ::orc::DoubleVectorBatch, double>(
                type, column_vector_batch, pool);
        case arrow::Type::type::BINARY:
        case arrow::Type::type::STRING:
            return MakeOrcBackedBinaryBuilder(type, column_vector_batch, pool);
        case arrow::Type::type::DATE32:
            return MakeOrcBackedDate32Builder(type, column_vector_batch, pool);
        case arrow::Type::type::TIMESTAMP:
            return MakeOrcBackedTimestampBuilder(type, column_vector_batch, pool);
        case arrow::Type::type::DECIMAL128:
            return MakeOrcBackedDecimal128Builder(type, column_vector_batch, pool);
        case arrow::Type::type::STRUCT:
            return MakeOrcBackedStructBuilder(type, column_vector_batch, pool);
        case arrow::Type::type::LIST:
            return MakeOrcBackedListBuilder(type, column_vector_batch, pool);
        case arrow::Type::type::MAP:
            return MakeOrcBackedMapBuilder(type, column_vector_batch, pool);
        default: {
            return Status::NotImplemented("Unknown or unsupported Arrow type: ", type->ToString());
        }
    }
}

}  // namespace

Result<std::shared_ptr<arrow::Array>> OrcAdapter::AppendBatch(
    const std::shared_ptr<arrow::DataType>& type, ::orc::ColumnVectorBatch* batch,
    arrow::MemoryPool* pool) {
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ArrayBuilder> builder,
                           MakeArrowBuilder(type, batch, pool));
    std::shared_ptr<arrow::Array> array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Finish(&array));
    return array;
}

namespace {

arrow::Status WriteBatch(const arrow::Array& array, ::orc::ColumnVectorBatch* column_vector_batch);

// Make sure children of StructArray have appropriate null.
arrow::Result<std::shared_ptr<arrow::Array>> NormalizeArray(
    const std::shared_ptr<arrow::Array>& array) {
    arrow::Type::type kind = array->type_id();
    switch (kind) {
        case arrow::Type::type::STRUCT: {
            auto struct_array = arrow::internal::checked_cast<arrow::StructArray*>(array.get());
            const std::shared_ptr<arrow::Buffer> bitmap = struct_array->null_bitmap();
            std::shared_ptr<arrow::DataType> struct_type = struct_array->type();
            std::size_t size = struct_type->fields().size();
            arrow::ArrayVector new_children(size, nullptr);
            for (std::size_t i = 0; i < size; i++) {
                auto child_length = struct_array->data()->child_data[i]->length;
                auto child_offset = struct_array->data()->child_data[i]->offset;
                // field function will change length & offset in child data
                std::shared_ptr<arrow::Array> child = struct_array->field(static_cast<int>(i));
                const std::shared_ptr<arrow::Buffer> child_bitmap = child->null_bitmap();
                std::shared_ptr<arrow::Buffer> final_child_bitmap;
                if (child_bitmap == nullptr && bitmap == nullptr) {
                    final_child_bitmap = nullptr;
                } else if (child_bitmap == nullptr) {
                    final_child_bitmap = bitmap;
                } else if (bitmap == nullptr) {
                    final_child_bitmap = child_bitmap;
                } else {
                    ARROW_ASSIGN_OR_RAISE(final_child_bitmap,
                                          arrow::internal::BitmapAnd(
                                              arrow::default_memory_pool(), bitmap->data(), 0,
                                              child_bitmap->data(), 0, struct_array->length(), 0));
                }
                std::shared_ptr<arrow::ArrayData> child_array_data = child->data();
                std::vector<std::shared_ptr<arrow::Buffer>> child_buffers =
                    child_array_data->buffers;
                child_buffers[0] = final_child_bitmap;
                // When slicing, we do not know the null count of the sliced range without
                // doing some computation. To avoid doing this eagerly, we set the null count
                // to -1.
                std::shared_ptr<arrow::ArrayData> new_child_array_data = arrow::ArrayData::Make(
                    child->type(), child_length, child_buffers, child_array_data->child_data,
                    child_array_data->dictionary,
                    /*null_count=*/arrow::kUnknownNullCount, child_offset);
                ARROW_ASSIGN_OR_RAISE(new_children[i],
                                      NormalizeArray(arrow::MakeArray(new_child_array_data)));
            }
            return std::make_shared<arrow::StructArray>(
                struct_type, struct_array->length(), new_children, bitmap,
                struct_array->null_count(), struct_array->offset());
        }
        case arrow::Type::type::LIST: {
            auto list_array = arrow::internal::checked_cast<arrow::ListArray*>(array.get());
            ARROW_ASSIGN_OR_RAISE(auto value_array, NormalizeArray(list_array->values()));
            return std::make_shared<arrow::ListArray>(
                list_array->type(), list_array->length(), list_array->value_offsets(), value_array,
                list_array->null_bitmap(), list_array->null_count(), list_array->offset());
        }
        case arrow::Type::type::MAP: {
            auto map_array = arrow::internal::checked_cast<arrow::MapArray*>(array.get());
            ARROW_ASSIGN_OR_RAISE(auto key_array, NormalizeArray(map_array->keys()));
            ARROW_ASSIGN_OR_RAISE(auto item_array, NormalizeArray(map_array->items()));
            return std::make_shared<arrow::MapArray>(
                map_array->type(), map_array->length(), map_array->value_offsets(), key_array,
                item_array, map_array->null_bitmap(), map_array->null_count(), map_array->offset());
        }
        default: {
            return array;
        }
    }
}

template <class DataType, class BatchType, typename Enable = void>
struct Appender {};

// Types for long/double-like Appender, that is, numeric, boolean or date32
template <typename T>
using is_generic_type = std::integral_constant<bool, arrow::is_number_type<T>::value ||
                                                         std::is_same_v<arrow::Date32Type, T> ||
                                                         arrow::is_boolean_type<T>::value>;
template <typename T, typename R = void>
using enable_if_generic = arrow::enable_if_t<is_generic_type<T>::value, R>;

// Number-like
template <class DataType, class BatchType>
struct Appender<DataType, BatchType, enable_if_generic<DataType>> {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    using ValueType = typename arrow::TypeTraits<DataType>::CType;
    arrow::Status VisitNull() {
        batch->notNull[running_orc_offset] = false;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    arrow::Status VisitValue(ValueType v) {
        batch->data[running_orc_offset] = array.Value(running_arrow_offset);
        batch->notNull[running_orc_offset] = true;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    const ArrayType& array;
    BatchType* batch;
    int64_t running_orc_offset, running_arrow_offset;
};

// Binary
template <class DataType>
struct Appender<DataType, ::orc::StringVectorBatch> {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    using COffsetType = typename arrow::TypeTraits<DataType>::OffsetType::c_type;
    arrow::Status VisitNull() {
        batch->notNull[running_orc_offset] = false;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    arrow::Status VisitValue(std::string_view v) {
        batch->notNull[running_orc_offset] = true;
        COffsetType data_length = 0;
        batch->data[running_orc_offset] = reinterpret_cast<char*>(
            const_cast<uint8_t*>(array.GetValue(running_arrow_offset, &data_length)));
        batch->length[running_orc_offset] = data_length;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    const ArrayType& array;
    ::orc::StringVectorBatch* batch;
    int64_t running_orc_offset, running_arrow_offset;
};

// Decimal
template <>
struct Appender<arrow::Decimal128Type, ::orc::Decimal64VectorBatch> {
    arrow::Status VisitNull() {
        batch->notNull[running_orc_offset] = false;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    arrow::Status VisitValue(std::string_view v) {
        batch->notNull[running_orc_offset] = true;
        const arrow::Decimal128 dec_value(array.GetValue(running_arrow_offset));
        batch->values[running_orc_offset] = static_cast<int64_t>(dec_value.low_bits());
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    const arrow::Decimal128Array& array;
    ::orc::Decimal64VectorBatch* batch;
    int64_t running_orc_offset, running_arrow_offset;
};

template <>
struct Appender<arrow::Decimal128Type, ::orc::Decimal128VectorBatch> {
    arrow::Status VisitNull() {
        batch->notNull[running_orc_offset] = false;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    arrow::Status VisitValue(std::string_view v) {
        batch->notNull[running_orc_offset] = true;
        const arrow::Decimal128 dec_value(array.GetValue(running_arrow_offset));
        batch->values[running_orc_offset] =
            ::orc::Int128(dec_value.high_bits(), dec_value.low_bits());
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    const arrow::Decimal128Array& array;
    ::orc::Decimal128VectorBatch* batch;
    int64_t running_orc_offset, running_arrow_offset;
};

// Timestamp
template <class DataType>
struct TimestampAppender {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    arrow::Status VisitNull() {
        batch->notNull[running_orc_offset] = false;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    arrow::Status VisitValue(int64_t v) {
        int64_t data = array.Value(running_arrow_offset);
        batch->notNull[running_orc_offset] = true;
        auto [second, nanosecond] = DateTimeUtils::TimestampConverter(
            data, time_type, DateTimeUtils::SECOND, DateTimeUtils::NANOSECOND);
        batch->data[running_orc_offset] = second;
        batch->nanoseconds[running_orc_offset] = nanosecond;
        running_orc_offset++;
        batch->numElements = running_orc_offset;
        running_arrow_offset++;
        return arrow::Status::OK();
    }
    DateTimeUtils::TimeType time_type = DateTimeUtils::TimeType::MICROSECOND;
    const ArrayType& array;
    ::orc::TimestampVectorBatch* batch;
    int64_t running_orc_offset, running_arrow_offset;
};

template <class DataType, class BatchType>
arrow::Status ShallowCopyGenericBatch(const arrow::Array& array,
                                      ::orc::ColumnVectorBatch* column_vector_batch) {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    using value_type = typename ArrayType::value_type;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    auto batch = arrow::internal::checked_cast<BatchType*>(column_vector_batch);
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    int64_t length = array_.length();
    for (int64_t i = 0; i < length; i++) {
        batch->notNull[i] = array_.IsValid(i) ? static_cast<char>(1) : static_cast<char>(0);
    }
    auto* raw_values = const_cast<value_type*>(array_.raw_values());
    batch->data.setData(raw_values, sizeof(value_type) * length);
    batch->numElements = length;
    return arrow::Status::OK();
}

// static_cast from int64_t or double to itself shouldn't introduce overhead
// Please see
// https://stackoverflow.com/questions/19106826/
// can-static-cast-to-same-type-introduce-runtime-overhead
template <class DataType, class BatchType>
arrow::Status WriteGenericBatch(const arrow::Array& array,
                                ::orc::ColumnVectorBatch* column_vector_batch) {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    auto batch = arrow::internal::checked_cast<BatchType*>(column_vector_batch);
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    Appender<DataType, BatchType> appender{array_, batch, /*orc_offset=*/0, 0};
    arrow::ArraySpanVisitor<DataType> visitor;
    ARROW_RETURN_NOT_OK(visitor.Visit(*array_.data(), &appender));
    return arrow::Status::OK();
}

template <class DataType>
arrow::Status WriteTimestampBatch(const arrow::Array& array,
                                  ::orc::ColumnVectorBatch* column_vector_batch) {
    using ArrayType = typename arrow::TypeTraits<DataType>::ArrayType;
    const auto& array_(arrow::internal::checked_cast<const ArrayType&>(array));
    auto batch = arrow::internal::checked_cast<::orc::TimestampVectorBatch*>(column_vector_batch);
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    auto timestamp_type = arrow::internal::checked_pointer_cast<DataType>(array.type());
    auto time_type = DateTimeUtils::GetTimeTypeFromArrowType(timestamp_type);
    TimestampAppender<DataType> appender{time_type, array_, batch,
                                         /*orc_offset=*/0, 0};
    arrow::ArraySpanVisitor<DataType> visitor;
    ARROW_RETURN_NOT_OK(visitor.Visit(*array_.data(), &appender));
    return arrow::Status::OK();
}

arrow::Status WriteStructBatch(const arrow::Array& array,
                               ::orc::ColumnVectorBatch* column_vector_batch) {
    std::shared_ptr<arrow::Array> array_ = arrow::MakeArray(array.data());
    auto* struct_array = arrow::internal::checked_cast<arrow::StructArray*>(array_.get());
    assert(struct_array);
    auto batch = arrow::internal::checked_cast<::orc::StructVectorBatch*>(column_vector_batch);
    std::size_t size = array.type()->fields().size();
    int64_t arrow_length = array.length();
    batch->numElements = arrow_length;
    int64_t running_arrow_offset = 0, running_orc_offset = 0;
    // First fill fields of ColumnVectorBatch
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    for (; running_arrow_offset < arrow_length; running_orc_offset++, running_arrow_offset++) {
        batch->notNull[running_orc_offset] =
            array.IsValid(running_arrow_offset) ? static_cast<char>(1) : static_cast<char>(0);
    }
    // Fill the fields
    for (std::size_t i = 0; i < size; i++) {
        batch->fields[i]->resize(arrow_length);
        ARROW_RETURN_NOT_OK(
            WriteBatch(*(struct_array->field(static_cast<int>(i))), batch->fields[i]));
    }
    return arrow::Status::OK();
}

template <class ArrayType>
arrow::Status WriteListBatch(const arrow::Array& array,
                             ::orc::ColumnVectorBatch* column_vector_batch) {
    const auto& list_array(arrow::internal::checked_cast<const ArrayType&>(array));
    auto batch = arrow::internal::checked_cast<::orc::ListVectorBatch*>(column_vector_batch);
    ::orc::ColumnVectorBatch* element_batch = (batch->elements).get();
    int64_t arrow_length = array.length();
    batch->numElements = arrow_length;
    int64_t running_arrow_offset = 0, running_orc_offset = 0;
    if (running_orc_offset == 0) {
        batch->offsets[0] = 0;
    }
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    element_batch->resize(list_array.values()->length());
    ARROW_RETURN_NOT_OK(WriteBatch(*list_array.values(), element_batch));
    // TODO(liancheng.lsz): if array large list, can memcpy offsets
    for (; running_arrow_offset < arrow_length; running_orc_offset++, running_arrow_offset++) {
        if (array.IsValid(running_arrow_offset)) {
            batch->notNull[running_orc_offset] = true;
            batch->offsets[running_orc_offset + 1] =
                batch->offsets[running_orc_offset] +
                list_array.value_offset(running_arrow_offset + 1) -
                list_array.value_offset(running_arrow_offset);
        } else {
            batch->notNull[running_orc_offset] = false;
            batch->offsets[running_orc_offset + 1] = batch->offsets[running_orc_offset];
        }
    }
    return arrow::Status::OK();
}

arrow::Status WriteMapBatch(const arrow::Array& array,
                            ::orc::ColumnVectorBatch* column_vector_batch) {
    const auto& map_array(arrow::internal::checked_cast<const arrow::MapArray&>(array));
    auto batch = arrow::internal::checked_cast<::orc::MapVectorBatch*>(column_vector_batch);
    ::orc::ColumnVectorBatch* key_batch = (batch->keys).get();
    ::orc::ColumnVectorBatch* element_batch = (batch->elements).get();
    std::shared_ptr<arrow::Array> key_array = map_array.keys();
    std::shared_ptr<arrow::Array> element_array = map_array.items();
    int64_t arrow_length = array.length();
    batch->numElements = arrow_length;
    int64_t running_arrow_offset = 0, running_orc_offset = 0;
    if (running_orc_offset == 0) {
        batch->offsets[0] = 0;
    }
    if (array.null_count()) {
        batch->hasNulls = true;
    }
    key_batch->resize(key_array->length());
    element_batch->resize(element_array->length());
    ARROW_RETURN_NOT_OK(WriteBatch(*key_array, key_batch));
    ARROW_RETURN_NOT_OK(WriteBatch(*element_array, element_batch));
    for (; running_arrow_offset < arrow_length; running_orc_offset++, running_arrow_offset++) {
        if (array.IsValid(running_arrow_offset)) {
            batch->notNull[running_orc_offset] = true;
            batch->offsets[running_orc_offset + 1] =
                batch->offsets[running_orc_offset] +
                map_array.value_offset(running_arrow_offset + 1) -
                map_array.value_offset(running_arrow_offset);
        } else {
            batch->notNull[running_orc_offset] = false;
            batch->offsets[running_orc_offset + 1] = batch->offsets[running_orc_offset];
        }
    }
    return arrow::Status::OK();
}

arrow::Status WriteBatch(const arrow::Array& array, ::orc::ColumnVectorBatch* column_vector_batch) {
    arrow::Type::type kind = array.type_id();
    column_vector_batch->numElements = 0;
    switch (kind) {
        case arrow::Type::type::BOOL:
            return WriteGenericBatch<arrow::BooleanType, ::orc::ByteVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::INT8:
            return ShallowCopyGenericBatch<arrow::Int8Type, ::orc::ByteVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::INT16:
            return ShallowCopyGenericBatch<arrow::Int16Type, ::orc::ShortVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::INT32:
            return ShallowCopyGenericBatch<arrow::Int32Type, ::orc::IntVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::INT64:
            return ShallowCopyGenericBatch<arrow::Int64Type, ::orc::LongVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::FLOAT:
            return ShallowCopyGenericBatch<arrow::FloatType, ::orc::FloatVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::DOUBLE:
            return ShallowCopyGenericBatch<arrow::DoubleType, ::orc::DoubleVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::BINARY:
            return WriteGenericBatch<arrow::BinaryType, ::orc::StringVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::STRING:
            return WriteGenericBatch<arrow::StringType, ::orc::StringVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::DATE32:
            return WriteGenericBatch<arrow::Date32Type, ::orc::LongVectorBatch>(
                array, column_vector_batch);
        case arrow::Type::type::TIMESTAMP:
            return WriteTimestampBatch<arrow::TimestampType>(array, column_vector_batch);
        case arrow::Type::type::DECIMAL128: {
            int32_t precision =
                arrow::internal::checked_cast<arrow::Decimal128Type*>(array.type().get())
                    ->precision();
            if (precision > 18 || precision == 0) {
                return WriteGenericBatch<arrow::Decimal128Type, ::orc::Decimal128VectorBatch>(
                    array, column_vector_batch);
            } else {
                return WriteGenericBatch<arrow::Decimal128Type, ::orc::Decimal64VectorBatch>(
                    array, column_vector_batch);
            }
        }
        case arrow::Type::type::STRUCT:
            return WriteStructBatch(array, column_vector_batch);
        case arrow::Type::type::LIST:
            return WriteListBatch<arrow::ListArray>(array, column_vector_batch);
        case arrow::Type::type::MAP:
            return WriteMapBatch(array, column_vector_batch);
        default: {
            return arrow::Status::NotImplemented("Unknown or unsupported Arrow type: ",
                                                 array.type()->ToString());
        }
    }
}

void SetAttributes(const std::shared_ptr<arrow::Field>& field, ::orc::Type* type) {
    if (field->HasMetadata()) {
        const auto& metadata = field->metadata();
        for (int64_t i = 0; i < metadata->size(); i++) {
            type->setAttribute(metadata->key(i), metadata->value(i));
        }
    }
}

arrow::Result<std::unique_ptr<::orc::Type>> GetOrcType(const arrow::DataType& type) {
    arrow::Type::type kind = type.id();
    switch (kind) {
        case arrow::Type::type::BOOL:
            return ::orc::createPrimitiveType(::orc::TypeKind::BOOLEAN);
        case arrow::Type::type::INT8:
            return ::orc::createPrimitiveType(::orc::TypeKind::BYTE);
        case arrow::Type::type::INT16:
            return ::orc::createPrimitiveType(::orc::TypeKind::SHORT);
        case arrow::Type::type::INT32:
            return ::orc::createPrimitiveType(::orc::TypeKind::INT);
        case arrow::Type::type::INT64:
            return ::orc::createPrimitiveType(::orc::TypeKind::LONG);
        case arrow::Type::type::FLOAT:
            return ::orc::createPrimitiveType(::orc::TypeKind::FLOAT);
        case arrow::Type::type::DOUBLE:
            return ::orc::createPrimitiveType(::orc::TypeKind::DOUBLE);
        // Use STRING instead of VARCHAR for now, both use UTF-8
        case arrow::Type::type::STRING:
            return ::orc::createPrimitiveType(::orc::TypeKind::STRING);
        case arrow::Type::type::BINARY:
            return ::orc::createPrimitiveType(::orc::TypeKind::BINARY);
        case arrow::Type::type::DATE32:
            return ::orc::createPrimitiveType(::orc::TypeKind::DATE);
        case arrow::Type::type::TIMESTAMP: {
            const auto& timestamp_type =
                arrow::internal::checked_cast<const arrow::TimestampType&>(type);
            if (timestamp_type.timezone().empty()) {
                return ::orc::createPrimitiveType(::orc::TypeKind::TIMESTAMP);
            }
            return ::orc::createPrimitiveType(::orc::TypeKind::TIMESTAMP_INSTANT);
        }
        case arrow::Type::type::DECIMAL128: {
            const auto precision = static_cast<uint64_t>(
                arrow::internal::checked_cast<const arrow::Decimal128Type&>(type).precision());
            const auto scale = static_cast<uint64_t>(
                arrow::internal::checked_cast<const arrow::Decimal128Type&>(type).scale());
            return ::orc::createDecimalType(precision, scale);
        }
        case arrow::Type::type::LIST: {
            const auto& value_field =
                arrow::internal::checked_cast<const arrow::BaseListType&>(type).value_field();
            ARROW_ASSIGN_OR_RAISE(auto orc_subtype, paimon::orc::GetOrcType(*value_field->type()));
            SetAttributes(value_field, orc_subtype.get());
            return ::orc::createListType(std::move(orc_subtype));
        }
        case arrow::Type::type::STRUCT: {
            std::unique_ptr<::orc::Type> out_type = ::orc::createStructType();
            arrow::FieldVector arrow_fields =
                arrow::internal::checked_cast<const arrow::StructType&>(type).fields();
            for (auto& arrow_field : arrow_fields) {
                std::string field_name = arrow_field->name();
                ARROW_ASSIGN_OR_RAISE(auto orc_subtype, GetOrcType(*arrow_field->type()));
                SetAttributes(arrow_field, orc_subtype.get());
                out_type->addStructField(field_name, std::move(orc_subtype));
            }
            return out_type;
        }
        case arrow::Type::type::MAP: {
            const auto& key_field =
                arrow::internal::checked_cast<const arrow::MapType&>(type).key_field();
            const auto& item_field =
                arrow::internal::checked_cast<const arrow::MapType&>(type).item_field();
            ARROW_ASSIGN_OR_RAISE(auto key_orc_type, GetOrcType(*key_field->type()));
            ARROW_ASSIGN_OR_RAISE(auto item_orc_type, GetOrcType(*item_field->type()));
            SetAttributes(key_field, key_orc_type.get());
            SetAttributes(item_field, item_orc_type.get());
            return ::orc::createMapType(std::move(key_orc_type), std::move(item_orc_type));
        }
        default: {
            return arrow::Status::NotImplemented("Unknown or unsupported Arrow type: ",
                                                 type.ToString());
        }
    }
}
}  // namespace

Status OrcAdapter::WriteBatch(const std::shared_ptr<arrow::Array>& array,
                              ::orc::ColumnVectorBatch* column_vector_batch) {
    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> normal_array,
                                      NormalizeArray(array));
    if (static_cast<uint64_t>(normal_array->length()) > column_vector_batch->capacity) {
        return Status::Invalid(
            fmt::format("need to copy {} rows of arrow array, while orc batch has only {} capacity",
                        normal_array->length(), column_vector_batch->capacity));
    }
    uint64_t num_written_elements = normal_array->length();
    if (num_written_elements > 0) {
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            paimon::orc::WriteBatch(*normal_array, column_vector_batch));
    }
    column_vector_batch->numElements = num_written_elements;
    return Status::OK();
}

Result<std::shared_ptr<arrow::DataType>> OrcAdapter::GetArrowType(const ::orc::Type* type) {
    // When subselecting fields on read, orc will set some nodes to nullptr,
    // so we need to check for nullptr before progressing
    if (type == nullptr) {
        return arrow::null();
    }
    ::orc::TypeKind kind = type->getKind();
    const int subtype_count = static_cast<int>(type->getSubtypeCount());

    switch (kind) {
        case ::orc::BOOLEAN:
            return arrow::boolean();
        case ::orc::BYTE:
            return arrow::int8();
        case ::orc::SHORT:
            return arrow::int16();
        case ::orc::INT:
            return arrow::int32();
        case ::orc::LONG:
            return arrow::int64();
        case ::orc::FLOAT:
            return arrow::float32();
        case ::orc::DOUBLE:
            return arrow::float64();
        case ::orc::CHAR:
        case ::orc::VARCHAR:
        case ::orc::STRING:
            return arrow::utf8();
        case ::orc::BINARY:
            return arrow::binary();
        case ::orc::TIMESTAMP:
            // Values of TIMESTAMP type are stored in the writer timezone in the Orc file.
            // Values are read back in the reader timezone. However, the writer timezone
            // information in the Orc stripe footer is optional and may be missing. What is
            // more, stripes in the same Orc file may have different writer timezones (though
            // unlikely). So we cannot tell the exact timezone of values read back in the
            // arrow::TimestampArray. In the adapter implementations, we set both writer and
            // reader timezone to UTC to avoid any conversion so users can get the same values
            // as written. To get rid of this burden, TIMESTAMP_INSTANT type is always preferred
            // over TIMESTAMP type.
            // for timestamp type, precision info is missing from orc type
            return timestamp(arrow::TimeUnit::NANO);
        case ::orc::TIMESTAMP_INSTANT: {
            auto timezone = DateTimeUtils::GetLocalTimezoneName();
            return timestamp(arrow::TimeUnit::NANO, timezone);
        }
        case ::orc::DATE:
            return arrow::date32();
        case ::orc::DECIMAL: {
            const int precision = static_cast<int>(type->getPrecision());
            const int scale = static_cast<int>(type->getScale());
            return arrow::decimal128(precision, scale);
        }
        case ::orc::LIST: {
            if (subtype_count != 1) {
                return Status::TypeError("Invalid Orc List type");
            }
            PAIMON_ASSIGN_OR_RAISE(auto elem_field, GetArrowField("item", type->getSubtype(0)));
            return list(std::move(elem_field));
        }
        case ::orc::MAP: {
            if (subtype_count != 2) {
                return Status::TypeError("Invalid Orc Map type");
            }
            PAIMON_ASSIGN_OR_RAISE(auto key_field,
                                   GetArrowField("key", type->getSubtype(0), /*nullable=*/false));
            PAIMON_ASSIGN_OR_RAISE(auto value_field, GetArrowField("value", type->getSubtype(1)));
            return std::make_shared<arrow::MapType>(std::move(key_field), std::move(value_field));
        }
        case ::orc::STRUCT: {
            arrow::FieldVector fields(subtype_count);
            for (int child = 0; child < subtype_count; ++child) {
                const auto& name = type->getFieldName(child);
                PAIMON_ASSIGN_OR_RAISE(auto elem_field,
                                       GetArrowField(name, type->getSubtype(child)));
                fields[child] = std::move(elem_field);
            }
            return arrow::struct_(std::move(fields));
        }
        default:
            return Status::TypeError("Unknown Orc type kind: ", type->toString());
    }
}

Result<std::unique_ptr<::orc::Type>> OrcAdapter::GetOrcType(const arrow::Schema& schema) {
    int32_t numFields = schema.num_fields();
    std::unique_ptr<::orc::Type> out_type = ::orc::createStructType();
    for (int32_t i = 0; i < numFields; i++) {
        const auto& field = schema.field(i);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::unique_ptr<::orc::Type> orc_subtype,
                                          paimon::orc::GetOrcType(*field->type()));
        SetAttributes(field, orc_subtype.get());
        out_type->addStructField(field->name(), std::move(orc_subtype));
    }
    return out_type;
}

Result<std::shared_ptr<const arrow::KeyValueMetadata>> OrcAdapter::GetFieldMetadata(
    const ::orc::Type* type) {
    if (type == nullptr) {
        return std::shared_ptr<const arrow::KeyValueMetadata>();
    }
    const auto keys = type->getAttributeKeys();
    if (keys.empty()) {
        return std::shared_ptr<const arrow::KeyValueMetadata>();
    }
    auto metadata = std::make_shared<arrow::KeyValueMetadata>();
    for (const auto& key : keys) {
        metadata->Append(key, type->getAttributeValue(key));
    }
    return std::const_pointer_cast<const arrow::KeyValueMetadata>(metadata);
}

Result<std::shared_ptr<arrow::Field>> OrcAdapter::GetArrowField(const std::string& name,
                                                                const ::orc::Type* type,
                                                                bool nullable) {
    PAIMON_ASSIGN_OR_RAISE(auto arrow_type, GetArrowType(type));
    PAIMON_ASSIGN_OR_RAISE(auto metadata, GetFieldMetadata(type));
    return field(name, std::move(arrow_type), nullable, std::move(metadata));
}

}  // namespace paimon::orc
