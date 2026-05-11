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

#include "paimon/core/mergetree/write_buffer.h"

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/utils/fields_comparator.h"
#include "paimon/core/core_options.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/io/key_value_record_reader.h"
#include "paimon/core/mergetree/compact/deduplicate_merge_function.h"
#include "paimon/core/mergetree/compact/reducer_merge_function_wrapper.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon {
template <typename T>
class MergeFunctionWrapper;
}  // namespace paimon

namespace paimon::test {
struct ReaderResult {
    std::vector<int64_t> sequence_numbers;
    std::vector<int8_t> row_kind_values;
};

class WriteBufferTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        tmp_dir_ = UniqueTestDirectory::Create();
        ASSERT_TRUE(tmp_dir_);
        io_manager_ = std::make_shared<IOManager>(tmp_dir_->Str(), tmp_dir_->GetFileSystem());
        value_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                         DataField(1, arrow::field("f1", arrow::int32())),
                         DataField(2, arrow::field("f2", arrow::int32())),
                         DataField(3, arrow::field("f3", arrow::float64()))};
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(value_fields_);
        value_type_ = DataField::ConvertDataFieldsToArrowStructType(value_fields_);
        primary_keys_ = {"f0"};
        ASSERT_OK_AND_ASSIGN(key_comparator_,
                             FieldsComparator::Create({value_fields_[0]},
                                                      /*is_ascending_order=*/true));

        auto merge_function = std::make_unique<DeduplicateMergeFunction>(/*ignore_delete=*/false);
        merge_function_wrapper_ =
            std::make_shared<ReducerMergeFunctionWrapper>(std::move(merge_function));
    }

    std::unique_ptr<WriteBuffer> CreateWriteBuffer(int64_t last_sequence_number,
                                                   const CoreOptions& options) const {
        EXPECT_OK_AND_ASSIGN(
            auto write_buffer,
            WriteBuffer::Create(last_sequence_number, value_schema_, primary_keys_,
                                /*user_defined_sequence_fields=*/{}, key_comparator_,
                                /*user_defined_seq_comparator=*/nullptr, merge_function_wrapper_,
                                options, io_manager_, pool_));
        return write_buffer;
    }

    std::unique_ptr<RecordBatch> CreateBatch(
        const std::shared_ptr<arrow::Array>& array,
        const std::vector<RecordBatch::RowKind>& row_kinds) const {
        ::ArrowArray c_array;
        EXPECT_TRUE(arrow::ExportArray(*array, &c_array).ok());
        RecordBatchBuilder batch_builder(&c_array);
        batch_builder.SetRowKinds(row_kinds);
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
        return batch;
    }

    Result<ReaderResult> ReadReaderResult(KeyValueRecordReader* reader) const {
        PAIMON_ASSIGN_OR_RAISE(auto iterator, reader->NextBatch());

        ReaderResult result;
        while (iterator->HasNext()) {
            PAIMON_ASSIGN_OR_RAISE(KeyValue key_value, iterator->Next());
            result.sequence_numbers.push_back(key_value.sequence_number);
            result.row_kind_values.push_back(key_value.value_kind->ToByteValue());
        }
        return result;
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<UniqueTestDirectory> tmp_dir_;
    std::shared_ptr<IOManager> io_manager_;
    std::vector<DataField> value_fields_;
    std::shared_ptr<arrow::Schema> value_schema_;
    std::shared_ptr<arrow::DataType> value_type_;
    std::vector<std::string> primary_keys_;
    std::shared_ptr<FieldsComparator> key_comparator_;
    std::shared_ptr<MergeFunctionWrapper<KeyValue>> merge_function_wrapper_;
};

TEST_F(WriteBufferTest, TestFlushResetsStateAndAdvancesSequenceNumber) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(/*options_map=*/{}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/9, options);

    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1],
      ["Bob", 20, 1, 14.1]
    ])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Charlie", 30, 2, 15.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool buffered1,
                         write_buffer->Write(CreateBatch(array1, /*row_kinds=*/{})));
    ASSERT_TRUE(buffered1);
    ASSERT_OK_AND_ASSIGN(bool buffered2,
                         write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
    ASSERT_TRUE(buffered2);
    ASSERT_FALSE(write_buffer->IsEmpty());
    ASSERT_GT(write_buffer->GetMemoryUsage(), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());

    ASSERT_EQ(readers.size(), 2);
    write_buffer->Clear();
    ASSERT_TRUE(write_buffer->IsEmpty());
    ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);

    ASSERT_OK_AND_ASSIGN(auto first_result, ReadReaderResult(readers[0].get()));
    ASSERT_EQ(first_result.sequence_numbers, (std::vector<int64_t>{10, 11}));
    ASSERT_EQ(
        first_result.row_kind_values,
        (std::vector<int8_t>{RowKind::Insert()->ToByteValue(), RowKind::Insert()->ToByteValue()}));

    ASSERT_OK_AND_ASSIGN(auto second_result, ReadReaderResult(readers[1].get()));
    ASSERT_EQ(second_result.sequence_numbers, (std::vector<int64_t>{12}));
    ASSERT_EQ(second_result.row_kind_values,
              (std::vector<int8_t>{RowKind::Insert()->ToByteValue()}));
}

TEST_F(WriteBufferTest, TestFlushPreservesRowKinds) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(/*options_map=*/{}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1],
      ["Bob", 20, 1, 14.1],
      ["Charlie", 30, 2, 15.1],
      ["Diana", 40, 3, 16.1]
    ])")
            .ValueOrDie();
    std::vector<RecordBatch::RowKind> row_kinds = {
        RecordBatch::RowKind::INSERT,
        RecordBatch::RowKind::UPDATE_BEFORE,
        RecordBatch::RowKind::UPDATE_AFTER,
        RecordBatch::RowKind::DELETE,
    };

    ASSERT_OK_AND_ASSIGN(bool buffered, write_buffer->Write(CreateBatch(array, row_kinds)));
    ASSERT_TRUE(buffered);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_EQ(readers.size(), 1);

    ASSERT_OK_AND_ASSIGN(auto reader_result, ReadReaderResult(readers[0].get()));

    ASSERT_EQ(reader_result.row_kind_values,
              (std::vector<int8_t>{
                  RowKind::Insert()->ToByteValue(), RowKind::UpdateBefore()->ToByteValue(),
                  RowKind::UpdateAfter()->ToByteValue(), RowKind::Delete()->ToByteValue()}));
    ASSERT_EQ(reader_result.sequence_numbers, (std::vector<int64_t>{0, 1, 2, 3}));
}

}  // namespace paimon::test
