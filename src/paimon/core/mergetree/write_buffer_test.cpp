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
#include "paimon/testing/utils/test_helper.h"
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
                                options, io_manager_, /*enable_multi_thread_spill=*/false, pool_));
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

    Result<int64_t> GetOnlySpillFileSize() const {
        PAIMON_ASSIGN_OR_RAISE(std::string spill_dir, io_manager_->GetSpillDir());
        std::vector<std::unique_ptr<FileStatus>> spill_files;
        PAIMON_RETURN_NOT_OK(tmp_dir_->GetFileSystem()->ListFileStatus(spill_dir, &spill_files));
        if (spill_files.size() != 1 || spill_files[0]->IsDir()) {
            return Status::Invalid("expected exactly one spill file");
        }
        return static_cast<int64_t>(spill_files[0]->GetLen());
    }

    Result<ReaderResult> ReadReaderResult(KeyValueRecordReader* reader) const {
        PAIMON_ASSIGN_OR_RAISE(auto iterator, reader->NextBatch());

        ReaderResult result;
        while (true) {
            PAIMON_ASSIGN_OR_RAISE(bool has_next, iterator->HasNext());
            if (!has_next) {
                break;
            }
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

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array1, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                         write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);
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

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array, row_kinds)));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_EQ(readers.size(), 1);

    ASSERT_OK_AND_ASSIGN(auto reader_result, ReadReaderResult(readers[0].get()));

    ASSERT_EQ(reader_result.row_kind_values,
              (std::vector<int8_t>{
                  RowKind::Insert()->ToByteValue(), RowKind::UpdateBefore()->ToByteValue(),
                  RowKind::UpdateAfter()->ToByteValue(), RowKind::Delete()->ToByteValue()}));
    ASSERT_EQ(reader_result.sequence_numbers, (std::vector<int64_t>{0, 1, 2, 3}));
}

TEST_F(WriteBufferTest, TestWriteRequestsFlushWriteBufferWhenSpillDisabled) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "false"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_memory,
                         write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_FALSE(has_remaining_memory);
    ASSERT_FALSE(write_buffer->IsEmpty());
    ASSERT_GT(write_buffer->GetMemoryUsage(), 0);
}

TEST_F(WriteBufferTest, TestSpillDiskQuotaEnforcement) {
    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
      ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(CoreOptions ref_options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));
    auto ref_write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, ref_options);
    ASSERT_OK(ref_write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_OK_AND_ASSIGN(int64_t spill_file_size, GetOnlySpillFileSize());
    ref_write_buffer->Clear();

    // Case 1: FlushMemory consumes remaining disk quota exactly → returns false.
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "true"},
                                                   {Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE,
                                                    std::to_string(spill_file_size)}}));
        auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

        ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                             write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);
        ASSERT_OK_AND_ASSIGN(bool has_remaining_disk, write_buffer->FlushMemory());
        ASSERT_FALSE(has_remaining_disk);
        // write_buffer is not empty because spilled data on disk still belongs to the buffer.
        ASSERT_FALSE(write_buffer->IsEmpty());
        ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);
    }

    // Case 2: Write auto-spill consumes remaining disk quota exactly → returns false.
    {
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                                   {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                                   {Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE,
                                                    std::to_string(spill_file_size)}}));
        auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

        ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                             write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
        ASSERT_FALSE(has_remaining_quota);
        // write_buffer is not empty because spilled data on disk still belongs to the buffer.
        ASSERT_FALSE(write_buffer->IsEmpty());
        ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);
    }

    // Case 3: Multiple spills exhaust disk quota (quota = 2 spill files).
    {
        int64_t quota_for_two_files = spill_file_size * 2;
        ASSERT_OK_AND_ASSIGN(CoreOptions options,
                             CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                                   {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                                   {Options::WRITE_BUFFER_SPILL_MAX_DISK_SIZE,
                                                    std::to_string(quota_for_two_files)}}));
        auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

        std::shared_ptr<arrow::Array> array2 =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
            ["Bob", 20, 1, 14.1]
        ])")
                .ValueOrDie();

        // Write 1: under disk quota → true.
        ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                             write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);

        // Write 2: WRITE_BUFFER_SIZE=1 causes UpdateSpillParameters() to clamp actual_max_fan_in_
        // to 2, triggering intermediate merge which reduces total_spill_disk_bytes_ below disk
        // quota. So quota is NOT exhausted here.
        ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                             write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);

        // Write 3: spill adds a new file to level 0, but no merge is triggered (each level has
        // fewer than fan_in files), so total disk usage exceeds quota → returns false.
        ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                             write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
        ASSERT_FALSE(has_remaining_quota);

        ASSERT_FALSE(write_buffer->IsEmpty());
        ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
        std::vector<int64_t> all_sequence_numbers;
        for (auto& reader : readers) {
            ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
            all_sequence_numbers.insert(all_sequence_numbers.end(), result.sequence_numbers.begin(),
                                        result.sequence_numbers.end());
        }
        std::sort(all_sequence_numbers.begin(), all_sequence_numbers.end());
        ASSERT_EQ(all_sequence_numbers, (std::vector<int64_t>{0, 2}));
    }
}

TEST_F(WriteBufferTest, TestCreateReadersMergesSingleInMemoryReaderLocally) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SPILLABLE, "false"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1],
        ["Alice", 20, 1, 14.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_memory,
                         write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_memory);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_EQ(readers.size(), 1);

    ASSERT_OK_AND_ASSIGN(auto reader_result, ReadReaderResult(readers[0].get()));
    ASSERT_EQ(reader_result.sequence_numbers, (std::vector<int64_t>{1}));
    ASSERT_EQ(reader_result.row_kind_values,
              (std::vector<int8_t>{RowKind::Insert()->ToByteValue()}));
}

TEST_F(WriteBufferTest, TestCreateReadersReturnsBothSpillAndMemoryReaders) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));

    // Case 1: Same key in spill and memory — each reader returns its own data independently.
    {
        auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

        std::shared_ptr<arrow::Array> spill_array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
            ["Alice", 10, 0, 13.1]
        ])")
                .ValueOrDie();
        std::shared_ptr<arrow::Array> memory_array =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
            ["Alice", 20, 1, 14.1]
        ])")
                .ValueOrDie();

        ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                             write_buffer->Write(CreateBatch(spill_array, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);
        ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
        ASSERT_TRUE(has_remaining_quota);

        ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                             write_buffer->Write(CreateBatch(memory_array, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);

        ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
        ASSERT_EQ(readers.size(), 2);

        ASSERT_OK_AND_ASSIGN(auto first_result, ReadReaderResult(readers[0].get()));
        ASSERT_EQ(first_result.sequence_numbers, (std::vector<int64_t>{0}));

        ASSERT_OK_AND_ASSIGN(auto second_result, ReadReaderResult(readers[1].get()));
        ASSERT_EQ(second_result.sequence_numbers, (std::vector<int64_t>{1}));
    }

    // Case 2: Multiple rows in spill and memory — readers cover all data, Clear cleans up files.
    {
        auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

        std::shared_ptr<arrow::Array> array1 =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
            ["Alice", 10, 0, 13.1],
            ["Bob", 20, 1, 14.1],
            ["Charlie", 30, 2, 15.1]
        ])")
                .ValueOrDie();
        std::shared_ptr<arrow::Array> array2 =
            arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
            ["Diana", 40, 3, 16.1],
            ["Eve", 50, 4, 17.1],
            ["Frank", 60, 5, 18.1]
        ])")
                .ValueOrDie();

        ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                             write_buffer->Write(CreateBatch(array1, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);

        ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
        ASSERT_TRUE(has_remaining_quota);
        ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);
        ASSERT_FALSE(write_buffer->IsEmpty());

        ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                             write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
        ASSERT_TRUE(has_remaining_quota);
        ASSERT_GT(write_buffer->GetMemoryUsage(), 0);

        ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
        ASSERT_GE(readers.size(), 2);

        std::vector<int64_t> all_sequence_numbers;
        for (auto& reader : readers) {
            ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
            all_sequence_numbers.insert(all_sequence_numbers.end(), result.sequence_numbers.begin(),
                                        result.sequence_numbers.end());
        }
        std::sort(all_sequence_numbers.begin(), all_sequence_numbers.end());
        ASSERT_EQ(all_sequence_numbers, (std::vector<int64_t>{0, 1, 2, 3, 4, 5}));

        // Verify Clear deletes spill files and resets state.
        write_buffer->Clear();
        ASSERT_TRUE(write_buffer->IsEmpty());
        ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);
        ASSERT_EQ(TestHelper::CountChannelFiles(tmp_dir_->GetFileSystem(), tmp_dir_->Str()), 0);
    }
}

TEST_F(WriteBufferTest, TestSpillReaderReturnsDataInSortedOrder) {
    // Write rows in reverse key order; each Write triggers a spill (SIZE=1).
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                               {Options::LOCAL_SORT_MAX_NUM_FILE_HANDLES, "128"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Charlie", 30, 2, 15.1]
    ])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Bob", 20, 1, 14.1]
    ])")
            .ValueOrDie();
    std::shared_ptr<arrow::Array> array3 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array1, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                         write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                         write_buffer->Write(CreateBatch(array3, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());

    std::vector<int64_t> all_sequence_numbers;
    for (auto& reader : readers) {
        ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
        all_sequence_numbers.insert(all_sequence_numbers.end(), result.sequence_numbers.begin(),
                                    result.sequence_numbers.end());
    }
    std::sort(all_sequence_numbers.begin(), all_sequence_numbers.end());
    ASSERT_EQ(all_sequence_numbers, (std::vector<int64_t>{0, 1, 2}));

    // Also test sorted order within a single spill file with multiple rows.
    ASSERT_OK_AND_ASSIGN(CoreOptions options2,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));
    auto write_buffer2 = CreateWriteBuffer(/*last_sequence_number=*/-1, options2);

    std::shared_ptr<arrow::Array> multi_row_array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Charlie", 30, 2, 15.1],
        ["Alice", 10, 0, 13.1],
        ["Bob", 20, 1, 14.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                         write_buffer2->Write(CreateBatch(multi_row_array, {})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer2->FlushMemory());
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(auto readers2, write_buffer2->CreateReaders());
    ASSERT_EQ(readers2.size(), 1);

    ASSERT_OK_AND_ASSIGN(auto result2, ReadReaderResult(readers2[0].get()));
    ASSERT_EQ(result2.sequence_numbers.size(), 3);
    // Sorted by key: Alice(seq=1), Bob(seq=2), Charlie(seq=0).
    ASSERT_EQ(result2.sequence_numbers, (std::vector<int64_t>{1, 2, 0}));
}

TEST_F(WriteBufferTest, TestEmptyBufferBehavior) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    ASSERT_TRUE(write_buffer->IsEmpty());
    ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);
    ASSERT_OK_AND_ASSIGN(bool has_remaining_disk, write_buffer->FlushMemory());
    ASSERT_TRUE(has_remaining_disk);
    ASSERT_TRUE(write_buffer->IsEmpty());
    ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);

    ASSERT_EQ(TestHelper::CountChannelFiles(tmp_dir_->GetFileSystem(), tmp_dir_->Str()), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_TRUE(readers.empty());
}

TEST_F(WriteBufferTest, TestMergeSpilledFilesSkipsWithSingleFile) {
    // HANDLES=2: with only 1 spill file, merge is not triggered.
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                               {Options::LOCAL_SORT_MAX_NUM_FILE_HANDLES, "2"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_FALSE(write_buffer->IsEmpty());
    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_FALSE(readers.empty());

    std::vector<int64_t> all_sequence_numbers;
    for (auto& reader : readers) {
        ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
        all_sequence_numbers.insert(all_sequence_numbers.end(), result.sequence_numbers.begin(),
                                    result.sequence_numbers.end());
    }
    ASSERT_EQ(all_sequence_numbers, (std::vector<int64_t>{0}));
}

TEST_F(WriteBufferTest, TestMultipleFlushWriteCyclesWorkCorrectly) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));

    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1]
    ])")
            .ValueOrDie();
    // Cycle 1: Write → Flush → Read → Clear.
    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array1, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(auto readers1, write_buffer->CreateReaders());
    ASSERT_FALSE(readers1.empty());
    std::vector<int64_t> seq1;
    for (auto& reader : readers1) {
        ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
        seq1.insert(seq1.end(), result.sequence_numbers.begin(), result.sequence_numbers.end());
    }
    ASSERT_EQ(seq1, (std::vector<int64_t>{0}));

    write_buffer->Clear();
    ASSERT_TRUE(write_buffer->IsEmpty());
    ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);

    // Cycle 2: Write after Clear
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Bob", 20, 1, 14.1],
        ["Charlie", 30, 2, 15.1]
    ])")
            .ValueOrDie();
    ASSERT_OK_AND_ASSIGN(has_remaining_quota,
                         write_buffer->Write(CreateBatch(array2, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(auto readers2, write_buffer->CreateReaders());
    ASSERT_FALSE(readers2.empty());
    std::vector<int64_t> seq2;
    for (auto& reader : readers2) {
        ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(reader.get()));
        seq2.insert(seq2.end(), result.sequence_numbers.begin(), result.sequence_numbers.end());
    }
    std::sort(seq2.begin(), seq2.end());
    ASSERT_EQ(seq2, (std::vector<int64_t>{1, 2}));

    write_buffer->Clear();
    ASSERT_TRUE(write_buffer->IsEmpty());
}

TEST_F(WriteBufferTest, TestMergeSpilledFilesDeduplicationAndRowKinds) {
    // SIZE=1: every Write triggers spill. HANDLES=2: merge after every 2 spill files.
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "1"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"},
                                               {Options::LOCAL_SORT_MAX_NUM_FILE_HANDLES, "2"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array1 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1],
        ["Charlie", 30, 2, 15.1]
    ])")
            .ValueOrDie();
    std::vector<RecordBatch::RowKind> row_kinds1 = {
        RecordBatch::RowKind::INSERT,
        RecordBatch::RowKind::UPDATE_AFTER,
    };
    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array1, row_kinds1)));
    ASSERT_TRUE(has_remaining_quota);

    // Bob(DELETE), Alice(UPDATE_AFTER) — cross-file overlap on Alice.
    std::shared_ptr<arrow::Array> array2 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Bob", 20, 1, 14.1],
        ["Alice", 40, 3, 16.1]
    ])")
            .ValueOrDie();
    std::vector<RecordBatch::RowKind> row_kinds2 = {
        RecordBatch::RowKind::DELETE,
        RecordBatch::RowKind::UPDATE_AFTER,
    };
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->Write(CreateBatch(array2, row_kinds2)));
    ASSERT_TRUE(has_remaining_quota);

    // Diana(INSERT), Bob(INSERT) — overlap on Bob.
    std::shared_ptr<arrow::Array> array3 =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Diana", 50, 4, 17.1],
        ["Bob", 60, 5, 18.1]
    ])")
            .ValueOrDie();
    std::vector<RecordBatch::RowKind> row_kinds3 = {
        RecordBatch::RowKind::INSERT,
        RecordBatch::RowKind::INSERT,
    };
    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->Write(CreateBatch(array3, row_kinds3)));
    ASSERT_TRUE(has_remaining_quota);

    // After dedup: Alice keeps seq=3, Bob keeps seq=5, Charlie seq=1, Diana seq=4.
    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());

    ASSERT_EQ(readers.size(), 1);
    ASSERT_OK_AND_ASSIGN(auto result, ReadReaderResult(readers[0].get()));

    ASSERT_EQ(result.sequence_numbers.size(), 4);
    ASSERT_EQ(result.sequence_numbers, (std::vector<int64_t>{3, 5, 1, 4}));
    ASSERT_EQ(result.row_kind_values,
              (std::vector<int8_t>{
                  RowKind::UpdateAfter()->ToByteValue(), RowKind::Insert()->ToByteValue(),
                  RowKind::UpdateAfter()->ToByteValue(), RowKind::Insert()->ToByteValue()}));
}

TEST_F(WriteBufferTest, TestSpillPreservesNullValues) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, null],
        ["Bob", 20, 1, 14.1],
        ["Charlie", 30, 2, null]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
    ASSERT_TRUE(has_remaining_quota);
    ASSERT_EQ(write_buffer->GetMemoryUsage(), 0);

    ASSERT_OK_AND_ASSIGN(auto readers, write_buffer->CreateReaders());
    ASSERT_EQ(readers.size(), 1);
    auto& reader = readers[0];
    ASSERT_OK_AND_ASSIGN(auto iterator, reader->NextBatch());

    std::vector<int64_t> sequence_numbers;
    std::vector<bool> f3_is_null;
    std::vector<double> f3_values;

    while (true) {
        ASSERT_OK_AND_ASSIGN(bool has_next, iterator->HasNext());
        if (!has_next) break;
        ASSERT_OK_AND_ASSIGN(KeyValue key_value, iterator->Next());
        sequence_numbers.push_back(key_value.sequence_number);
        bool is_null = key_value.value->IsNullAt(3);
        f3_is_null.push_back(is_null);
        if (!is_null) {
            f3_values.push_back(key_value.value->GetDouble(3));
        }
    }

    ASSERT_EQ(sequence_numbers, (std::vector<int64_t>{0, 1, 2}));
    ASSERT_EQ(f3_is_null, (std::vector<bool>{true, false, true}));
    ASSERT_EQ(f3_values.size(), 1);
    ASSERT_DOUBLE_EQ(f3_values[0], 14.1);
}

TEST_F(WriteBufferTest, TestDestructorCleansUpSpillFiles) {
    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({{Options::WRITE_BUFFER_SIZE, "4096000"},
                                               {Options::WRITE_BUFFER_SPILLABLE, "true"}}));
    auto write_buffer = CreateWriteBuffer(/*last_sequence_number=*/-1, options);

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(value_type_, R"([
        ["Alice", 10, 0, 13.1],
        ["Bob", 20, 1, 14.1]
    ])")
            .ValueOrDie();

    ASSERT_OK_AND_ASSIGN(bool has_remaining_quota,
                         write_buffer->Write(CreateBatch(array, /*row_kinds=*/{})));
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_OK_AND_ASSIGN(has_remaining_quota, write_buffer->FlushMemory());
    ASSERT_TRUE(has_remaining_quota);

    ASSERT_EQ(TestHelper::CountChannelFiles(tmp_dir_->GetFileSystem(), tmp_dir_->Str()), 1);

    // Destroy without calling Clear() — destructor should clean up spill files.
    write_buffer.reset();

    ASSERT_EQ(TestHelper::CountChannelFiles(tmp_dir_->GetFileSystem(), tmp_dir_->Str()), 0);
}

}  // namespace paimon::test
