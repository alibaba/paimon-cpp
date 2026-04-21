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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/disk/io_manager.h"
#include "paimon/core/mergetree/spill_channel_manager.h"
#include "paimon/core/mergetree/spill_reader.h"
#include "paimon/core/mergetree/spill_writer.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SpillReaderWriterTest : public ::testing::TestWithParam<std::string> {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        test_dir_ = UniqueTestDirectory::Create();
        file_system_ = test_dir_->GetFileSystem();

        io_manager_ = std::make_unique<IOManager>(test_dir_->Str(), test_dir_->GetFileSystem());
        ASSERT_OK_AND_ASSIGN(channel_enumerator_, io_manager_->CreateChannelEnumerator());
        spill_channel_manager_ = std::make_shared<SpillChannelManager>(file_system_, 128);

        // Build write schema: [_SEQUENCE_NUMBER, _VALUE_KIND, key fields..., value fields...]
        value_fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                         DataField(1, arrow::field("f1", arrow::int32()))};
        key_fields_ = {DataField(0, arrow::field("f0", arrow::utf8()))};

        key_schema_ = DataField::ConvertDataFieldsToArrowSchema(key_fields_);
        value_schema_ = DataField::ConvertDataFieldsToArrowSchema(value_fields_);
        write_schema_ = SpecialFields::CompleteSequenceAndValueKindField(value_schema_);
        write_type_ = arrow::struct_(write_schema_->fields());
    }

    std::shared_ptr<arrow::RecordBatch> CreateRecordBatch(const std::string& json_data,
                                                          int64_t num_rows) const {
        auto array = arrow::ipc::internal::json::ArrayFromJSON(write_type_, json_data).ValueOrDie();
        auto struct_array = std::dynamic_pointer_cast<arrow::StructArray>(array);
        return arrow::RecordBatch::Make(write_schema_, num_rows, struct_array->fields());
    }

    Result<std::unique_ptr<SpillWriter>> CreateSpillWriter() const {
        return SpillWriter::Create(file_system_, write_schema_, channel_enumerator_,
                                   spill_channel_manager_, GetParam(), /*compression_level=*/1);
    }

    FileIOChannel::ID WriteSpillFile(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches) {
        EXPECT_OK_AND_ASSIGN(auto writer, CreateSpillWriter());
        for (const auto& batch : batches) {
            EXPECT_OK(writer->WriteBatch(batch));
        }
        EXPECT_OK(writer->Close());
        return writer->GetChannelId();
    }

    Result<std::unique_ptr<SpillReader>> CreateSpillReader(
        const FileIOChannel::ID& channel_id) const {
        return SpillReader::Create(file_system_, key_schema_, value_schema_, pool_, channel_id);
    }

 protected:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<FileSystem> file_system_;
    std::unique_ptr<UniqueTestDirectory> test_dir_;
    std::unique_ptr<IOManager> io_manager_;
    std::shared_ptr<FileIOChannel::Enumerator> channel_enumerator_;
    std::shared_ptr<SpillChannelManager> spill_channel_manager_;

    std::vector<DataField> value_fields_;
    std::vector<DataField> key_fields_;
    std::shared_ptr<arrow::Schema> write_schema_;
    std::shared_ptr<arrow::DataType> write_type_;
    std::shared_ptr<arrow::Schema> key_schema_;
    std::shared_ptr<arrow::Schema> value_schema_;
};

TEST_P(SpillReaderWriterTest, TestWriteBatch) {
    FileIOChannel::ID channel_id_1;
    FileIOChannel::ID channel_id_2;

    // First writer
    {
        ASSERT_OK_AND_ASSIGN(auto writer, CreateSpillWriter());

        auto batch = CreateRecordBatch(R"([
            [0, 1, "Alice", 10],
            [1, 1, "Bob",   20]
        ])",
                                       2);
        ASSERT_OK(writer->WriteBatch(batch));
        ASSERT_OK_AND_ASSIGN(int64_t file_size, writer->GetFileSize());
        ASSERT_GT(file_size, 0);
        ASSERT_OK(writer->Close());
        channel_id_1 = writer->GetChannelId();
    }
    // Second writer
    {
        ASSERT_OK_AND_ASSIGN(auto writer, CreateSpillWriter());

        auto batch_a = CreateRecordBatch(R"([
            [2, 1, "Carol", 30],
            [3, 1, "Dave",  40]
        ])",
                                         2);
        auto batch_b = CreateRecordBatch(R"([
            [4, 1, "Eve",   50],
            [5, 1, "Frank", 60],
            [6, 1, "Grace", 70]
        ])",
                                         3);
        ASSERT_OK(writer->WriteBatch(batch_a));
        ASSERT_OK_AND_ASSIGN(int64_t size_before, writer->GetFileSize());
        ASSERT_OK(writer->WriteBatch(batch_b));
        ASSERT_OK_AND_ASSIGN(int64_t size_after, writer->GetFileSize());
        ASSERT_GT(size_after, size_before);
        ASSERT_OK(writer->Close());
        channel_id_2 = writer->GetChannelId();
    }
    // Read back first writer's data
    {
        ASSERT_OK_AND_ASSIGN(auto reader, CreateSpillReader(channel_id_1));

        std::vector<std::string_view> expected_keys = {"Alice", "Bob"};
        int total_rows = 0;
        int batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
            if (iter == nullptr) {
                break;
            }
            batch_count++;
            while (iter->HasNext()) {
                ASSERT_OK_AND_ASSIGN(auto kv, iter->Next());
                ASSERT_EQ(kv.key->GetStringView(0), expected_keys[total_rows]);
                total_rows++;
            }
        }
        ASSERT_EQ(batch_count, 1);
        ASSERT_EQ(total_rows, 2);
        reader->Close();
    }
    // Read back second writer's data
    {
        ASSERT_OK_AND_ASSIGN(auto reader, CreateSpillReader(channel_id_2));

        std::vector<std::string_view> expected_keys = {"Carol", "Dave", "Eve", "Frank", "Grace"};
        int total_rows = 0;
        int batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
            if (iter == nullptr) {
                break;
            }
            batch_count++;
            while (iter->HasNext()) {
                ASSERT_OK_AND_ASSIGN(auto kv, iter->Next());
                ASSERT_EQ(kv.key->GetStringView(0), expected_keys[total_rows]);
                total_rows++;
            }
        }
        ASSERT_EQ(batch_count, 2);
        ASSERT_EQ(total_rows, 5);
        reader->Close();
    }
}

TEST_P(SpillReaderWriterTest, TestReadBatch) {
    {
        auto batch1 = CreateRecordBatch(R"([[0, 1, "Alice", 10], [1, 1, "Bob", 20]])", 2);
        auto batch2 =
            CreateRecordBatch(R"([[2, 1, "Carol", 30], [3, 2, "Dave", 40], [4, 3, "Eve", 50]])", 3);

        auto channel_id = WriteSpillFile({batch1, batch2});
        ASSERT_OK_AND_ASSIGN(auto reader, CreateSpillReader(channel_id));

        std::vector<std::string_view> expected_keys = {"Alice", "Bob", "Carol", "Dave", "Eve"};
        std::vector<int32_t> expected_values = {10, 20, 30, 40, 50};
        std::vector<int64_t> expected_seqs = {0, 1, 2, 3, 4};
        std::vector<int8_t> expected_kinds = {1, 1, 1, 2, 3};

        int total_rows = 0;
        int batch_count = 0;
        while (true) {
            ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
            if (iter == nullptr) break;
            batch_count++;
            while (iter->HasNext()) {
                ASSERT_OK_AND_ASSIGN(auto kv, iter->Next());
                ASSERT_EQ(kv.key->GetStringView(0), expected_keys[total_rows]);
                ASSERT_EQ(kv.value->GetStringView(0), expected_keys[total_rows]);
                ASSERT_EQ(kv.value->GetInt(1), expected_values[total_rows]);
                ASSERT_EQ(kv.sequence_number, expected_seqs[total_rows]);
                ASSERT_EQ(kv.value_kind->ToByteValue(), expected_kinds[total_rows]);
                total_rows++;
            }
        }
        ASSERT_EQ(batch_count, 2);
        ASSERT_EQ(total_rows, 5);
        reader->Close();
    }
    {
        auto empty_batch =
            arrow::RecordBatch::Make(write_schema_, 0,
                                     {arrow::MakeEmptyArray(arrow::int64()).ValueOrDie(),
                                      arrow::MakeEmptyArray(arrow::int8()).ValueOrDie(),
                                      arrow::MakeEmptyArray(arrow::utf8()).ValueOrDie(),
                                      arrow::MakeEmptyArray(arrow::int32()).ValueOrDie()});

        auto channel_id = WriteSpillFile({empty_batch});
        ASSERT_OK_AND_ASSIGN(auto reader, CreateSpillReader(channel_id));

        ASSERT_OK_AND_ASSIGN(auto iter, reader->NextBatch());
        if (iter != nullptr) {
            ASSERT_FALSE(iter->HasNext());
        }
        reader->Close();
    }
}

INSTANTIATE_TEST_SUITE_P(CompressionTypes, SpillReaderWriterTest,
                         ::testing::Values("zstd", "none", "uncompressed", "lz4"));

}  // namespace paimon::test
