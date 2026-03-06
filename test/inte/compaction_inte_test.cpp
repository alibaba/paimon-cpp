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

#include <memory>
#include <string>

#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/core/append/bucketed_append_compact_manager.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/operation/append_only_file_store_write.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/executor.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/write_context.h"

namespace paimon::test {

class CompactionTest : public testing::Test {
 public:
    std::shared_ptr<DataFileMeta> MakeFileMeta(const std::string& file_name) {
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/1, /*row_count=*/1, /*min_key=*/BinaryRow::EmptyRow(),
            /*max_key=*/BinaryRow::EmptyRow(), /*key_stats=*/SimpleStats::EmptyStats(),
            /*value_stats=*/SimpleStats::EmptyStats(), /*min_sequence_number=*/0,
            /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1721643142472ll, 0), /*delete_row_count=*/0,
            /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt, /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
    }

    arrow::Result<std::shared_ptr<arrow::StructArray>> PrepareData(
        const arrow::FieldVector& fields) {
        arrow::StringBuilder f0_builder;
        arrow::Int32Builder f1_builder;
        arrow::Int32Builder f2_builder;
        arrow::DoubleBuilder f3_builder;

        std::vector<std::tuple<std::string, int, int, double>> data = {{"Lily", 10, 0, 17.1}};

        for (const auto& row : data) {
            ARROW_RETURN_NOT_OK(f0_builder.Append(std::get<0>(row)));
            ARROW_RETURN_NOT_OK(f1_builder.Append(std::get<1>(row)));
            ARROW_RETURN_NOT_OK(f2_builder.Append(std::get<2>(row)));
            ARROW_RETURN_NOT_OK(f3_builder.Append(std::get<3>(row)));
        }

        std::shared_ptr<arrow::Array> f0_array, f1_array, f2_array, f3_array;
        ARROW_RETURN_NOT_OK(f0_builder.Finish(&f0_array));
        ARROW_RETURN_NOT_OK(f1_builder.Finish(&f1_array));
        ARROW_RETURN_NOT_OK(f2_builder.Finish(&f2_array));
        ARROW_RETURN_NOT_OK(f3_builder.Finish(&f3_array));

        std::vector<std::shared_ptr<arrow::Array>> children = {f0_array, f1_array, f2_array,
                                                               f3_array};
        auto struct_type = arrow::struct_(fields);
        return std::make_shared<arrow::StructArray>(struct_type, f0_array->length(), children);
    }
};

TEST_F(CompactionTest, TestCompaction) {
    std::string path = paimon::test::GetDataDir() + "/" + "orc" + "/append_09.db/append_09";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    auto pool = GetDefaultPool();
    ASSERT_TRUE(TestUtil::CopyDirectory(path, table_path));
    WriteContextBuilder context_builder(table_path, "commit_user_1");

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.WithStreamingMode(true).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> write,
                         FileStoreWrite::Create(std::move(write_context)));
    auto append_write = dynamic_cast<AppendOnlyFileStoreWrite*>(write.get());
    ASSERT_TRUE(append_write);

    BinaryRow partition(1);
    BinaryRowWriter binary_row_writer(&partition, 0, pool.get());
    binary_row_writer.WriteInt(0, 10);
    binary_row_writer.Complete();

    {
        arrow::FieldVector fields = {
            arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
            arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
        auto struct_array = PrepareData(fields);
        ASSERT_TRUE(struct_array.ok());
        ::ArrowArray arrow_array;
        auto arrow_status = arrow::ExportArray(*struct_array.ValueUnsafe(), &arrow_array);
        ASSERT_TRUE(arrow_status.ok());
        paimon::RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> record_batch,
                             batch_builder.SetPartition({{"f1", "10"}}).SetBucket(1).Finish());

        ASSERT_OK(append_write->Write(std::move(record_batch)));
        ASSERT_OK(append_write->Compact({{"f1", "10"}}, 1, /*full_compaction=*/true));
    }

    ASSERT_OK_AND_ASSIGN(auto msgs, append_write->PrepareCommit(/*wait_compaction=*/true, 1));
    ASSERT_EQ(msgs.size(), 1);
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msgs[0].get());
    auto compact_increment = msg_impl->GetCompactIncrement();
    ASSERT_EQ(compact_increment.CompactBefore().size(), 4);
    ASSERT_EQ(compact_increment.CompactAfter().size(), 1);
    std::cout << compact_increment.CompactAfter()[0]->file_name << std::endl;
    CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.IgnoreEmptyCommit(false).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> commit,
                         FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(msgs, 2));
}

}  // namespace paimon::test
