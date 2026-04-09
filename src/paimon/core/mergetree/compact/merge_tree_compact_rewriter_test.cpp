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

#include "paimon/core/mergetree/compact/merge_tree_compact_rewriter.h"

#include "arrow/api.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/core/mergetree/compact/interval_partition.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class MergeTreeCompactRewriterTest : public testing::Test {
 public:
    Result<std::unique_ptr<MergeTreeCompactRewriter>> CreateCompactRewriter(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        int32_t bucket, const BinaryRow& partition) const {
        PAIMON_ASSIGN_OR_RAISE(auto options, CoreOptions::FromMap(table_schema->Options()));
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        auto dv_factory = [](const std::string&) -> Result<std::shared_ptr<DeletionVector>> {
            return std::shared_ptr<DeletionVector>();
        };

        auto cancellation_controller = std::make_shared<CancellationController>();
        auto path_factory_cache =
            std::make_shared<FileStorePathFactoryCache>(table_path, table_schema, options, pool_);
        return MergeTreeCompactRewriter::Create(bucket, partition, table_schema, dv_factory,
                                                path_factory_cache, options,
                                                cancellation_controller, pool_);
    }

    Result<std::vector<std::vector<SortedRun>>> GenerateSortedRuns(
        const std::string& table_path, const std::shared_ptr<TableSchema>& table_schema,
        int32_t bucket, const std::map<std::string, std::string>& partition) const {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.SetBucketFilter(bucket).SetPartitionFilter({partition});
        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto result_plan, table_scan->CreatePlan());
        auto splits = result_plan->Splits();
        EXPECT_EQ(1, splits.size());

        auto data_split_impl = std::dynamic_pointer_cast<DataSplitImpl>(splits[0]);
        EXPECT_TRUE(data_split_impl);
        auto metas = data_split_impl->DataFiles();

        PAIMON_ASSIGN_OR_RAISE(auto pk_fields,
                               table_schema->GetFields(table_schema->TrimmedPrimaryKeys().value()));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FieldsComparator> key_comparator,
                               FieldsComparator::Create(pk_fields, /*is_ascending_order=*/true));
        IntervalPartition interval_partition(metas, key_comparator);
        return interval_partition.Partition();
    }

    void CheckResult(const std::string& compact_file_name, const std::shared_ptr<FileSystem>& fs,
                     const std::shared_ptr<TableSchema>& table_schema,
                     const std::shared_ptr<arrow::ChunkedArray>& expected_array) const {
        ASSERT_OK_AND_ASSIGN(auto file_format,
                             FileFormatFactory::Get("orc", table_schema->Options()));
        ASSERT_OK_AND_ASSIGN(auto reader_builder,
                             file_format->CreateReaderBuilder(/*batch_size=*/10));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             fs->Open(compact_file_name));
        ASSERT_OK_AND_ASSIGN(auto file_batch_reader, reader_builder->Build(input_stream));
        ASSERT_OK_AND_ASSIGN(auto result_array,
                             ReadResultCollector::CollectResult(file_batch_reader.get()));
        // handle type nullable, as result_array does not have not null flag
        result_array = result_array->View(expected_array->type()).ValueOrDie();

        ASSERT_TRUE(expected_array->type()->Equals(result_array->type()))
            << "result=" << result_array->type()->ToString()
            << ", expected=" << expected_array->type()->ToString() << std::endl;
        ASSERT_TRUE(expected_array->Equals(*result_array)) << result_array->ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(MergeTreeCompactRewriterTest, TestSimple) {
    std::string origin_table_path = GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/";
    auto table_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(TestUtil::CopyDirectory(origin_table_path, table_dir->Str()));
    std::string table_path = table_dir->Str() + "/pk_table_scan_and_read_mor";
    auto fs = table_dir->GetFileSystem();

    // load table schema
    SchemaManager schema_manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        auto rewriter,
        CreateCompactRewriter(table_path, table_schema, /*bucket=*/1,
                              /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool_.get())));

    // generate sorted runs and rewrite
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(table_path, table_schema, /*bucket=*/1,
                                                       /*partition=*/{{"f1", "10"}}))
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/5, /*drop_delete=*/true, runs));
    // check compact result
    ASSERT_EQ(4, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());
    const auto& compact_file_meta = compact_result.After()[0];
    auto expected_file_meta = std::make_shared<DataFileMeta>(
        "file.orc", 100l, /*row_count=*/7,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Bob"), 0}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Skye2"), 0}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 0}, {std::string("Skye2"), 0},
                                          {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Skye2"), 10, 0, 31.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0l, /*max_sequence_number=*/10l, /*schema_id=*/0, /*level=*/5,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), /*delete_row_count=*/0,
        nullptr, FileSource::Compact(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    ASSERT_TRUE(expected_file_meta->TEST_Equal(*compact_file_meta));
    // check compact file exist
    std::string compact_file_name =
        table_path + "/f1=10/bucket-1/" + compact_result.After()[0]->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  "Bob",  10,  0,  12.1],
[4,  0,  "David",  10,  0,  17.1],
[1,  0,  "Emily",  10,  0,  13.1],
[7,  0,  "Marco",  10,  0,  21.1],
[10, 0,  "Marco2",  10,  0,  31.1],
[6,  0,  "Skye",  10,  0,  21],
[9,  0,  "Skye2",  10,  0,  31]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, fs, table_schema, expected_array);
}

TEST_F(MergeTreeCompactRewriterTest, TestNotDropDelete) {
    std::string origin_table_path = GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/";
    auto table_dir = UniqueTestDirectory::Create("local");
    ASSERT_TRUE(TestUtil::CopyDirectory(origin_table_path, table_dir->Str()));
    std::string table_path = table_dir->Str() + "/pk_table_scan_and_read_mor";
    auto fs = table_dir->GetFileSystem();

    // load table schema
    SchemaManager schema_manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(
        auto rewriter,
        CreateCompactRewriter(table_path, table_schema, /*bucket=*/1,
                              /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool_.get())));

    // generate sorted runs and rewrite
    ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(table_path, table_schema, /*bucket=*/1,
                                                       /*partition=*/{{"f1", "10"}}))
    ASSERT_OK_AND_ASSIGN(auto compact_result, rewriter->Rewrite(
                                                  /*output_level=*/5, /*drop_delete=*/false, runs));
    // check compact result
    ASSERT_EQ(4, compact_result.Before().size());
    ASSERT_EQ(1, compact_result.After().size());
    const auto& compact_file_meta = compact_result.After()[0];
    auto expected_file_meta = std::make_shared<DataFileMeta>(
        "file.orc", 100l, /*row_count=*/9,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex"), 0}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Tony"), 0}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 0}, {std::string("Tony"), 0},
                                          {0, 0}, pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 31.2}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0l, /*max_sequence_number=*/11l, /*schema_id=*/0, /*level=*/5,
        std::vector<std::optional<std::string>>(), Timestamp(0l, 0), /*delete_row_count=*/2,
        nullptr, FileSource::Compact(), std::nullopt, std::nullopt, std::nullopt, std::nullopt);
    ASSERT_TRUE(expected_file_meta->TEST_Equal(*compact_file_meta));

    std::string compact_file_name =
        table_path + "/f1=10/bucket-1/" + compact_result.After()[0]->file_name;
    ASSERT_OK_AND_ASSIGN(bool exist, fs->Exists(compact_file_name));
    ASSERT_TRUE(exist);

    // check file content
    auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    auto type_with_special_fields =
        arrow::struct_(SpecialFields::CompleteSequenceAndValueKindField(arrow_schema)->fields());
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[11, 3,  "Alex",  10,  0,  31.2],
[0,  0,  "Bob",  10,  0,  12.1],
[4,  0,  "David",  10,  0,  17.1],
[1,  0,  "Emily",  10,  0,  13.1],
[7,  0,  "Marco",  10,  0,  21.1],
[10, 0,  "Marco2",  10,  0,  31.1],
[6,  0,  "Skye",  10,  0,  21],
[9,  0,  "Skye2",  10,  0,  31],
[5,  3,  "Tony",  10,  0, 14.1]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    CheckResult(compact_file_name, fs, table_schema, expected_array);
}

TEST_F(MergeTreeCompactRewriterTest, TestIOException) {
    std::string origin_table_path = GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/";

    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 500; i += RandomNumber(1, 17)) {
        auto table_dir = UniqueTestDirectory::Create("local");
        ASSERT_TRUE(TestUtil::CopyDirectory(origin_table_path, table_dir->Str()));
        std::string table_path = table_dir->Str() + "/pk_table_scan_and_read_mor";
        auto fs = table_dir->GetFileSystem();

        // load table schema
        SchemaManager schema_manager(fs, table_path);
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_OK_AND_ASSIGN(auto rewriter,
                             CreateCompactRewriter(
                                 table_path, table_schema, /*bucket=*/1,
                                 /*partition=*/BinaryRowGenerator::GenerateRow({10}, pool_.get())));

        // generate sorted runs and rewrite
        ASSERT_OK_AND_ASSIGN(auto runs, GenerateSortedRuns(table_path, table_schema, /*bucket=*/1,
                                                           /*partition=*/{{"f1", "10"}}))
        // rewrite may trigger I/O exception
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        auto compact_result = rewriter->Rewrite(
            /*output_level=*/5, /*drop_delete=*/true, runs);
        CHECK_HOOK_STATUS(compact_result.status(), i);
        io_hook->Clear();

        // check compact result
        ASSERT_EQ(4, compact_result.value().Before().size());
        ASSERT_EQ(1, compact_result.value().After().size());
        std::string compact_file_name =
            table_path + "/f1=10/bucket-1/" + compact_result.value().After()[0]->file_name;
        ASSERT_OK_AND_ASSIGN(bool exist, fs->Exists(compact_file_name));
        ASSERT_TRUE(exist);

        // check file content
        auto arrow_schema = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
        auto type_with_special_fields = arrow::struct_(
            SpecialFields::CompleteSequenceAndValueKindField(arrow_schema)->fields());
        std::shared_ptr<arrow::ChunkedArray> expected_array;
        auto array_status =
            arrow::ipc::internal::json::ChunkedArrayFromJSON(type_with_special_fields, {R"([
[0,  0,  "Bob",  10,  0,  12.1],
[4,  0,  "David",  10,  0,  17.1],
[1,  0,  "Emily",  10,  0,  13.1],
[7,  0,  "Marco",  10,  0,  21.1],
[10, 0,  "Marco2",  10,  0,  31.1],
[6,  0,  "Skye",  10,  0,  21],
[9,  0,  "Skye2",  10,  0,  31]
])"},
                                                             &expected_array);
        ASSERT_TRUE(array_status.ok());
        CheckResult(compact_file_name, fs, table_schema, expected_array);
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

}  // namespace paimon::test
