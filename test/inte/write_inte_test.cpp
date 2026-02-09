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

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/type.h"
#include "fmt/format.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/data/binary_array.h"
#include "paimon/common/data/binary_array_writer.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/factories/io_hook.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/common/utils/decimal_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/snapshot.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_utils.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/data/blob.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/reader_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/read_context.h"
#include "paimon/reader/batch_reader.h"
#include "paimon/reader/file_batch_reader.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/startup_mode.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/data_generator.h"
#include "paimon/testing/utils/io_exception_helper.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/testing/utils/timezone_guard.h"
#include "paimon/utils/roaring_bitmap32.h"
#include "paimon/write_context.h"
namespace paimon {
class CommitMessage;
class TableSchema;
}  // namespace paimon

namespace paimon::test {
class WriteInteTest : public testing::Test, public ::testing::WithParamInterface<std::string> {
 public:
    void SetUp() override {
        file_system_ = std::make_shared<LocalFileSystem>();
        pool_ = GetDefaultPool();
        int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
        std::srand(seed);
    }

    Result<std::string> CreateTestTable(const std::string& base_path, const std::string& db_name,
                                        const std::string& table_name, ::ArrowSchema* schema,
                                        const std::vector<std::string>& partition_keys,
                                        const std::vector<std::string>& primary_keys,
                                        const std::map<std::string, std::string>& options) const {
        PAIMON_ASSIGN_OR_RAISE(auto catalog, Catalog::Create(base_path, options));
        PAIMON_RETURN_NOT_OK(catalog->CreateDatabase(db_name, options, /*ignore_if_exists=*/false));
        Identifier table_id(db_name, table_name);
        PAIMON_RETURN_NOT_OK(catalog->CreateTable(table_id, schema, partition_keys, primary_keys,
                                                  options,
                                                  /*ignore_if_exists=*/false));
        return PathUtil::JoinPath(base_path, db_name + ".db/" + table_name);
    }

    using ValueType = std::tuple<std::string, int32_t, int32_t, double>;
    Result<std::unique_ptr<RecordBatch>> MakeRecordBatch(
        const std::vector<ValueType>& raw_data,
        const std::map<std::string, std::string>& partition_map, int32_t bucket) const {
        ::ArrowArray arrow_array;
        std::shared_ptr<arrow::Array> array = GenerateArrowArray(raw_data);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        return batch_builder.SetPartition(partition_map).SetBucket(bucket).Finish();
    }

    BinaryRow MakeBinaryRow(const RowKind* kind, const std::string& f0, const std::string& f1,
                            int32_t f2, double f3) const {
        return BinaryRowGenerator::GenerateRow(kind, {f0, f1, f2, f3}, pool_.get());
    }

    void CheckFileCount(const std::string& root_path, const std::vector<std::string>& subdirs,
                        int32_t expect_file_count) const {
        std::vector<std::unique_ptr<BasicFileStatus>> status_list;
        for (const auto& dir : subdirs) {
            ASSERT_OK(file_system_->ListDir(PathUtil::JoinPath(root_path, dir), &status_list));
        }
        int32_t file_count = 0;
        for (const auto& file_status : status_list) {
            if (!file_status->IsDir()) {
                file_count++;
            }
        }
        ASSERT_EQ(file_count, expect_file_count);
    }

    std::shared_ptr<arrow::Array> GenerateArrowArray(const std::vector<ValueType>& raw_data,
                                                     bool exist_null_value = false) const {
        auto string_field = arrow::field("f0", arrow::utf8());
        auto int_field = arrow::field("f1", arrow::int32());
        auto int_field1 = arrow::field("f2", arrow::int32());
        auto double_field = arrow::field("f3", arrow::float64());
        auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
        auto schema =
            arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

        arrow::StructBuilder struct_builder(
            struct_type, arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::Int32Builder>(), std::make_shared<arrow::DoubleBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto int_builder1 = static_cast<arrow::Int32Builder*>(struct_builder.field_builder(2));
        auto double_builder = static_cast<arrow::DoubleBuilder*>(struct_builder.field_builder(3));

        for (const auto& d : raw_data) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append(std::get<0>(d)).ok());
            EXPECT_TRUE(int_builder->Append(std::get<1>(d)).ok());
            EXPECT_TRUE(int_builder1->Append(std::get<2>(d)).ok());
            if (exist_null_value) {
                EXPECT_TRUE(double_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(double_builder->Append(std::get<3>(d)).ok());
            }
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    }

    std::shared_ptr<DataFileMeta> ReconstructDataFileMeta(
        const std::shared_ptr<DataFileMeta>& file_meta) const {
        if (GetParam() != "lance" && GetParam() != "avro") {
            return file_meta;
        }
        // for lance and avro format, all stats is null
        auto new_meta = std::make_shared<DataFileMeta>(
            file_meta->file_name, file_meta->file_size, file_meta->row_count, file_meta->min_key,
            file_meta->max_key, file_meta->key_stats, file_meta->value_stats,
            file_meta->min_sequence_number, file_meta->max_sequence_number, file_meta->schema_id,
            file_meta->level, file_meta->extra_files, file_meta->creation_time,
            file_meta->delete_row_count, file_meta->embedded_index, file_meta->file_source,
            file_meta->value_stats_cols, file_meta->external_path, file_meta->first_row_id,
            file_meta->write_cols);
        auto generate_null_stats = [this](const SimpleStats& stats) -> SimpleStats {
            if (stats == SimpleStats::EmptyStats()) {
                return stats;
            }
            BinaryRowGenerator::ValueType min_values(stats.MinValues().GetFieldCount(), NullType());
            BinaryRowGenerator::ValueType max_values(stats.MaxValues().GetFieldCount(), NullType());
            BinaryArray null_counts;
            BinaryArrayWriter array_writer(&null_counts, stats.NullCounts().Size(),
                                           /*element_size=*/sizeof(int64_t), pool_.get());
            for (int32_t i = 0; i < stats.NullCounts().Size(); ++i) {
                array_writer.SetNullAt(i);
            }
            array_writer.Complete();

            return SimpleStats(BinaryRowGenerator::GenerateRow(min_values, pool_.get()),
                               BinaryRowGenerator::GenerateRow(max_values, pool_.get()),
                               null_counts);
        };
        new_meta->key_stats = generate_null_stats(new_meta->key_stats);
        new_meta->value_stats = generate_null_stats(new_meta->value_stats);
        return new_meta;
    }

    void CheckCreationTime(const std::vector<std::shared_ptr<CommitMessage>>& commit_messages) {
        TimezoneGuard guard("Asia/Shanghai");
        for (const auto& msg : commit_messages) {
            auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
            ASSERT_TRUE(msg_impl);
            const auto& data_increment = msg_impl->GetNewFilesIncrement();
            for (const auto& meta : data_increment.NewFiles()) {
                const auto& creation_time = meta->creation_time;
                ASSERT_OK_AND_ASSIGN(int64_t utc_creation_time, meta->CreationTimeEpochMillis());
                ASSERT_EQ(creation_time.GetMillisecond() - utc_creation_time, 28800000l);
                // creation time > 2023-09-28 and < 2223-09-28
                ASSERT_GT(creation_time.GetMillisecond(), 1695866400000l);
                ASSERT_LT(creation_time.GetMillisecond(), 8007213600000l);
            }
        }
    }

 private:
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<MemoryPool> pool_;
};

std::vector<std::string> GetTestValuesForWriteInteTest() {
    std::vector<std::string> values;
    values.emplace_back("parquet");
#ifdef PAIMON_ENABLE_ORC
    values.emplace_back("orc");
#endif
#ifdef PAIMON_ENABLE_LANCE
    values.emplace_back("lance");
#endif
#ifdef PAIMON_ENABLE_AVRO
    values.emplace_back("avro");
#endif
    return values;
}

INSTANTIATE_TEST_SUITE_P(FileFormat, WriteInteTest,
                         ::testing::ValuesIn(GetTestValuesForWriteInteTest()));

TEST_P(WriteInteTest, TestAppendTableBatchWrite) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::FILE_SYSTEM, "local"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;

    std::string data_1 =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-2), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), NullType(), static_cast<int64_t>(-4294967298),
             static_cast<float>(0.5), 1.141592659, std::string("20250326"), NullType()},
            {true, static_cast<int8_t>(1), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), NullType(), static_cast<int64_t>(4294967296),
             static_cast<float>(2.0), 3.141592657, std::string("20250327"), NullType()},
            std::vector<int64_t>({1, 0, 0, 1, 4, 1, 0, 0, 1, 0}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/-1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));
    CheckCreationTime(commit_msgs);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(4, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    std::string expected_data_1 =
        R"([[0, true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [0, false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [0, null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [0, true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);
    std::string data_2 =
        fmt::format(R"([
                   [true, {},{},{},null,{},{},{},"∞",""],
                   [false,{},{},{},null,{},{},{}, "",""],
                   [true, 42,-1,999999999,null,123456789012345,0.1,-Inf,"a\u20ACb","binary\ndata"]])",
                    std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<float>::min(), std::numeric_limits<double>::min(),
                    std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<float>::max(), std::numeric_limits<double>::max());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_NOK_WITH_MSG(
        helper->WriteAndCommit(std::move(batch_2), commit_identifier++, std::nullopt),
        "batch write mode only support one-time committing.");
}

TEST_P(WriteInteTest, TestAppendTableStreamWriteWithOneBucket) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f5"},         {Options::FILE_SYSTEM, "local"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    std::string data_1 =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-2), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), NullType(), static_cast<int64_t>(-4294967298),
             static_cast<float>(0.5), 1.141592659, std::string("20250326"), NullType()},
            {true, static_cast<int8_t>(1), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), NullType(), static_cast<int64_t>(4294967296),
             static_cast<float>(2.0), 3.141592657, std::string("20250327"), NullType()},
            std::vector<int64_t>({1, 0, 0, 1, 4, 1, 0, 0, 1, 0}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(4, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    std::string expected_data_1 =
        R"([[0, true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [0, false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [0, null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [0, true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);

    std::string data_2 =
        fmt::format(R"([
                   [true, {},{},{},null,{},{},{},"∞",""],
                   [false,{},{},{},null,{},{},{}, "",""],
                   [true, 42,-1,999999999,null,123456789012345,0.1,-Inf,"a\u20ACb","binary\ndata"]])",
                    std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<float>::min(), std::numeric_limits<double>::min(),
                    std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<float>::max(), std::numeric_limits<double>::max());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {false, std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
             std::numeric_limits<int32_t>::min(), NullType(), std::numeric_limits<int64_t>::min(),
             std::numeric_limits<float>::min(), -std::numeric_limits<double>::infinity(),
             std::string(""), NullType()},
            {true, std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
             std::numeric_limits<int32_t>::max(), NullType(), std::numeric_limits<int64_t>::max(),
             std::numeric_limits<float>::max(), std::numeric_limits<double>::max(),
             std::string("∞"), NullType()},
            std::vector<int64_t>({0, 0, 0, 0, 3, 0, 0, 0, 0, 0}), pool_.get()),
        /*min_sequence_number=*/4, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment_2, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_2};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(7, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(3, snapshot2.value().DeltaRecordCount().value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 1);

    std::string expected_data_2 =
        fmt::format(R"([
                   [0, true, {},{},{},null,{},{},{},"∞",""],
                   [0, false,{},{},{},null,{},{},{}, "",""],
                   [0, true, 42,-1,999999999,null,123456789012345,0.1,-Inf,"a\u20ACb","binary\ndata"]])",
                    std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<float>::min(), std::numeric_limits<double>::min(),
                    std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<float>::max(), std::numeric_limits<double>::max());
    ASSERT_OK_AND_ASSIGN(success,
                         helper->ReadAndCheckResult(data_type, data_splits_2, expected_data_2));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestAppendTableStreamWriteWithPartitionAndMultiBuckets) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::utf8()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f2", "f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> datas_1;
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({0, 19, std::string("20250326"), 9.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({0, 19, std::string("20250326"), 10.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({1, 19, std::string("20250326"), 11.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({1, 19, std::string("20250326"), 12.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({1, 19, std::string("20250326"), 13.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({0, 22, std::string("20250326"), 14.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({0, 22, std::string("20250326"), 15.1}, pool_.get()));
    datas_1.push_back(
        BinaryRowGenerator::GenerateRow({0, 22, std::string("20250326"), 16.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_1, gen.SplitArrayByPartitionAndBucket(datas_1));
    ASSERT_EQ(3, batches_1.size());
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batches_1), commit_identifier++, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(8, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(8, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 3);

    std::map<std::pair<std::string, int32_t>, std::string> expected_datas_1;
    expected_datas_1[std::make_pair("f2=20250326/f1=19/", 1)] = R"([
[0, 0, 19, "20250326", 9.1],
[0, 0, 19, "20250326", 10.1]
])";

    expected_datas_1[std::make_pair("f2=20250326/f1=19/", 0)] = R"([
[0, 1, 19, "20250326",  11.1],
[0, 1, 19, "20250326",  12.1],
[0, 1, 19, "20250326",  13.1]
])";

    expected_datas_1[std::make_pair("f2=20250326/f1=22/", 1)] = R"([
[0, 0, 22, "20250326", 14.1],
[0, 0, 22, "20250326", 15.1],
[0, 0, 22, "20250326", 16.1]
])";

    for (const auto& split : data_splits_1) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_1.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas_1.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }

    std::vector<BinaryRow> datas_2;
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({0, 20, std::string("20250326"), 19.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({0, 20, std::string("20250326"), 20.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({1, 20, std::string("20250326"), 21.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({1, 20, std::string("20250326"), 22.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({1, 20, std::string("20250326"), 23.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({0, 23, std::string("20250326"), 24.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({0, 23, std::string("20250326"), 25.1}, pool_.get()));
    datas_2.push_back(
        BinaryRowGenerator::GenerateRow({0, 23, std::string("20250326"), 26.1}, pool_.get()));
    ASSERT_OK_AND_ASSIGN(auto batches_2, gen.SplitArrayByPartitionAndBucket(datas_2));
    ASSERT_EQ(3, batches_2.size());
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs_2,
        helper->WriteAndCommit(std::move(batches_2), commit_identifier++, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(16, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(8, snapshot2.value().DeltaRecordCount().value());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 3);

    std::map<std::pair<std::string, int32_t>, std::string> expected_datas_2;
    expected_datas_2[std::make_pair("f2=20250326/f1=20/", 1)] = R"([
[0, 0, 20, "20250326", 19.1],
[0, 0, 20, "20250326", 20.1]
])";

    expected_datas_2[std::make_pair("f2=20250326/f1=20/", 0)] = R"([
[0, 1, 20, "20250326",  21.1],
[0, 1, 20, "20250326",  22.1],
[0, 1, 20, "20250326",  23.1]
])";

    expected_datas_2[std::make_pair("f2=20250326/f1=23/", 1)] = R"([
[0, 0, 23, "20250326", 24.1],
[0, 0, 23, "20250326", 25.1],
[0, 0, 23, "20250326", 26.1]
])";

    for (const auto& split : data_splits_2) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_2.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas_2.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }
}

TEST_P(WriteInteTest, TestAppendTableWriteWithComplexType) {
    if (GetParam() == "lance") {
        // lance do not support map
        return;
    }
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f5"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    std::string data_1 = R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data_1, /*partition_map=*/{}, /*bucket=*/0,
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT}));

    SimpleStats value_stats = BinaryRowGenerator::GenerateStats(
        {NullType(), NullType(), NullType(), TimestampType(Timestamp(0, 0), 3),
         static_cast<int32_t>(24), Decimal(2, 2, DecimalUtils::StrToInt128("12").value())},
        {NullType(), NullType(), NullType(), TimestampType(Timestamp(123999, 000000), 3),
         static_cast<int32_t>(2456), Decimal(2, 2, DecimalUtils::StrToInt128("78").value())},
        file_format == "parquet" ? std::vector<int64_t>({-1, -1, -1, 0, 0, 0})
                                 : std::vector<int64_t>({0, 0, 0, 0, 0, 0}),
        pool_.get());

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/6, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(), value_stats,
        /*min_sequence_number=*/0, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(6, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(6, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    std::string expected_data_1 = R"([
        [0, [[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [0, [[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [0, [[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [0, [[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";

    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);

    std::string data_2 = R"([
        [[[10, 11]], [1.1, 1.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [[[10, 20], [20, 30]], [2.0, 3.0], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 61], [21, 31]], [2.1, 3.1], [false, 1], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data_2, /*partition_map=*/{}, /*bucket=*/0,
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT}));

    SimpleStats value_stats_2 = value_stats;
    value_stats_2.max_values_.SetInt(4, 245);
    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/4, /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(), value_stats_2,
        /*min_sequence_number=*/6, /*max_sequence_number=*/9, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment_2, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_2};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(10, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot2.value().DeltaRecordCount().value());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 1);
    std::string expected_data_2 = R"([
        [0, [[10, 11]], [1.1, 1.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [0, [[10, 20], [20, 30]], [2.0, 3.0], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [0, [[11, 61], [21, 31]], [2.1, 3.1], [false, 1], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(success,
                         helper->ReadAndCheckResult(data_type, data_splits_2, expected_data_2));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestPkTableStreamWrite) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    // round 1 write
    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> datas_1;
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "David", "20250325", 21, 13.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
    ASSERT_OK_AND_ASSIGN(auto batches_1, gen.SplitArrayByPartitionAndBucket(datas_1));
    ASSERT_EQ(3, batches_1.size());

    auto file_meta_1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David")}, {std::string("David")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325"), 21, 13.1},
                                          {std::string("David"), std::string("20250325"), 21, 13.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_1 = ReconstructDataFileMeta(file_meta_1);
    DataIncrement data_increment_1({file_meta_1}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/0,
        /*total_bucket=*/2, data_increment_1, CompactIncrement({}, {}, {}));

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Cathy")}, {std::string("Cathy")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Cathy"), std::string("20250325"), 20, 12.1},
                                          {std::string("Cathy"), std::string("20250325"), 20, 12.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, data_increment_2, CompactIncrement({}, {}, {}));

    auto file_meta_3 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Evan")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex")}, {std::string("Evan")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), std::string("20250326"), 18, 10.1},
                                          {std::string("Evan"), std::string("20250326"), 22, 14.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_3 = ReconstructDataFileMeta(file_meta_3);
    DataIncrement data_increment_3({file_meta_3}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_3 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, data_increment_3, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1, expected_commit_message_2, expected_commit_message_3};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batches_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

    // round 1 read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 3);

    std::map<std::pair<std::string, int32_t>, std::string> expected_datas_1;
    expected_datas_1[std::make_pair("f1=20250325/", 0)] = R"([[0, "David", "20250325", 21, 13.1]])";
    expected_datas_1[std::make_pair("f1=20250325/", 1)] = R"([[0, "Cathy", "20250325", 20, 12.1]])";
    expected_datas_1[std::make_pair("f1=20250326/", 1)] = R"([[0, "Evan", "20250326", 22, 14.1]])";

    for (const auto& split : data_splits_1) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_1.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas_1.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }

    // round 2 write
    std::vector<BinaryRow> datas_2;
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Farm", "20250326", 15, 22.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Go", "20250325", 22, 23.1));
    datas_2.push_back(MakeBinaryRow(RowKind::UpdateAfter(), "David", "20250325", 22, 24.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Hi", "20250325", 23, 24.1));
    ASSERT_OK_AND_ASSIGN(auto batches_2, gen.SplitArrayByPartitionAndBucket(datas_2));
    ASSERT_EQ(3, batches_2.size());

    auto file_meta_4 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David")}, {std::string("David")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325"), 22, 24.1},
                                          {std::string("David"), std::string("20250325"), 22, 24.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_4 = ReconstructDataFileMeta(file_meta_4);
    DataIncrement data_increment_4({file_meta_4}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_4 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/0,
        /*total_bucket=*/2, data_increment_4, CompactIncrement({}, {}, {}));

    auto file_meta_5 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Go")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Hi")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Go")}, {std::string("Hi")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Go"), std::string("20250325"), 22, 23.1},
                                          {std::string("Hi"), std::string("20250325"), 23, 24.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_5 = ReconstructDataFileMeta(file_meta_5);
    DataIncrement data_increment_5({file_meta_5}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_5 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, data_increment_5, CompactIncrement({}, {}, {}));

    auto file_meta_6 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Farm")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Farm")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Farm")}, {std::string("Farm")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Farm"), std::string("20250326"), 15, 22.1},
                                          {std::string("Farm"), std::string("20250326"), 15, 22.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_6 = ReconstructDataFileMeta(file_meta_6);
    DataIncrement data_increment_6({file_meta_6}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_6 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")}, pool_.get()),
        /*bucket=*/0,
        /*total_bucket=*/2, data_increment_6, CompactIncrement({}, {}, {}));

    std::shared_ptr<CommitMessage> expected_commit_message_7 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, DataIncrement({}, {}, {}), CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_4, expected_commit_message_5, expected_commit_message_6,
        expected_commit_message_7};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batches_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(9, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot2.value().DeltaRecordCount().value());

    // round 2 read
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 3);
    std::map<std::pair<std::string, int32_t>, std::string> expected_datas_2;
    expected_datas_2[std::make_pair("f1=20250326/", 0)] = R"([[0, "Farm", "20250326", 15, 22.1]])";
    expected_datas_2[std::make_pair("f1=20250325/", 0)] = R"([[2, "David", "20250325", 22, 24.1]])";
    expected_datas_2[std::make_pair("f1=20250325/", 1)] =
        R"([[0, "Go", "20250325", 22, 23.1], [0, "Hi", "20250325", 23, 24.1]])";

    for (const auto& split : data_splits_2) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_2.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas_2.end()) << partition_str << " " << split_impl->Bucket();
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        EXPECT_TRUE(success);
    }
}

TEST_P(WriteInteTest, TestPkTableBatchWrite) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> datas_1;
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "David", "20250325", 21, 13.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
    ASSERT_OK_AND_ASSIGN(auto batches_1, gen.SplitArrayByPartitionAndBucket(datas_1));
    ASSERT_EQ(3, batches_1.size());
    auto file_meta_1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David")}, {std::string("David")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325"), 21, 13.1},
                                          {std::string("David"), std::string("20250325"), 21, 13.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_1 = ReconstructDataFileMeta(file_meta_1);
    DataIncrement data_increment_1({file_meta_1}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/0,
        /*total_bucket=*/2, data_increment_1, CompactIncrement({}, {}, {}));

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Cathy")}, {std::string("Cathy")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Cathy"), std::string("20250325"), 20, 12.1},
                                          {std::string("Cathy"), std::string("20250325"), 20, 12.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, data_increment_2, CompactIncrement({}, {}, {}));

    auto file_meta_3 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/3,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Evan")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex")}, {std::string("Evan")}, {0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), std::string("20250326"), 18, 10.1},
                                          {std::string("Evan"), std::string("20250326"), 22, 14.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_3 = ReconstructDataFileMeta(file_meta_3);
    DataIncrement data_increment_3({file_meta_3}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_3 = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")}, pool_.get()),
        /*bucket=*/1,
        /*total_bucket=*/2, data_increment_3, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1, expected_commit_message_2, expected_commit_message_3};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batches_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 3);

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    std::map<std::pair<std::string, int32_t>, std::string> expected_datas_1;
    expected_datas_1[std::make_pair("f1=20250325/", 0)] = R"([[0, "David", "20250325", 21, 13.1]])";
    expected_datas_1[std::make_pair("f1=20250325/", 1)] = R"([[0, "Cathy", "20250325", 20, 12.1]])";
    expected_datas_1[std::make_pair("f1=20250326/", 1)] = R"([[0, "Evan", "20250326", 22, 14.1]])";

    for (const auto& split : data_splits_1) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_1.find(std::make_pair(partition_str, split_impl->Bucket()));
        ASSERT_TRUE(iter != expected_datas_1.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }

    std::vector<BinaryRow> datas_2;
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Farm", "20250326", 15, 22.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Go", "20250325", 22, 23.1));
    datas_2.push_back(MakeBinaryRow(RowKind::UpdateAfter(), "David", "20250325", 22, 24.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Hi", "20250325", 23, 24.1));
    ASSERT_OK_AND_ASSIGN(auto batches_2, gen.SplitArrayByPartitionAndBucket(datas_2));
    ASSERT_EQ(3, batches_2.size());
    ASSERT_NOK_WITH_MSG(helper->WriteAndCommit(std::move(batches_2), commit_identifier++,
                                               /*expected_commit_messages=*/std::nullopt),
                        "batch write mode only support one-time committing.");
}

TEST_P(WriteInteTest, TestPkTableWriteWithNoPartitionKey) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> datas_1;
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "David", "20250325", 21, 13.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
    datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
    ASSERT_OK_AND_ASSIGN(auto batches_1, gen.SplitArrayByPartitionAndBucket(datas_1));
    ASSERT_EQ(2, batches_1.size());

    auto file_meta_1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1,
        /*min_key=*/
        BinaryRowGenerator::GenerateRow({std::string("David"), std::string("20250325")},
                                        pool_.get()),
        /*max_key=*/
        BinaryRowGenerator::GenerateRow({std::string("David"), std::string("20250325")},
                                        pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325")},
                                          {std::string("David"), std::string("20250325")}, {0, 0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325"), 21, 13.1},
                                          {std::string("David"), std::string("20250325"), 21, 13.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_1 = ReconstructDataFileMeta(file_meta_1);
    DataIncrement data_increment_1({file_meta_1}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/2, data_increment_1, CompactIncrement({}, {}, {}));

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/4,
        /*min_key=*/
        BinaryRowGenerator::GenerateRow({std::string("Alex"), std::string("20250326")},
                                        pool_.get()),
        /*max_key=*/
        BinaryRowGenerator::GenerateRow({std::string("Evan"), std::string("20250326")},
                                        pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), std::string("20250325")},
                                          {std::string("Evan"), std::string("20250326")}, {0, 0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Alex"), std::string("20250325"), 18, 10.1},
                                          {std::string("Evan"), std::string("20250326"), 22, 14.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/2, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/1,
        /*total_bucket=*/2, data_increment_2, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1, expected_commit_message_2};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batches_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

    // round1 read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 2);

    std::map<int32_t, std::string> expected_datas_1;
    expected_datas_1[0] = R"([[0, "David", "20250325", 21, 13.1]])";
    expected_datas_1[1] =
        R"([[0, "Cathy", "20250325", 20, 12.1], [0, "Evan", "20250326", 22, 14.1]])";
    for (const auto& split : data_splits_1) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_1.find(split_impl->Bucket());
        ASSERT_TRUE(iter != expected_datas_1.end());
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        ASSERT_TRUE(success);
    }

    // round2
    std::vector<BinaryRow> datas_2;
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Farm", "20250326", 15, 22.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Go", "20250325", 22, 23.1));
    datas_2.push_back(MakeBinaryRow(RowKind::UpdateAfter(), "David", "20250325", 22, 24.1));
    datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Hi", "20250325", 23, 24.1));
    ASSERT_OK_AND_ASSIGN(auto batches_2, gen.SplitArrayByPartitionAndBucket(datas_2));
    ASSERT_EQ(2, batches_2.size());

    auto file_meta_3 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/2,
        /*min_key=*/
        BinaryRowGenerator::GenerateRow({std::string("David"), std::string("20250325")},
                                        pool_.get()),
        /*max_key=*/
        BinaryRowGenerator::GenerateRow({std::string("Farm"), std::string("20250326")},
                                        pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325")},
                                          {std::string("Farm"), std::string("20250326")}, {0, 0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("David"), std::string("20250325"), 15, 22.1},
                                          {std::string("Farm"), std::string("20250326"), 22, 24.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/2, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_3 = ReconstructDataFileMeta(file_meta_3);
    DataIncrement data_increment_3({file_meta_3}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_3 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/2, data_increment_3, CompactIncrement({}, {}, {}));

    auto file_meta_4 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/2,
        /*min_key=*/
        BinaryRowGenerator::GenerateRow({std::string("Go"), std::string("20250325")}, pool_.get()),
        /*max_key=*/
        BinaryRowGenerator::GenerateRow({std::string("Hi"), std::string("20250325")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Go"), std::string("20250325")},
                                          {std::string("Hi"), std::string("20250325")}, {0, 0},
                                          pool_.get()),
        /*value_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("Go"), std::string("20250325"), 22, 23.1},
                                          {std::string("Hi"), std::string("20250325"), 23, 24.1},
                                          {0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/6, /*max_sequence_number=*/7, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_4 = ReconstructDataFileMeta(file_meta_4);
    DataIncrement data_increment_4({file_meta_4}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_4 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/1,
        /*total_bucket=*/2, data_increment_4, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_3, expected_commit_message_4};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batches_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(9, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot2.value().DeltaRecordCount().value());

    // round2 read
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 2);
    std::map<int32_t, std::string> expected_datas_2;
    expected_datas_2[0] =
        R"([[2, "David", "20250325", 22, 24.1],[0, "Farm", "20250326", 15, 22.1]])";
    expected_datas_2[1] = R"([[0, "Go", "20250325", 22, 23.1], [0, "Hi", "20250325", 23, 24.1]])";

    for (const auto& split : data_splits_2) {
        auto split_impl = dynamic_cast<DataSplitImpl*>(split.get());
        ASSERT_OK_AND_ASSIGN(std::string partition_str,
                             helper->PartitionStr(split_impl->Partition()));
        auto iter = expected_datas_2.find(split_impl->Bucket());
        ASSERT_TRUE(iter != expected_datas_2.end()) << partition_str << " " << split_impl->Bucket();
        ASSERT_OK_AND_ASSIGN(bool success,
                             helper->ReadAndCheckResult(data_type, {split}, iter->second));
        EXPECT_TRUE(success);
    }
}

TEST_P(WriteInteTest, TestPkTableWriteWithComplexType) {
    if (GetParam() == "lance") {
        // lance do not support map
        return;
    }
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("f4", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("f5", arrow::date32()),
        arrow::field("f6", arrow::decimal128(2, 2))};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f6", "f4", "f5"};
    std::vector<std::string> partition_keys = {};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f5"},         {Options::FILE_SYSTEM, "local"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    std::string data_1 = R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [[[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [[[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data_1, /*partition_map=*/{}, /*bucket=*/0,
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT}));

    auto min_key = BinaryRowGenerator::GenerateRow(
        {Decimal(2, 2, DecimalUtils::StrToInt128("12").value()),
         TimestampType(Timestamp(123123, 000000), 3), static_cast<int32_t>(245)},
        pool_.get());
    auto max_key = BinaryRowGenerator::GenerateRow(
        {Decimal(2, 2, DecimalUtils::StrToInt128("78").value()),
         TimestampType(Timestamp(123, 000000), 3), static_cast<int32_t>(24)},
        pool_.get());
    SimpleStats key_stats = BinaryRowGenerator::GenerateStats(
        {Decimal(2, 2, DecimalUtils::StrToInt128("12").value()), TimestampType(Timestamp(0, 0), 3),
         static_cast<int32_t>(24)},
        {Decimal(2, 2, DecimalUtils::StrToInt128("78").value()),
         TimestampType(Timestamp(123999, 000000), 3), static_cast<int32_t>(2456)},
        std::vector<int64_t>({0, 0, 0}), pool_.get());
    SimpleStats value_stats = BinaryRowGenerator::GenerateStats(
        {NullType(), NullType(), NullType(), TimestampType(Timestamp(0, 0), 3),
         static_cast<int32_t>(24), Decimal(2, 2, DecimalUtils::StrToInt128("12").value())},
        {NullType(), NullType(), NullType(), TimestampType(Timestamp(123999, 000000), 3),
         static_cast<int32_t>(2456), Decimal(2, 2, DecimalUtils::StrToInt128("78").value())},
        file_format == "parquet" ? std::vector<int64_t>({-1, -1, -1, 0, 0, 0})
                                 : std::vector<int64_t>({0, 0, 0, 0, 0, 0}),
        pool_.get());

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/5, min_key, max_key, key_stats, value_stats,
        /*min_sequence_number=*/1, /*max_sequence_number=*/5, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    std::string expected_data_1 = R"([
        [0, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [0, [[10, 10]], [1.1, 1.2], [false, 12], "1970-01-01 00:02:03.123", 2456, "0.22"],
        [0, [[0, 1]], [0.1, 0.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [0, [[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [0, [[11, 64], [12, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);

    std::string data_2 = R"([
        [[[10, 11]], [1.1, 1.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [[[10, 20], [20, 30]], [2.0, 3.0], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [[[11, 61], [21, 31]], [2.1, 3.1], [false, 1], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_2,
        TestHelper::MakeRecordBatch(
            arrow::struct_(fields), data_2, /*partition_map=*/{}, /*bucket=*/0,
            {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::DELETE,
             RecordBatch::RowKind::UPDATE_BEFORE, RecordBatch::RowKind::UPDATE_AFTER}));

    auto min_key_2 = BinaryRowGenerator::GenerateRow(
        {Decimal(2, 2, DecimalUtils::StrToInt128("12").value()),
         TimestampType(Timestamp(123123, 000000), 3), static_cast<int32_t>(245)},
        pool_.get());
    auto max_key_2 = BinaryRowGenerator::GenerateRow(
        {Decimal(2, 2, DecimalUtils::StrToInt128("78").value()),
         TimestampType(Timestamp(123, 000000), 3), static_cast<int32_t>(24)},
        pool_.get());
    auto key_stats_2 = key_stats;
    key_stats_2.max_values_.SetInt(2, 245);

    auto value_stats_2 = value_stats;
    value_stats_2.max_values_.SetInt(4, 245);

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/4, min_key_2, max_key_2, key_stats_2, value_stats_2,
        /*min_sequence_number=*/6, /*max_sequence_number=*/9, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment_2, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_2};

    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(9, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot2.value().DeltaRecordCount().value());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 1);
    std::string expected_data_2 = R"([
        [3, [[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123", 245, "0.12"],
        [0, [[10, 11]], [1.1, 1.3], [true, 1], "1970-01-01 00:02:03.999", 24, "0.28"],
        [1, [[10, 20], [20, 30]], [2.0, 3.0], [true, 2], "1970-01-01 00:00:00.0", 24, "0.78"],
        [2, [[11, 61], [21, 31]], [2.1, 3.1], [false, 1], "1970-01-01 00:00:00.123", 24, "0.78"]
    ])";

    ASSERT_OK_AND_ASSIGN(success,
                         helper->ReadAndCheckResult(data_type, data_splits_2, expected_data_2));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestPkTableForceLookup) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
        {Options::FORCE_LOOKUP, "true"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> data;
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
    data.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
    data.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 120, 112.1));
    ASSERT_OK_AND_ASSIGN(auto batches, gen.SplitArrayByPartitionAndBucket(data));
    ASSERT_EQ(1, batches.size());

    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batches), commit_identifier++, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(4, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot1.value().DeltaRecordCount().value());

    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));
    ASSERT_EQ(data_splits.size(), 1);
    std::string expected_data = R"([
[0, "Cathy", "20250325", 120, 112.1], [0, "Evan", "20250326", 22, 14.1]
])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestPkTableEnableDeletionVector) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f0"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DELETION_VECTORS_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, partition_keys, primary_keys, options,
                                        /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    DataGenerator gen(table_schema.value(), pool_);
    std::vector<BinaryRow> data;
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
    data.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
    data.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
    data.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 120, 112.1));
    ASSERT_OK_AND_ASSIGN(auto batches, gen.SplitArrayByPartitionAndBucket(data));
    ASSERT_EQ(1, batches.size());

    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batches), commit_identifier++, std::nullopt));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(4, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot1.value().DeltaRecordCount().value());

    // read
    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt,
                                         /*is_streaming=*/false));
    ASSERT_TRUE(data_splits.empty());
}

TEST_P(WriteInteTest, TestPkTableWriteWithIOException) {
    ::testing::GTEST_FLAG(throw_on_failure) = true;
    // create table
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::utf8()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f0"},         {Options::FILE_SYSTEM, "local"},
    };
    bool run_complete = false;
    auto io_hook = IOHook::GetInstance();

    for (size_t i = 0; i < 500; i++) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
        CHECK_HOOK_STATUS(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/false), i);
        ::ArrowSchema c_schema;
        ScopeGuard arrow_guard([&c_schema]() { ArrowSchemaRelease(&c_schema); });
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        CHECK_HOOK_STATUS(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, partition_keys,
                                               primary_keys, options, /*ignore_if_exists=*/false),
                          i);
        std::string root_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
        SchemaManager schema_manger(file_system_, root_path);
        auto table_schema_result = schema_manger.ReadSchema(/*schema_id=*/0);
        CHECK_HOOK_STATUS(table_schema_result.status(), i);
        std::shared_ptr<TableSchema> table_schema = table_schema_result.value();

        // prepare data
        DataGenerator gen(table_schema, pool_);
        std::vector<BinaryRow> datas_1;
        datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Alex", "20250326", 18, 10.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Bob", "20250326", 19, 11.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Cathy", "20250325", 20, 12.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "David", "20250325", 21, 13.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Insert(), "Evan", "20250326", 22, 14.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Alex", "20250326", 18, 10.1));
        datas_1.push_back(MakeBinaryRow(RowKind::Delete(), "Bob", "20250326", 19, 11.1));
        ASSERT_OK_AND_ASSIGN(auto batches_1, gen.SplitArrayByPartitionAndBucket(datas_1));
        ASSERT_EQ(3, batches_1.size());

        std::vector<BinaryRow> datas_2;
        datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Farm", "20250326", 15, 22.1));
        datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Go", "20250325", 22, 23.1));
        datas_2.push_back(MakeBinaryRow(RowKind::UpdateAfter(), "David", "20250325", 22, 24.1));
        datas_2.push_back(MakeBinaryRow(RowKind::Insert(), "Hi", "20250325", 23, 24.1));
        ASSERT_OK_AND_ASSIGN(auto batches_2, gen.SplitArrayByPartitionAndBucket(datas_2));
        ASSERT_EQ(3, batches_2.size());

        // write data
        WriteContextBuilder context_builder(root_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             context_builder.SetOptions(options).WithStreamingMode(true).Finish());
        Result<std::unique_ptr<FileStoreWrite>> write =
            FileStoreWrite::Create(std::move(write_context));
        CHECK_HOOK_STATUS(write.status(), i);
        auto& file_store_write = write.value();
        // round 1
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_1[0])), i);
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_1[1])), i);
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_1[2])), i);
        Result<std::vector<std::shared_ptr<CommitMessage>>> results_1 =
            file_store_write->PrepareCommit(/*wait_compaction=*/false, 0);
        CHECK_HOOK_STATUS(results_1.status(), i);
        std::vector<std::shared_ptr<CommitMessage>> results_1_value = results_1.value();
        ASSERT_EQ(results_1_value.size(), 3);
        // round 2
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_2[0])), i);
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_2[1])), i);
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batches_2[2])), i);
        Result<std::vector<std::shared_ptr<CommitMessage>>> results_2 =
            file_store_write->PrepareCommit(/*wait_compaction=*/false, 1);
        CHECK_HOOK_STATUS(results_2.status(), i);
        std::vector<std::shared_ptr<CommitMessage>> results_2_value = results_2.value();
        ASSERT_EQ(results_2_value.size(), 4);
        io_hook->Clear();

        std::vector<std::string> subdirs = {"f1=20250325/bucket-0", "f1=20250325/bucket-1",
                                            "f1=20250326/bucket-0", "f1=20250326/bucket-1"};
        CheckFileCount(root_path, subdirs, /*expect_file_count=*/6);

        auto file_meta_1 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("David")}, {std::string("David")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("David"), std::string("20250325"), 21, 13.1},
                {std::string("David"), std::string("20250325"), 21, 13.1}, {0, 0, 0, 0},
                pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_1 = ReconstructDataFileMeta(file_meta_1);
        DataIncrement data_increment_1({file_meta_1}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_1 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")},
                                                                  pool_.get()),
                /*bucket=*/0,
                /*total_bucket=*/2, data_increment_1, CompactIncrement({}, {}, {}));

        auto file_meta_2 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Cathy")}, {std::string("Cathy")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("Cathy"), std::string("20250325"), 20, 12.1},
                {std::string("Cathy"), std::string("20250325"), 20, 12.1}, {0, 0, 0, 0},
                pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_2 = ReconstructDataFileMeta(file_meta_2);
        DataIncrement data_increment_2({file_meta_2}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_2 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")},
                                                                  pool_.get()),
                /*bucket=*/1,
                /*total_bucket=*/2, data_increment_2, CompactIncrement({}, {}, {}));

        auto file_meta_3 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/3,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alex")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Evan")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Alex")}, {std::string("Evan")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("Alex"), std::string("20250326"), 18, 10.1},
                {std::string("Evan"), std::string("20250326"), 22, 14.1}, {0, 0, 0, 0},
                pool_.get()),
            /*min_sequence_number=*/2, /*max_sequence_number=*/4, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/2, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_3 = ReconstructDataFileMeta(file_meta_3);
        DataIncrement data_increment_3({file_meta_3}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_3 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")},
                                                                  pool_.get()),
                /*bucket=*/1,
                /*total_bucket=*/2, data_increment_3, CompactIncrement({}, {}, {}));

        std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
            expected_commit_message_1, expected_commit_message_2, expected_commit_message_3};

        auto file_meta_4 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("David")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("David")}, {std::string("David")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("David"), std::string("20250325"), 22, 24.1},
                {std::string("David"), std::string("20250325"), 22, 24.1}, {0, 0, 0, 0},
                pool_.get()),
            /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_4 = ReconstructDataFileMeta(file_meta_4);
        DataIncrement data_increment_4({file_meta_4}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_4 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")},
                                                                  pool_.get()),
                /*bucket=*/0,
                /*total_bucket=*/2, data_increment_4, CompactIncrement({}, {}, {}));

        auto file_meta_5 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/2,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Go")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Hi")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Go")}, {std::string("Hi")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("Go"), std::string("20250325"), 22, 23.1},
                {std::string("Hi"), std::string("20250325"), 23, 24.1}, {0, 0, 0, 0}, pool_.get()),
            /*min_sequence_number=*/1, /*max_sequence_number=*/2, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_5 = ReconstructDataFileMeta(file_meta_5);
        DataIncrement data_increment_5({file_meta_5}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_5 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250325")},
                                                                  pool_.get()),
                /*bucket=*/1,
                /*total_bucket=*/2, data_increment_5, CompactIncrement({}, {}, {}));

        auto file_meta_6 = std::make_shared<DataFileMeta>(
            "data-xxx.xxx", /*file_size=*/543,
            /*row_count=*/1,
            /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Farm")}, pool_.get()),
            /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Farm")}, pool_.get()),
            /*key_stats=*/
            BinaryRowGenerator::GenerateStats({std::string("Farm")}, {std::string("Farm")}, {0},
                                              pool_.get()),
            /*value_stats=*/
            BinaryRowGenerator::GenerateStats(
                {std::string("Farm"), std::string("20250326"), 15, 22.1},
                {std::string("Farm"), std::string("20250326"), 15, 22.1}, {0, 0, 0, 0},
                pool_.get()),
            /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
            /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(1724090888706ll, 0),
            /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
            /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
            /*first_row_id=*/std::nullopt,
            /*write_cols=*/std::nullopt);
        file_meta_6 = ReconstructDataFileMeta(file_meta_6);
        DataIncrement data_increment_6({file_meta_6}, {}, {});
        std::shared_ptr<CommitMessage> expected_commit_message_6 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")},
                                                                  pool_.get()),
                /*bucket=*/0,
                /*total_bucket=*/2, data_increment_6, CompactIncrement({}, {}, {}));

        std::shared_ptr<CommitMessage> expected_commit_message_7 =
            std::make_shared<CommitMessageImpl>(
                /*partition_map=*/BinaryRowGenerator::GenerateRow({std::string("20250326")},
                                                                  pool_.get()),
                /*bucket=*/1,
                /*total_bucket=*/2, DataIncrement({}, {}, {}), CompactIncrement({}, {}, {}));

        std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
            expected_commit_message_4, expected_commit_message_5, expected_commit_message_6,
            expected_commit_message_7};

        TestHelper::CheckCommitMessages(expected_commit_messages_1, results_1_value);
        TestHelper::CheckCommitMessages(expected_commit_messages_2, results_2_value);
        run_complete = true;
        break;
    }
    ASSERT_TRUE(run_complete);
}

TEST_F(WriteInteTest, TestAppendTableWriteWithAlterTable) {
    std::string test_data_path =
        paimon::test::GetDataDir() +
        "/orc/append_table_with_alter_table.db/append_table_with_alter_table/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    arrow::FieldVector fields = {
        arrow::field("key0", arrow::int32()), arrow::field("key1", arrow::int32()),
        arrow::field("k", arrow::int32()),    arrow::field("c", arrow::int32()),
        arrow::field("d", arrow::int32()),    arrow::field("a", arrow::int32()),
        arrow::field("e", arrow::int32()),
    };
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(table_path, options, /*is_streaming_mode=*/true));
    // scan with empty split
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> empty_splits,
                         helper->NewScan(StartupMode::Latest(), /*snapshot_id=*/std::nullopt));
    ASSERT_TRUE(empty_splits.empty());

    int64_t commit_identifier = 0;
    auto data_type = arrow::struct_(fields);
    std::string data = R"([[1, 1, 116, 113, 567, 115, 668]])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(data_type, data, {{"key0", "1"}, {"key1", "1"}}, /*bucket=*/0,
                                    /*row_kinds=*/{}));
    // for append only unaware bucket table, previous files will be ignored
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({1, 1, 116, 113, 567, 115, 668},
                                          {1, 1, 116, 113, 567, 115, 668}, {0, 0, 0, 0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({1, 1}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/-1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages = {
        expected_commit_message};
    ASSERT_OK(
        helper->WriteAndCommit(std::move(batch), commit_identifier++, expected_commit_messages));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);

    ASSERT_EQ(1, snapshot.value().SchemaId());
    ASSERT_EQ(3, snapshot.value().Id());

    // read
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits, helper->Scan());
    ASSERT_EQ(data_splits.size(), 1);

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type_with_row_kind = arrow::struct_(fields_with_row_kind);
    std::string expected_data = R"([[0, 1, 1, 116, 113, 567, 115, 668]])";
    ASSERT_OK_AND_ASSIGN(bool success, helper->ReadAndCheckResult(data_type_with_row_kind,
                                                                  data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_F(WriteInteTest, TestPKTableWriteWithAlterTable) {
    std::string test_data_path =
        paimon::test::GetDataDir() + "/orc/pk_table_with_mor.db/pk_table_with_mor/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));
    arrow::FieldVector fields = {
        arrow::field("k1", arrow::int32(), /*nullable=*/false),
        arrow::field("k0", arrow::int32(), /*nullable=*/false),
        arrow::field("p0", arrow::int32(), /*nullable=*/false),
        arrow::field("p1", arrow::int32(), /*nullable=*/false),
        arrow::field("s1", arrow::utf8()),
        arrow::field("s0", arrow::binary()),
        arrow::field("v0", arrow::int32()),
        arrow::field("v1", arrow::utf8()),
        arrow::field("v2", arrow::int32()),
    };
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper,
                         TestHelper::Create(table_path, options, /*is_streaming_mode=*/true));

    int64_t commit_identifier = 0;
    auto data_type = arrow::struct_({fields});
    std::string data = R"([
        [0, 0, 0, 0, "apple", "see", 210, "new", 210]
    ])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch,
        TestHelper::MakeRecordBatch(data_type, data, {{"p0", "0"}, {"p1", "0"}}, /*bucket=*/0,
                                    /*row_kinds=*/{}));

    auto min_key = BinaryRowGenerator::GenerateRow({0, 0}, pool_.get());
    auto max_key = BinaryRowGenerator::GenerateRow({0, 0}, pool_.get());
    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/1, min_key, max_key,
        BinaryRowGenerator::GenerateStats({0, 0}, {0, 0}, {0, 0}, pool_.get()),
        BinaryRowGenerator::GenerateStats(
            {0, 0, 0, 0, std::string("apple"), NullType(), 210, std::string("new"), 210},
            {0, 0, 0, 0, std::string("apple"), NullType(), 210, std::string("new"), 210},
            {0, 0, 0, 0, 0, 0, 0, 0, 0}, pool_.get()),
        /*min_sequence_number=*/9, /*max_sequence_number=*/9, /*schema_id=*/4,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({0, 0}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages = {
        expected_commit_message};

    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++, expected_commit_messages));

    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(4, snapshot.value().SchemaId());

    auto impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
    // check sequence_number in commit msg
    ASSERT_EQ(impl->data_increment_.new_files_[0]->min_sequence_number, 9);
    ASSERT_EQ(impl->data_increment_.new_files_[0]->max_sequence_number, 9);
    ASSERT_EQ(4, impl->data_increment_.new_files_[0]->schema_id);
    // check sequence_number in data file
    std::string file_name =
        table_path + "/p0=0/p1=0/bucket-0/" + impl->data_increment_.new_files_[0]->file_name;
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("orc", options));
    ASSERT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(/*batch_size=*/10));
    ASSERT_OK_AND_ASSIGN(auto orc_batch_reader, reader_builder->Build(input_stream));
    arrow::FieldVector read_fields;
    read_fields.push_back(arrow::field("_SEQUENCE_NUMBER", arrow::int64()));
    read_fields.push_back(arrow::field("_VALUE_KIND", arrow::int8()));
    auto read_schema = arrow::schema(read_fields);
    std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
    auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
    ASSERT_TRUE(arrow_status.ok());
    ASSERT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                              /*selection_bitmap=*/std::nullopt));
    ASSERT_OK_AND_ASSIGN(auto result_array,
                         ReadResultCollector::CollectResult(orc_batch_reader.get()));
    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(read_fields), {R"([
        [9, 0]
    ])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->Equals(*result_array));
}

TEST_P(WriteInteTest, TestWriteAndCommitIOException) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
    };

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1"},
                                         /*primary_keys=*/{}, options));

    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    bool write_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        WriteContextBuilder context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             context_builder.SetOptions(options).WithStreamingMode(true).Finish());

        Result<std::unique_ptr<FileStoreWrite>> write =
            FileStoreWrite::Create(std::move(write_context));
        CHECK_HOOK_STATUS(write.status(), i);
        auto& file_store_write = write.value();

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                             MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batch_0)), i);
        Result<std::vector<std::shared_ptr<CommitMessage>>> results =
            file_store_write->PrepareCommit(/*wait_compaction=*/false, 0);
        CHECK_HOOK_STATUS(results.status(), i);
        commit_messages = results.value();
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);

    bool commit_run_complete = false;
    for (size_t i = 0; i < 400; i += paimon::test::RandomNumber(5, 15)) {
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                .AddOption(Options::FILE_SYSTEM, "local")
                .IgnoreEmptyCommit(false)
                .Finish());
        auto commit = FileStoreCommit::Create(std::move(commit_context));
        CHECK_HOOK_STATUS(commit.status(), i);
        Result<int32_t> committed = commit.value()->FilterAndCommit({{0, commit_messages}});
        CHECK_HOOK_STATUS(committed.status(), i);
        commit_run_complete = true;
        break;
    }
    ASSERT_TRUE(commit_run_complete);

    std::vector<int64_t> actual_snapshots;
    ASSERT_OK(FileUtils::ListVersionedFiles(file_system_,
                                            PathUtil::JoinPath(table_path, "snapshot"),
                                            SnapshotManager::SNAPSHOT_PREFIX, &actual_snapshots));
    std::vector<int64_t> expected_snapshots = {1};
    ASSERT_EQ(actual_snapshots, expected_snapshots);
}

TEST_P(WriteInteTest, TestWriteWithFieldId) {
    auto file_format = GetParam();
    if (file_format == "lance" || file_format == "avro") {
        return;
    }
    // prepare write schema and write data
    auto map_type = arrow::map(arrow::int8(), arrow::int16());
    auto list_type = arrow::list(DataField::ConvertDataFieldToArrowField(
        DataField(536871936, arrow::field("item", arrow::float32()))));
    std::vector<DataField> struct_fields = {DataField(3, arrow::field("f0", arrow::boolean())),
                                            DataField(4, arrow::field("f1", arrow::int64()))};
    auto struct_type = DataField::ConvertDataFieldsToArrowStructType(struct_fields);
    std::vector<DataField> data_fields = {
        DataField(0, arrow::field("f1", map_type)),
        DataField(1, arrow::field("f2", list_type)),
        DataField(2, arrow::field("f3", struct_type)),
        DataField(5, arrow::field("f4", arrow::timestamp(arrow::TimeUnit::NANO))),
        DataField(6, arrow::field("f5", arrow::date32())),
        DataField(7, arrow::field("f6", arrow::decimal128(2, 2)))};
    std::shared_ptr<arrow::DataType> arrow_data_type =
        DataField::ConvertDataFieldsToArrowStructType(data_fields);
    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow_data_type, R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], "1970-01-01 00:02:03.123123", 2456, "0.22"],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], "1970-01-01 00:02:03.123123",
        245, "0.12"],
        [[[1, 64], [2, 32]], [2.2, 3.2], [true, 2], "1970-01-01 00:00:00.0", 24, null]
    ])")
            .ValueOrDie());

    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportType(*arrow_data_type, &c_schema).ok());
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
    };

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK_AND_ASSIGN(std::string table_path, CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                                                 /*table_name=*/"bar", &c_schema,
                                                                 /*partition_keys=*/{},
                                                                 /*primary_keys=*/{}, options));

    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).Finish());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));
    ::ArrowArray c_array;
    ASSERT_TRUE(arrow::ExportArray(*src_array, &c_array).ok());
    RecordBatchBuilder batch_builder(&c_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());
    ASSERT_OK(file_store_write->Write(std::move(batch)));

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                         file_store_write->PrepareCommit());
    ASSERT_OK(file_store_write->Close());

    // prepare CommitContext
    CommitContextBuilder commit_context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    // commit
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(commit_messages));

    // check data file has field id meta
    std::vector<std::unique_ptr<BasicFileStatus>> status_list;
    ASSERT_OK(file_system_->ListDir(table_path + "/bucket-0/", &status_list));
    std::vector<std::string> files;
    for (const auto& file_status : status_list) {
        if (!file_status->IsDir()) {
            files.emplace_back(file_status->GetPath());
        }
    }
    ASSERT_EQ(files.size(), 1);
    std::string file_name = files[0];
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream, file_system_->Open(file_name));
    ASSERT_OK_AND_ASSIGN(auto file_format_impl,
                         FileFormatFactory::Get(file_format, /*options=*/{}));
    ASSERT_OK_AND_ASSIGN(auto reader_builder,
                         file_format_impl->CreateReaderBuilder(/*batch_size=*/1024));
    ASSERT_OK_AND_ASSIGN(auto file_batch_reader,
                         reader_builder->WithMemoryPool(pool_)->Build(input_stream));
    ASSERT_OK_AND_ASSIGN(auto c_file_schema, file_batch_reader->GetFileSchema());
    auto arrow_file_schema = arrow::ImportSchema(c_file_schema.get()).ValueOrDie();
    ASSERT_OK_AND_ASSIGN(auto write_data_fields,
                         DataField::ConvertArrowSchemaToDataFields(arrow_file_schema));
    ASSERT_EQ(write_data_fields, data_fields);
}

TEST_P(WriteInteTest, TestAppendTableWriteAndReadWithExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = "FILE://" + external_dir->Str();
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DATA_FILE_EXTERNAL_PATHS, external_test_dir},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    // create table
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"append_table_with_external_path",
                                         /*table_name=*/"append_table_with_external_path", &schema,
                                         /*partition_keys=*/{"f1"}, /*primary_keys=*/{}, options));
    // write
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).Finish());
    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])")
            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         batch_builder.SetPartition({{"f1", "10"}}).SetBucket(0).Finish());
    ASSERT_OK(file_store_write->Write(std::move(batch)));
    ArrowArrayRelease(&arrow_array);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit());
    ASSERT_EQ(results.size(), 1);
    auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(results[0]);
    auto meta = commit_msg_impl->data_increment_.new_files_[0];
    CommitContextBuilder commit_context_builder(root_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 1));

    // check external path
    ASSERT_OK_AND_ASSIGN(bool file_exist, file_system_->Exists(meta->external_path.value()));
    ASSERT_TRUE(file_exist);

    // check read result
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
        [0, "Alice", 10, 0, 11.1],
        [0, "Bob", 10, 1, 12.1],
        [0, "Cathy", 10, 0, 13.1],
        [0, "Emily", 10, 0, 14.1]
    ])")
            .ValueOrDie();
    auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(WriteInteTest, TestPKTableWriteAndReadWithExternalPath) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = "FILE://" + external_dir->Str();
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::BUCKET, "1"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DATA_FILE_EXTERNAL_PATHS, external_test_dir},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};
    // create table
    ASSERT_OK_AND_ASSIGN(
        std::string table_path,
        CreateTestTable(dir->Str(), /*db_name=*/"pk_table_with_external_path",
                        /*table_name=*/"pk_table_with_external_path", &schema,
                        /*partition_keys=*/{"f1"}, /*primary_keys=*/{"f0", "f1"}, options));
    // write
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).Finish());
    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
        ["Alice", 10, 0, 11.1],
        ["Bob", 10, 1, 12.1],
        ["Cathy", 10, 0, 13.1],
        ["Emily", 10, 0, 14.1]
    ])")
            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         batch_builder.SetPartition({{"f1", "10"}}).SetBucket(0).Finish());
    ASSERT_OK(file_store_write->Write(std::move(batch)));
    ArrowArrayRelease(&arrow_array);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit());
    ASSERT_EQ(results.size(), 1);
    auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(results[0]);
    auto meta = commit_msg_impl->data_increment_.new_files_[0];

    // check external path
    std::string external_path = meta->external_path.value();
    ASSERT_OK_AND_ASSIGN(bool file_exist, file_system_->Exists(external_path));
    ASSERT_TRUE(file_exist);
    ASSERT_TRUE(external_path.find("tmp") != std::string::npos);
}

TEST_P(WriteInteTest, TestAppendTableStreamWriteWithExternalPath) {
    auto dir = UniqueTestDirectory::Create();
    auto external_dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(external_dir);
    std::string external_test_dir = "FILE://" + external_dir->Str();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),       arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),         arrow::field("f3", arrow::int32()),
        arrow::field("field_null", arrow::int32()), arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),       arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),          arrow::field("f8", arrow::binary())};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "1"},
        {Options::BUCKET_KEY, "f5"},
        {Options::FILE_SYSTEM, "local"},
        {Options::DATA_FILE_EXTERNAL_PATHS, external_test_dir},
        {Options::DATA_FILE_EXTERNAL_PATHS_STRATEGY, "round-robin"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    std::string data_1 =
        R"([[true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {false, static_cast<int8_t>(-2), static_cast<int16_t>(-32768),
             static_cast<int32_t>(-2147483648), NullType(), static_cast<int64_t>(-4294967298),
             static_cast<float>(0.5), 1.141592659, std::string("20250326"), NullType()},
            {true, static_cast<int8_t>(1), static_cast<int16_t>(32767),
             static_cast<int32_t>(2147483647), NullType(), static_cast<int64_t>(4294967296),
             static_cast<float>(2.0), 3.141592657, std::string("20250327"), NullType()},
            std::vector<int64_t>({1, 0, 0, 1, 4, 1, 0, 0, 1, 0}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/3, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, "FILE:/tmp/xxx", std::nullopt, std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(4, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(4, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    std::string expected_data_1 =
        R"([[0, true, 0, 32767, 2147483647, null, 4294967295, 0.5, 1.141592659, "20250327", "banana"],
            [0, false, 1, 32767, null, null, 4294967296, 1.0, 2.141592658, "20250327", "dog"],
            [0, null, 1, 32767, 2147483647, null, null, 2.0, 3.141592657, null, "lucy"],
            [0, true, -2, -32768, -2147483648, null, -4294967298, 2.0, 3.141592657, "20250326", "mouse"]])";

    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);

    std::string data_2 =
        fmt::format(R"([
                   [true, {},{},{},null,{},{},{},"∞",""],
                   [false,{},{},{},null,{},{},{}, "",""],
                   [true, 42,-1,999999999,null,123456789012345,0.1,-Inf,"a\u20ACb","binary\ndata"]])",
                    std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<float>::min(), std::numeric_limits<double>::min(),
                    std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<float>::max(), std::numeric_limits<double>::max());

    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_2,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_2,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));

    auto file_meta_2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {false, std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
             std::numeric_limits<int32_t>::min(), NullType(), std::numeric_limits<int64_t>::min(),
             std::numeric_limits<float>::min(), -std::numeric_limits<double>::infinity(),
             std::string(""), NullType()},
            {true, std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
             std::numeric_limits<int32_t>::max(), NullType(), std::numeric_limits<int64_t>::max(),
             std::numeric_limits<float>::max(), std::numeric_limits<double>::max(),
             std::string("∞"), NullType()},
            std::vector<int64_t>({0, 0, 0, 0, 3, 0, 0, 0, 0, 0}), pool_.get()),
        /*min_sequence_number=*/4, /*max_sequence_number=*/6, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, "FILE:/tmp/xxx", std::nullopt, std::nullopt);
    file_meta_2 = ReconstructDataFileMeta(file_meta_2);
    DataIncrement data_increment_2({file_meta_2}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_2 = std::make_shared<CommitMessageImpl>(
        BinaryRow::EmptyRow(), /*bucket=*/0,
        /*total_bucket=*/1, data_increment_2, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_2 = {
        expected_commit_message_2};
    ASSERT_OK_AND_ASSIGN(auto commit_msgs_2,
                         helper->WriteAndCommit(std::move(batch_2), commit_identifier++,
                                                expected_commit_messages_2));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot2, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot2);
    ASSERT_EQ(2, snapshot2.value().Id());
    ASSERT_EQ(7, snapshot2.value().TotalRecordCount().value());
    ASSERT_EQ(3, snapshot2.value().DeltaRecordCount().value());
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_2, helper->Scan());
    ASSERT_EQ(data_splits_2.size(), 1);
    std::string expected_data_2 =
        fmt::format(R"([
                   [0, true, {},{},{},null,{},{},{},"∞",""],
                   [0, false,{},{},{},null,{},{},{}, "",""],
                   [0, true, 42,-1,999999999,null,123456789012345,0.1,-Inf,"a\u20ACb","binary\ndata"]])",
                    std::numeric_limits<int8_t>::min(), std::numeric_limits<int16_t>::min(),
                    std::numeric_limits<int32_t>::min(), std::numeric_limits<int64_t>::min(),
                    std::numeric_limits<float>::min(), std::numeric_limits<double>::min(),
                    std::numeric_limits<int8_t>::max(), std::numeric_limits<int16_t>::max(),
                    std::numeric_limits<int32_t>::max(), std::numeric_limits<int64_t>::max(),
                    std::numeric_limits<float>::max(), std::numeric_limits<double>::max());

    ASSERT_OK_AND_ASSIGN(success,
                         helper->ReadAndCheckResult(data_type, data_splits_2, expected_data_2));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestWriteAndReadWithSpecialPartitionValue) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),  arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64()),
        arrow::field("dt", arrow::utf8()),  arrow::field("hr", arrow::utf8())};

    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"}};
    // create table
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &schema,
                                         /*partition_keys=*/{"dt", "f1", "hr"},
                                         /*primary_keys=*/{}, options));

    // write
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).Finish());
    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));
    auto write = [&](const std::string& json_array,
                     const std::map<std::string, std::string>& partition) -> void {
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), json_array)
                .ValueOrDie();
        ::ArrowArray arrow_array;
        ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

        RecordBatchBuilder batch_builder(&arrow_array);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                             batch_builder.SetPartition(partition).SetBucket(0).Finish());
        ASSERT_OK(file_store_write->Write(std::move(batch)));
        ArrowArrayRelease(&arrow_array);
    };

    write(R"([["Alice", 10, 0, 11.1, " ", "a=b?"]])", {{"f1", "10"}, {"dt", " "}, {"hr", "a=b?"}});
    write(R"([["Bob", 10, 0, 12.1, "", "a=b?"]])", {{"f1", "10"}, {"dt", ""}, {"hr", "a=b?"}});
    write(R"([["Cathy", 10, 0, 13.1, "__DEFAULT_PARTITION__", "a=b?"]])",
          {{"f1", "10"}, {"dt", "__DEFAULT_PARTITION__"}, {"hr", "a=b?"}});
    write(R"([["David", 10, 0, 13.1, null, "a=b?"]])",
          {{"f1", "10"}, {"dt", "__DEFAULT_PARTITION__"}, {"hr", "a=b?"}});

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit());
    ASSERT_EQ(results.size(), 3);
    auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(results[0]);
    auto meta = commit_msg_impl->data_increment_.new_files_[0];
    CommitContextBuilder commit_context_builder(root_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 1));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        // NOTE:
        // Users should not use the system-reserved keyword "__DEFAULT_PARTITION__" as a partition
        // value. If used, it may lead to behavioral inconsistencies between C++ Paimon and Java
        // Paimon.
        //
        // In Java Paimon, "__DEFAULT_PARTITION__" is stored as-is in the `Partition` field of
        // `DataFileMeta`. In contrast, C++ Paimon treats "__DEFAULT_PARTITION__" as a placeholder
        // for null and stores a null value in `DataFileMeta`'s partition field.
        //
        // Additionally, if users specify "__DEFAULT_PARTITION__" as a PartitionFilter during a
        // scan, Java Paimon will NOT be able to retrieve the corresponding record, whereas C++
        // Paimon WILL retrieve it and treat the partition value as null.
        //
        // To ensure consistent behavior across platforms, avoid using "__DEFAULT_PARTITION__" in
        // data.
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
[0, "Cathy", 10, 0, 13.1, null, "a=b?"],
[0, "David", 10, 0, 13.1, null, "a=b?"],
[0, "Bob", 10, 0, 12.1, "", "a=b?"],
[0, "Alice", 10, 0, 11.1, " ", "a=b?"]
])")
                .ValueOrDie();
        auto expected = std::make_shared<arrow::ChunkedArray>(array);
        ASSERT_TRUE(expected);
        auto schema = std::make_shared<arrow::Schema>(fields_with_row_kind);
        ASSERT_OK_AND_ASSIGN(expected, ReadResultCollector::SortArray(expected, schema));
        if (read_result && read_result->length() > 0) {
            ASSERT_OK_AND_ASSIGN(read_result, ReadResultCollector::SortArray(read_result, schema));
        }
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    }
    {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        std::vector<std::map<std::string, std::string>> partition_filters = {
            {{"dt", " "}, {"hr", "a=b?"}}};
        ASSERT_OK_AND_ASSIGN(auto scan_context,
                             scan_context_builder.SetPartitionFilter(partition_filters).Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
[0, "Alice", 10, 0, 11.1, " ", "a=b?"]
])")
                .ValueOrDie();
        auto expected = std::make_shared<arrow::ChunkedArray>(array);
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    }
    {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        std::vector<std::map<std::string, std::string>> partition_filters = {
            {{"dt", ""}, {"hr", "a=b?"}}};
        ASSERT_OK_AND_ASSIGN(auto scan_context,
                             scan_context_builder.SetPartitionFilter(partition_filters).Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
[0, "Bob", 10, 0, 12.1, "", "a=b?"]
])")
                .ValueOrDie();
        auto expected = std::make_shared<arrow::ChunkedArray>(array);
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    }
    {
        ScanContextBuilder scan_context_builder(table_path);
        scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        std::vector<std::map<std::string, std::string>> partition_filters = {
            {{"dt", "__DEFAULT_PARTITION__"}, {"hr", "a=b?"}}};
        ASSERT_OK_AND_ASSIGN(auto scan_context,
                             scan_context_builder.SetPartitionFilter(partition_filters).Finish());
        ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

        ReadContextBuilder read_context_builder(table_path);
        read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
        ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
        ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

        ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
        ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
        ASSERT_OK_AND_ASSIGN(auto read_result,
                             ReadResultCollector::CollectResult(batch_reader.get()));
        std::shared_ptr<arrow::Array> array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
[0, "Cathy", 10, 0, 13.1, null, "a=b?"],
[0, "David", 10, 0, 13.1, null, "a=b?"]
])")
                .ValueOrDie();
        auto expected = std::make_shared<arrow::ChunkedArray>(array);
        ASSERT_TRUE(expected);
        ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
    }
}

TEST_P(WriteInteTest, TestWriteWithNestedSchema) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::struct_({arrow::field("v0", arrow::boolean()),
                                           arrow::field("v1", arrow::int64())}))};
    arrow::Schema typed_schema(fields);
    ::ArrowSchema schema;
    ASSERT_TRUE(arrow::ExportSchema(typed_schema, &schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::FILE_SYSTEM, "local"}};
    // create table
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &schema,
                                         /*partition_keys=*/{}, /*primary_keys=*/{}, options));
    // write
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).Finish());
    std::string root_path = write_context->GetRootPath();
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    std::shared_ptr<arrow::Array> array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields}), R"([
            [[true, 2]],
            [null],
            [[false, 22]],
            [null]
    ])")
            .ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         batch_builder.SetPartition({}).SetBucket(0).Finish());
    ASSERT_OK(file_store_write->Write(std::move(batch)));
    ArrowArrayRelease(&arrow_array);

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results,
                         file_store_write->PrepareCommit());
    ASSERT_EQ(results.size(), 1);
    CommitContextBuilder commit_context_builder(root_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .IgnoreEmptyCommit(false)
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    ASSERT_OK(commit->Commit(results, 1));

    // check read result
    ScanContextBuilder scan_context_builder(table_path);
    scan_context_builder.AddOption(Options::FILE_FORMAT, file_format);
    ASSERT_OK_AND_ASSIGN(auto scan_context, scan_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_scan, TableScan::Create(std::move(scan_context)));

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.AddOption(Options::FILE_FORMAT, file_format);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto result_plan, table_scan->CreatePlan());
    ASSERT_OK_AND_ASSIGN(auto batch_reader, table_read->CreateReader(result_plan->Splits()));
    ASSERT_OK_AND_ASSIGN(auto read_result, ReadResultCollector::CollectResult(batch_reader.get()));

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    std::shared_ptr<arrow::Array> expected_array =
        arrow::ipc::internal::json::ArrayFromJSON(data_type, R"([
            [0, [true, 2]],
            [0, null],
            [0, [false, 22]],
            [0, null]
    ])")
            .ValueOrDie();
    auto expected = std::make_shared<arrow::ChunkedArray>(expected_array);
    ASSERT_TRUE(expected->Equals(read_result)) << read_result->ToString();
}

TEST_P(WriteInteTest, TestWriteWithIOException) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
    };

    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    bool write_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 200; i++) {
        auto dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(dir);
        ::ArrowSchema arrow_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
        ASSERT_OK_AND_ASSIGN(std::string table_path,
                             CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                             /*table_name=*/"bar", &arrow_schema,
                                             /*partition_keys=*/{"f1"},
                                             /*primary_keys=*/{}, options));
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);

        WriteContextBuilder context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             context_builder.SetOptions(options).WithStreamingMode(true).Finish());

        Result<std::unique_ptr<FileStoreWrite>> write =
            FileStoreWrite::Create(std::move(write_context));
        CHECK_HOOK_STATUS(write.status(), i);
        auto& file_store_write = write.value();

        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                             MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
        CHECK_HOOK_STATUS(file_store_write->Write(std::move(batch_0)), i);
        Result<std::vector<std::shared_ptr<CommitMessage>>> results =
            file_store_write->PrepareCommit(/*wait_compaction=*/false, 0);
        CHECK_HOOK_STATUS(results.status(), i);
        commit_messages = results.value();
        write_run_complete = true;
        break;
    }
    ASSERT_TRUE(write_run_complete);
    ASSERT_EQ(commit_messages.size(), 1);
}

TEST_P(WriteInteTest, TestCommitWithIOException) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
    };

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1"},
                                         /*primary_keys=*/{}, options));
    // write data
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithStreamingMode(true).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));

    // commit data
    bool commit_run_complete = false;
    auto io_hook = IOHook::GetInstance();
    for (size_t i = 0; i < 400; i++) {
        auto tmp_dir = UniqueTestDirectory::Create();
        ASSERT_TRUE(TestUtil::CopyDirectory(table_path, tmp_dir->Str()));
        std::string tmp_table_path = tmp_dir->Str();
        ScopeGuard guard([&io_hook]() { io_hook->Clear(); });
        io_hook->Reset(i, IOHook::Mode::RETURN_ERROR);
        CommitContextBuilder commit_context_builder(tmp_table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(
            std::unique_ptr<CommitContext> commit_context,
            commit_context_builder.AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                .AddOption(Options::FILE_SYSTEM, "local")
                .IgnoreEmptyCommit(false)
                .Finish());
        auto commit = FileStoreCommit::Create(std::move(commit_context));
        CHECK_HOOK_STATUS(commit.status(), i);
        Result<int32_t> committed = commit.value()->FilterAndCommit({{0, commit_messages}});
        CHECK_HOOK_STATUS(committed.status(), i);
        commit_run_complete = true;
        // when commit complete, check snapshot exist
        io_hook->Clear();
        std::vector<int64_t> actual_snapshots;
        ASSERT_OK(FileUtils::ListVersionedFiles(
            file_system_, PathUtil::JoinPath(tmp_table_path, "snapshot"),
            SnapshotManager::SNAPSHOT_PREFIX, &actual_snapshots));
        std::vector<int64_t> expected_snapshots = {1};
        ASSERT_EQ(actual_snapshots, expected_snapshots);
        break;
    }
    ASSERT_TRUE(commit_run_complete);
}

TEST_P(WriteInteTest, TestWriteMemoryUse) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto struct_type = arrow::struct_({string_field, int_field, int_field1, double_field});
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, file_format},
        {Options::MANIFEST_FORMAT, "orc"},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::FILE_SYSTEM, "local"},
        {Options::BUCKET, "2"},
        {Options::BUCKET_KEY, "f2"},
    };

    std::vector<std::shared_ptr<CommitMessage>> commit_messages;
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::string table_path,
                         CreateTestTable(dir->Str(), /*db_name=*/"foo",
                                         /*table_name=*/"bar", &arrow_schema,
                                         /*partition_keys=*/{"f1"},
                                         /*primary_keys=*/{}, options));
    std::shared_ptr<MemoryPool> write_pool = GetMemoryPool();
    {
        WriteContextBuilder context_builder(table_path, "commit_user_1");
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                             context_builder.SetOptions(options)
                                 .WithMemoryPool(write_pool)
                                 .WithStreamingMode(true)
                                 .Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                             FileStoreWrite::Create(std::move(write_context)));
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                             MakeRecordBatch({{"Alice", 10, 1, 11.1}}, {{"f1", "10"}}, 0));
        ASSERT_OK(file_store_write->Write(std::move(batch_0)));
        ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> commit_messages,
                             file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
        ASSERT_EQ(commit_messages.size(), 1);
    }
    // check all memory is released
    ASSERT_GT(write_pool->MaxMemoryUsage(), 0);
    ASSERT_EQ(write_pool->CurrentUsage(), 0);
}

TEST_P(WriteInteTest, TestAppendTableWithAllNull) {
    if (GetParam() == "lance") {
        // lance do not support map
        return;
    }
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),
        arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()),
        arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()),
        arrow::field("f5", arrow::float32()),
        arrow::field("f6", arrow::float64()),
        arrow::field("f7", arrow::utf8()),
        arrow::field("f8", arrow::binary()),
        arrow::field("f9", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f10", arrow::list(arrow::float32())),
        arrow::field("f11", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                            arrow::field("f1", arrow::int64())})),
        arrow::field("f12", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f13", arrow::date32()),
        arrow::field("f14", arrow::decimal128(2, 2)),
        arrow::field("f15", arrow::decimal128(30, 2))};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},   {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "1024"}, {Options::BUCKET, "-1"},
        {Options::FILE_SYSTEM, "local"},
    };

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;

    std::string data =
        R"([[null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++, std::nullopt));
    ASSERT_EQ(commit_msgs.size(), 1);
    auto msg = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs[0]);
    ASSERT_EQ(msg->data_increment_.new_files_.size(), 1);
    auto stats = msg->data_increment_.new_files_[0]->value_stats;

    // test compatible with java
    ASSERT_EQ(stats.min_values_.HashCode(), 0xc7883013);
    ASSERT_EQ(stats.max_values_.HashCode(), 0xc7883013);
    if (GetParam() != "parquet" && GetParam() != "avro") {
        ASSERT_EQ(stats.null_counts_.HashCode(), 0x5ddc482d);
    }
}

TEST_P(WriteInteTest, TestPkTablePostponeBucket) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()), arrow::field("f1", arrow::int32()),
        arrow::field("f2", arrow::int32()), arrow::field("f3", arrow::float64())};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "-2"},
                                                  {Options::FILE_SYSTEM, "local"}};
    ASSERT_OK_AND_ASSIGN(auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                                         primary_keys, options,
                                                         /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(std::optional<std::shared_ptr<TableSchema>> table_schema,
                         helper->LatestSchema());
    ASSERT_TRUE(table_schema);

    std::string data = R"([
        ["Alice", 50, 0, 11.1],
        ["Paul", 20, 1, 12.1],
        ["Cathy", 30, 0, 13.1],
        ["Emily", 5, 0, 14.1],
        ["Cathy", 30, 0, 13.1]
    ])";
    // test not specified bucket id in batch
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(
                             arrow::struct_(fields), data,
                             /*partition_map=*/{}, /*bucket=*/std::numeric_limits<int32_t>::min(),
                             {RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::INSERT, RecordBatch::RowKind::INSERT,
                              RecordBatch::RowKind::DELETE}));

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543,
        /*row_count=*/5,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("Alice"), 50}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("Cathy"), 30}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({NullType(), NullType()}, {NullType(), NullType()},
                                          {-1, -1}, pool_.get()),
        /*value_stats=*/
        SimpleStats::EmptyStats(),
        /*min_sequence_number=*/-1, /*max_sequence_number=*/-1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/1, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message = std::make_shared<CommitMessageImpl>(
        /*partition_map=*/BinaryRow::EmptyRow(), /*bucket=*/-2,
        /*total_bucket=*/-2, data_increment, CompactIncrement({}, {}, {}));

    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages = {
        expected_commit_message};

    ASSERT_OK_AND_ASSIGN(
        auto commit_msgs,
        helper->WriteAndCommit(std::move(batch), commit_identifier++, expected_commit_messages));
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(5, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(5, snapshot1.value().DeltaRecordCount().value());

    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 1);

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    std::string expected_data = R"([
        [0, "Alice", 50, 0, 11.1],
        [0, "Paul", 20, 1, 12.1],
        [0, "Cathy", 30, 0, 13.1],
        [0, "Emily", 5, 0, 14.1],
        [3, "Cathy", 30, 0, 13.1]
    ])";

    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits, expected_data));
    ASSERT_TRUE(success);
}

TEST_F(WriteInteTest, TestBranchWrite) {
    arrow::FieldVector fields = {arrow::field("dt", arrow::utf8()),
                                 arrow::field("name", arrow::utf8()),
                                 arrow::field("amount", arrow::int32())};
    std::string test_data_path = paimon::test::GetDataDir() +
                                 "/orc/append_table_with_rt_branch.db/append_table_with_rt_branch/";
    auto dir = UniqueTestDirectory::Create();
    std::string table_path = dir->Str();
    ASSERT_TRUE(TestUtil::CopyDirectory(test_data_path, table_path));

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_SYSTEM, "local"}};
    WriteContextBuilder context_builder(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context,
                         context_builder.SetOptions(options).WithBranch("rt").Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write,
                         FileStoreWrite::Create(std::move(write_context)));

    // write to rt branch, pk table
    std::string data_0 =
        R"([["20240726", "watermelon", 23],
            ["20240726", "orange", 12]
        ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_0,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_0,
                                                     /*partition_map=*/
                                                     {{"dt", "20240726"}}, /*bucket=*/0, {}));
    ASSERT_OK(file_store_write->Write(std::move(batch_0)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results_0,
                         file_store_write->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(results_0.size(), 1);
    auto result_commit_msgs = std::dynamic_pointer_cast<CommitMessageImpl>(results_0[0]);
    ASSERT_TRUE(result_commit_msgs);
    auto file_meta_0 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/2,
        /*min_key=*/BinaryRowGenerator::GenerateRow({std::string("orange")}, pool_.get()),
        /*max_key=*/BinaryRowGenerator::GenerateRow({std::string("watermelon")}, pool_.get()),
        /*key_stats=*/
        BinaryRowGenerator::GenerateStats({std::string("orange")}, {std::string("watermelon")}, {0},
                                          pool_.get()),
        BinaryRowGenerator::GenerateStats({std::string("20240726"), std::string("orange"), 12},
                                          {std::string("20240726"), std::string("watermelon"), 23},
                                          std::vector<int64_t>({0, 0, 0}), pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/2, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment_0({file_meta_0}, {}, {});
    auto expected_commit_message_0 = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({std::string("20240726")}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/2, data_increment_0, CompactIncrement({}, {}, {}));
    ASSERT_TRUE(expected_commit_message_0->TEST_Equal(*result_commit_msgs));

    // write to main branch, append table
    WriteContextBuilder context_builder1(table_path, "commit_user_1");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<WriteContext> write_context1,
                         context_builder1.SetOptions(options).Finish());
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreWrite> file_store_write1,
                         FileStoreWrite::Create(std::move(write_context1)));
    std::string data_1 =
        R"([["20240725", "watermelon", 3],
            ["20240725", "orange", 2]
        ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch_1,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                                     /*partition_map=*/
                                                     {{"dt", "20240725"}}, /*bucket=*/0, {}));
    ASSERT_OK(file_store_write1->Write(std::move(batch_1)));
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<CommitMessage>> results_1,
                         file_store_write1->PrepareCommit(/*wait_compaction=*/false, 0));
    ASSERT_EQ(results_1.size(), 1);
    auto result_commit_msgs1 = std::dynamic_pointer_cast<CommitMessageImpl>(results_1[0]);
    ASSERT_TRUE(result_commit_msgs1);
    auto file_meta_1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/724, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(),
        /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/
        SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("20240725"), std::string("orange"), 2},
                                          {std::string("20240725"), std::string("watermelon"), 3},
                                          std::vector<int64_t>({0, 0, 0}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/1,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    DataIncrement data_increment_1({file_meta_1}, {}, {});
    auto expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({std::string("20240725")}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/-1, data_increment_1, CompactIncrement({}, {}, {}));
    ASSERT_TRUE(expected_commit_message_1->TEST_Equal(*result_commit_msgs1));
}

TEST_P(WriteInteTest, TestDataEvolutionWrite) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::utf8()),  arrow::field("f1", arrow::int8()),
        arrow::field("f2", arrow::int16()), arrow::field("f3", arrow::int32()),
        arrow::field("f4", arrow::int64()), arrow::field("f5", arrow::float64())};
    auto typed_schema = arrow::schema(fields);
    ::ArrowSchema c_schema;
    ASSERT_TRUE(arrow::ExportSchema(*typed_schema, &c_schema).ok());

    auto file_format = GetParam();
    auto dir = UniqueTestDirectory::Create();
    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, file_format},
                                                  {Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::ROW_TRACKING_ENABLED, "true"},
                                                  {Options::DATA_EVOLUTION_ENABLED, "true"},
                                                  {Options::FILE_SYSTEM, "local"}};

    // create table
    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), {}));
    ASSERT_OK(catalog->CreateDatabase("foo", {}, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &c_schema, /*partition_keys=*/{},
                                   /*primary_keys=*/{}, options,
                                   /*ignore_if_exists=*/false));
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");
    std::string commit_user = "commit_user_1";
    auto write_array = [&](const std::vector<std::string>& write_cols,
                           const std::shared_ptr<arrow::Array>& write_array)
        -> Result<std::vector<std::shared_ptr<CommitMessage>>> {
        WriteContextBuilder builder(table_path, commit_user);
        builder.WithWriteSchema(write_cols);
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriteContext> write_context, builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto file_store_write,
                               FileStoreWrite::Create(std::move(write_context)));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*write_array, &c_array));
        auto record_batch = std::make_unique<RecordBatch>(
            /*partition_map=*/std::map<std::string, std::string>(), /*bucket=*/0,
            /*row_kinds=*/std::vector<RecordBatch::RowKind>(), &c_array);
        PAIMON_RETURN_NOT_OK(file_store_write->Write(std::move(record_batch)));
        PAIMON_ASSIGN_OR_RAISE(auto commit_msgs,
                               file_store_write->PrepareCommit(
                                   /*wait_compaction=*/false, /*commit_identifier=*/0));
        PAIMON_RETURN_NOT_OK(file_store_write->Close());
        return commit_msgs;
    };

    auto check_meta = [&](const std::shared_ptr<CommitMessage>& result_commit_msg,
                          std::vector<std::shared_ptr<DataFileMeta>> expected_file_metas) {
        auto result_commit_msg_impl =
            std::dynamic_pointer_cast<CommitMessageImpl>(result_commit_msg);
        for (auto& file_meta : expected_file_metas) {
            file_meta = ReconstructDataFileMeta(file_meta);
        }
        DataIncrement data_increment(std::move(expected_file_metas), {}, {});
        auto expected_commit_message = std::make_shared<CommitMessageImpl>(
            BinaryRow::EmptyRow(), /*bucket=*/0,
            /*total_bucket=*/-1, data_increment, CompactIncrement({}, {}, {}));
        ASSERT_TRUE(expected_commit_message->TEST_Equal(*result_commit_msg_impl));
    };

    auto commit = [&](const std::vector<std::shared_ptr<CommitMessage>>& commit_msgs,
                      int64_t latest_snapshot_id, int64_t next_row_id) {
        CommitContextBuilder builder(table_path, commit_user);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<FileStoreCommit> file_store_commit,
                             FileStoreCommit::Create(std::move(commit_context)));
        ASSERT_OK(file_store_commit->Commit(commit_msgs));

        auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(file_store_commit.get());
        ASSERT_OK_AND_ASSIGN(auto latest_snapshot,
                             commit_impl->snapshot_manager_->LatestSnapshotOfUser(commit_user));
        ASSERT_TRUE(latest_snapshot);
        ASSERT_EQ(latest_snapshot.value().Id(), latest_snapshot_id);
        ASSERT_EQ(latest_snapshot.value().NextRowId(), next_row_id);
    };
    // first_row_id, write_cols, min_sequence_number, max_sequence_number
    using MetaType = std::tuple<int64_t, std::optional<std::vector<std::string>>, int64_t, int64_t>;
    auto check_committed_meta = [&](const std::vector<MetaType>& expected_meta) {
        ScanContextBuilder builder(table_path);
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<ScanContext> scan_context, builder.Finish());
        ASSERT_OK_AND_ASSIGN(std::unique_ptr<TableScan> table_scan,
                             TableScan::Create(std::move(scan_context)));
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<Plan> plan, table_scan->CreatePlan());
        ASSERT_EQ(plan->Splits().size(), 1);
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(plan->Splits()[0]);
        ASSERT_EQ(data_split->DataFiles().size(), expected_meta.size());
        for (size_t i = 0; i < expected_meta.size(); i++) {
            const auto& [first_row_id, write_cols, min_sequence_number, max_sequence_number] =
                expected_meta[i];
            const auto& result_meta = data_split->DataFiles()[i];
            ASSERT_EQ(result_meta->first_row_id.value(), first_row_id);
            ASSERT_EQ(result_meta->write_cols, write_cols);
            ASSERT_EQ(result_meta->min_sequence_number, min_sequence_number);
            ASSERT_EQ(result_meta->max_sequence_number, max_sequence_number);
        }
    };
    // write field: f0, f1, f2, f3, f4, f5
    std::vector<std::string> write_cols1 = typed_schema->field_names();
    auto src_array1 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        ["David", 5, 12, null, 2000, null],
        ["Lucy", 2, null, 1, null, null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs1, write_array(write_cols1, src_array1));
    ASSERT_EQ(commit_msgs1.size(), 1);
    // write_cols in data file meta is omitted for full write cols
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/100, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("David"), static_cast<int8_t>(2),
                                           static_cast<int16_t>(12), 1, 2000l, NullType()},
                                          {std::string("Lucy"), static_cast<int8_t>(5),
                                           static_cast<int16_t>(12), 1, 2000l, NullType()},
                                          std::vector<int64_t>({0, 0, 1, 1, 1, 2}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    check_meta(commit_msgs1[0], {file_meta1});
    commit(commit_msgs1, /*latest_snapshot_id=*/1, /*next_row_id=*/2);
    check_committed_meta({{0, std::nullopt, 1, 1}});

    // write field: f0, f2, f4, f5
    std::vector<std::string> write_cols2 = {"f0", "f2", "f4", "f5"};
    auto src_array2 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(
            arrow::struct_({fields[0], fields[2], fields[4], fields[5]}), R"([
        ["Alice", 50, null, 11.1],
        ["Paul", 20, 1, null],
        ["Cathy", 30, 0, 13.1],
        ["Emily", 5, 0, 14.1],
        ["Cathy", 30, 0, null]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs2, write_array(write_cols2, src_array2));
    ASSERT_EQ(commit_msgs2.size(), 1);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/100, /*row_count=*/5,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alice"), static_cast<int16_t>(5), 0l, 11.1},
                                          {std::string("Paul"), static_cast<int16_t>(50), 1l, 14.1},
                                          std::vector<int64_t>({0, 0, 1, 2}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/4, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/write_cols2);
    check_meta(commit_msgs2[0], {file_meta2});
    commit(commit_msgs2, /*latest_snapshot_id=*/2, /*next_row_id=*/7);
    check_committed_meta({{0, std::nullopt, 1, 1}, {2, write_cols2, 2, 2}});
    // write field: f0, according to file_meta1
    std::vector<std::string> write_cols3 = {"f0"};
    auto src_array3 = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({fields[0]}), R"([
        ["David3"],
        ["Lucy3"]
    ])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(auto commit_msgs3, write_array(write_cols3, src_array3));
    ASSERT_EQ(commit_msgs3.size(), 1);
    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/100, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("David3")}, {std::string("Lucy3")},
                                          std::vector<int64_t>({0}), pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/write_cols3);
    check_meta(commit_msgs3[0], {file_meta3});
    auto commit_msg_impl = std::dynamic_pointer_cast<CommitMessageImpl>(commit_msgs3[0]);
    ASSERT_TRUE(commit_msg_impl);
    // according to file_meta1
    commit_msg_impl->data_increment_.new_files_[0]->AssignFirstRowId(0);
    commit(commit_msgs3, /*latest_snapshot_id=*/3, /*next_row_id=*/7);
    // in scan, DataFileMeta is sorted by fist row id and sequence number
    check_committed_meta({
        {0, std::nullopt, 1, 1},
        {0, write_cols3, 3, 3},
        {2, write_cols2, 2, 2},
    });
}

TEST_P(WriteInteTest, TestAppendTableWriteWithBlobType) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8()),
                                 arrow::field("f1", arrow::int32()),
                                 BlobUtils::ToArrowField("blob", false)};
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::MANIFEST_FORMAT, "orc"},       {Options::FILE_FORMAT, file_format},
        {Options::TARGET_FILE_SIZE, "700"},      {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"}, {Options::DATA_EVOLUTION_ENABLED, "true"},
        {Options::FILE_SYSTEM, "local"},         {Options::BLOB_AS_DESCRIPTOR, "true"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/true));
    int64_t commit_identifier = 0;

    auto generate_blob_array = [&](const std::vector<PAIMON_UNIQUE_PTR<Bytes>>& blob_descriptors)
        -> std::shared_ptr<arrow::Array> {
        arrow::StructBuilder struct_builder(
            arrow::struct_(fields), arrow::default_memory_pool(),
            {std::make_shared<arrow::StringBuilder>(), std::make_shared<arrow::Int32Builder>(),
             std::make_shared<arrow::LargeBinaryBuilder>()});
        auto string_builder = dynamic_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
        auto int_builder = dynamic_cast<arrow::Int32Builder*>(struct_builder.field_builder(1));
        auto binary_builder =
            dynamic_cast<arrow::LargeBinaryBuilder*>(struct_builder.field_builder(2));
        for (size_t i = 0; i < blob_descriptors.size(); ++i) {
            EXPECT_TRUE(struct_builder.Append().ok());
            EXPECT_TRUE(string_builder->Append("str_" + std::to_string(i)).ok());
            if (i % 3 == 0) {
                // test null
                EXPECT_TRUE(int_builder->AppendNull().ok());
            } else {
                EXPECT_TRUE(int_builder->Append(i).ok());
            }
            EXPECT_TRUE(
                binary_builder->Append(blob_descriptors[i]->data(), blob_descriptors[i]->size())
                    .ok());
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return array;
    };

    std::vector<PAIMON_UNIQUE_PTR<Bytes>> blob_descriptors;
    std::string file1 = paimon::test::GetDataDir() + "/avro/data/avro_with_null";
    ASSERT_OK_AND_ASSIGN(auto blob1, Blob::FromPath(file1));
    blob_descriptors.emplace_back(blob1->ToDescriptor(pool_));

    std::string file2 = paimon::test::GetDataDir() + "/xxhash.data";
    ASSERT_OK_AND_ASSIGN(auto blob2, Blob::FromPath(file2, /*offset=*/0, /*length=*/91));
    blob_descriptors.emplace_back(blob2->ToDescriptor(pool_));
    ASSERT_OK_AND_ASSIGN(auto blob3, Blob::FromPath(file2, /*offset=*/92, /*length=*/85));
    blob_descriptors.emplace_back(blob3->ToDescriptor(pool_));
    ASSERT_OK_AND_ASSIGN(auto blob4, Blob::FromPath(file2, /*offset=*/300, /*length=*/3000));
    blob_descriptors.emplace_back(blob4->ToDescriptor(pool_));

    auto array = generate_blob_array(blob_descriptors);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch, batch_builder.Finish());

    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/405, /*row_count=*/4,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("str_0"), 1}, {std::string("str_3"), 2},
                                          std::vector<int64_t>({0, 2}), pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/0,
        /*write_cols=*/std::vector<std::string>({"f0", "f1"}));
    file_meta1 = ReconstructDataFileMeta(file_meta1);
    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-xxx.blob", /*file_size=*/764, /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, std::vector<int64_t>({0}),
                                          pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/0,
        /*write_cols=*/std::vector<std::string>({"blob"}));
    auto file_meta3 = std::make_shared<DataFileMeta>(
        "data-xxx.blob", /*file_size=*/3023, /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({NullType()}, {NullType()}, std::vector<int64_t>({0}),
                                          pool_.get()),
        /*min_sequence_number=*/1, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt, /*first_row_id=*/3,
        /*write_cols=*/std::vector<std::string>({"blob"}));
    std::vector<std::shared_ptr<DataFileMeta>> expected_meta = {file_meta1, file_meta2, file_meta3};

    // NOTE: Due to the write logic of C++ Paimon and Java Paimon is different, the first_row_id in
    // the CommitMessage may be different. The first_row_id will be rewritten again during the
    // commit process.
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
    ASSERT_EQ(commit_msgs.size(), 1);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(1, snapshot.value().Id());
    ASSERT_EQ(8, snapshot.value().TotalRecordCount().value());
    ASSERT_EQ(8, snapshot.value().DeltaRecordCount().value());
    ASSERT_EQ(4, snapshot.value().NextRowId().value());

    // check data file meta after commit
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits.size(), 1);
    auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(data_splits[0]);
    ASSERT_EQ(data_split->DataFiles().size(), 3);
    for (size_t i = 0; i < expected_meta.size(); i++) {
        ASSERT_TRUE(data_split->DataFiles()[i]->TEST_Equal(*expected_meta[i]));
    }
}

TEST_P(WriteInteTest, TestAppendTableWithDateFieldAsPartitionField) {
    // test date field as partition field
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::date32()),
        arrow::field("f1", arrow::int32()),
    };
    auto schema = arrow::schema(fields);

    auto file_format = GetParam();
    std::map<std::string, std::string> options = {{Options::MANIFEST_FORMAT, "orc"},
                                                  {Options::FILE_FORMAT, file_format},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::BUCKET, "-1"},
                                                  {Options::FILE_SYSTEM, "local"}};

    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{"f0"},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));

    std::string data_1 =
        R"([[2005, 11],
            [2005, 12]])";
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<RecordBatch> batch_1,
        TestHelper::MakeRecordBatch(arrow::struct_(fields), data_1,
                                    /*partition_map=*/{{"f0", "2005"}}, /*bucket=*/0, {}));

    auto file_meta = std::make_shared<DataFileMeta>(
        "data-xxx.xxx", /*file_size=*/543, /*row_count=*/2,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({2005, 11}, {2005, 12}, std::vector<int64_t>({0, 0}),
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/1, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/Timestamp(1724090888706ll, 0),
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    file_meta = ReconstructDataFileMeta(file_meta);
    DataIncrement data_increment({file_meta}, {}, {});
    std::shared_ptr<CommitMessage> expected_commit_message_1 = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({2005}, pool_.get()), /*bucket=*/0,
        /*total_bucket=*/-1, data_increment, CompactIncrement({}, {}, {}));
    std::vector<std::shared_ptr<CommitMessage>> expected_commit_messages_1 = {
        expected_commit_message_1};
    int64_t commit_identifier = 0;
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch_1), commit_identifier++,
                                                expected_commit_messages_1));
    CheckCreationTime(commit_msgs);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot1, helper->LatestSnapshot());
    ASSERT_TRUE(snapshot1);
    ASSERT_EQ(1, snapshot1.value().Id());
    ASSERT_EQ(2, snapshot1.value().TotalRecordCount().value());
    ASSERT_EQ(2, snapshot1.value().DeltaRecordCount().value());

    arrow::FieldVector fields_with_row_kind = fields;
    fields_with_row_kind.insert(fields_with_row_kind.begin(),
                                arrow::field("_VALUE_KIND", arrow::int8()));
    auto data_type = arrow::struct_(fields_with_row_kind);
    ASSERT_OK_AND_ASSIGN(std::vector<std::shared_ptr<Split>> data_splits_1,
                         helper->NewScan(StartupMode::LatestFull(), /*snapshot_id=*/std::nullopt));
    ASSERT_EQ(data_splits_1.size(), 1);
    std::string expected_data_1 =
        R"([[0, 2005, 11],
            [0, 2005, 12]])";
    ASSERT_OK_AND_ASSIGN(bool success,
                         helper->ReadAndCheckResult(data_type, data_splits_1, expected_data_1));
    ASSERT_TRUE(success);
}

TEST_P(WriteInteTest, TestNullabilityCheck) {
    auto dir = UniqueTestDirectory::Create();
    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8(), /*nullable=*/true),
                                 arrow::field("f1", arrow::int32(), /*nullable=*/false)};
    auto schema = arrow::schema(fields);
    auto file_format = GetParam();
    std::map<std::string, std::string> options = {
        {Options::FILE_FORMAT, StringUtils::ToUpperCase(file_format)},
        {Options::TARGET_FILE_SIZE, "1024"},
        {Options::BUCKET, "-1"},
    };
    ASSERT_OK_AND_ASSIGN(
        auto helper, TestHelper::Create(dir->Str(), schema, /*partition_keys=*/{},
                                        /*primary_keys=*/{}, options, /*is_streaming_mode=*/false));
    int64_t commit_identifier = 0;

    // write invalid data
    std::string data = R"([
            ["banana", 2],
            ["dog", 1],
            [null, 14],
            ["mouse", null]
    ])";
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<RecordBatch> batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_NOK_WITH_MSG(
        helper->WriteAndCommit(std::move(batch), commit_identifier++,
                               /*expected_commit_messages=*/std::nullopt),
        "CheckNullabilityMatch failed, field f1 not nullable while data have null value");

    // write valid data
    data = R"([
            ["banana", 2],
            ["dog", 1],
            [null, 14],
            ["mouse", 100]
    ])";
    ASSERT_OK_AND_ASSIGN(batch,
                         TestHelper::MakeRecordBatch(arrow::struct_(fields), data,
                                                     /*partition_map=*/{}, /*bucket=*/0, {}));
    ASSERT_OK_AND_ASSIGN(auto commit_msgs,
                         helper->WriteAndCommit(std::move(batch), commit_identifier++,
                                                /*expected_commit_messages=*/std::nullopt));
}

}  // namespace paimon::test
