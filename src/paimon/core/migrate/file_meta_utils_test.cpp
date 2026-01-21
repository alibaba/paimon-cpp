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

#include "paimon/migrate/file_meta_utils.h"

#include <optional>
#include <ostream>
#include <utility>
#include <variant>

#include "gtest/gtest.h"
#include "paimon/common/data/binary_row.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/manifest/file_source.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/data/decimal.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class FileMetaUtilsTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
        fs_ = std::make_shared<LocalFileSystem>();
        tmp_dir_ = UniqueTestDirectory::Create();
        dst_table_dir_ = UniqueTestDirectory::Create();
    }
    void TearDown() override {
        tmp_dir_.reset();
        dst_table_dir_.reset();
        fs_.reset();
        pool_.reset();
    }
    void CopyFile(const std::string& src_file, const std::string& dst_file) const {
        std::string content;
        ASSERT_OK(fs_->ReadFile(src_file, &content));
        ASSERT_OK(fs_->WriteFile(dst_file, content, /*overwrite=*/false));
    }
    void CreateTable(const std::string& table_name, const std::string& schema_path) const {
        std::string table_path = dst_table_dir_->Str() + "/" + table_name;
        ASSERT_OK(fs_->Mkdirs(table_path));
        std::string schema_dir = table_path + "/schema";
        ASSERT_OK(fs_->Mkdirs(schema_dir));
        CopyFile(schema_path, schema_dir + "/schema-0");
    }

    std::vector<std::string> CopyDataFilesToTempDir(
        const std::vector<std::string>& src_data_files) const {
        std::vector<std::string> tmp_data_files;
        for (const auto& name : src_data_files) {
            std::string tmp_data_file = tmp_dir_->Str() + "/" + PathUtil::GetName(name);
            CopyFile(name, tmp_data_file);
            tmp_data_files.push_back(tmp_data_file);
        }
        return tmp_data_files;
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
    std::shared_ptr<LocalFileSystem> fs_;
    std::unique_ptr<UniqueTestDirectory> tmp_dir_;
    std::unique_ptr<UniqueTestDirectory> dst_table_dir_;
};

TEST_F(FileMetaUtilsTest, TestSimple) {
    // copy a db from test/data to tmp path
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "orc/append_10_external_path.db/append_10_external_path/schema/schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::vector<std::string> src_data_files = {
        paimon::test::GetDataDir() +
            "/orc/append_09.db/append_09/f1=20/bucket-0/"
            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
        paimon::test::GetDataDir() +
            "/orc/append_09.db/append_09/f1=20/bucket-0/"
            "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"};

    std::vector<std::string> tmp_data_files = CopyDataFilesToTempDir(src_data_files);

    // generate commit message and check
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitMessage> msg,
        FileMetaUtils::GenerateCommitMessage(tmp_data_files, dst_table_path, {},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}));
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
    ASSERT_TRUE(msg_impl);
    ASSERT_EQ(2, msg_impl->GetNewFilesIncrement().NewFiles().size());

    // check commitMsg
    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc", /*file_size=*/506,
        /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Paul"), 20, 1, NullType()},
                                          {std::string("Paul"), 20, 1, NullType()}, {0, 0, 0, 1},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc", /*file_size=*/541,
        /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                          {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[1]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    CommitMessageImpl expected(BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/-1,
                               DataIncrement({file_meta1, file_meta2}, {}, {}),
                               CompactIncrement({}, {}, {}));
    ASSERT_EQ(expected, *msg_impl) << expected.ToString() << std::endl << msg_impl->ToString();

    // check data files move to dst table
    ASSERT_OK_AND_ASSIGN(
        bool exist,
        fs_->Exists(dst_table_path + "/bucket-0/data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(
        exist,
        fs_->Exists(dst_table_path + "/bucket-0/data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"));
    ASSERT_TRUE(exist);
}

TEST_F(FileMetaUtilsTest, TestFailover) {
    // total 2 files, rename the first file to target, and then fail over
    // only need to rename the second file next time
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/append_10_external_path.db/append_10_external_path/schema/schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::vector<std::string> src_data_files = {
        paimon::test::GetDataDir() +
            "/orc/append_09.db/append_09/f1=20/bucket-0/"
            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
        paimon::test::GetDataDir() +
            "/orc/append_09.db/append_09/f1=20/bucket-0/"
            "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"};

    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    std::vector<std::string> tmp_data_files = CopyDataFilesToTempDir(src_data_files);
    // simulate renaming the first file to target, and then fail ove
    CopyFile(src_data_files[0],
             dst_table_path + "/bucket-0/data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc");
    // generate commit message and check
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitMessage> msg,
        FileMetaUtils::GenerateCommitMessage(tmp_data_files, dst_table_path, {},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}));
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
    ASSERT_TRUE(msg_impl);
    ASSERT_EQ(2, msg_impl->GetNewFilesIncrement().NewFiles().size());

    // check commitMsg
    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc", /*file_size=*/506,
        /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Paul"), 20, 1, NullType()},
                                          {std::string("Paul"), 20, 1, NullType()}, {0, 0, 0, 1},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc", /*file_size=*/541,
        /*row_count=*/1,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Lucy"), 20, 1, 14.1},
                                          {std::string("Lucy"), 20, 1, 14.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[1]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    CommitMessageImpl expected(BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/-1,
                               DataIncrement({file_meta1, file_meta2}, {}, {}),
                               CompactIncrement({}, {}, {}));
    ASSERT_EQ(expected, *msg_impl) << expected.ToString() << std::endl << msg_impl->ToString();

    // check data files move to dst table
    ASSERT_OK_AND_ASSIGN(
        bool exist,
        fs_->Exists(dst_table_path + "/bucket-0/data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(
        exist,
        fs_->Exists(dst_table_path + "/bucket-0/data-db2b44c0-0d73-449d-82a0-4075bd2cb6e3-0.orc"));
    ASSERT_TRUE(exist);
}

TEST_F(FileMetaUtilsTest, TestWithPartition) {
    // copy a db from test/data to tmp path
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/multi_partition_append_table.db/multi_partition_append_table/schema/"
        "schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::vector<std::string> src_data_files = {
        paimon::test::GetDataDir() +
            "/orc/multi_partition_append_table.db/multi_partition_append_table/f1=10/f2=0/"
            "bucket-0/"
            "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc",
        paimon::test::GetDataDir() +
            "/orc/multi_partition_append_table.db/multi_partition_append_table/f1=10/f2=0/"
            "bucket-0/"
            "data-1c547e5f-48b2-4917-a996-71d306377661-0.orc"};

    std::vector<std::string> tmp_data_files = CopyDataFilesToTempDir(src_data_files);

    // generate commit message and check
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    std::map<std::string, std::string> partition = {{"f1", "10"}, {"f2", "0"}};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitMessage> msg,
        FileMetaUtils::GenerateCommitMessage(tmp_data_files, dst_table_path, partition,
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}));
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
    ASSERT_TRUE(msg_impl);
    ASSERT_EQ(2, msg_impl->GetNewFilesIncrement().NewFiles().size());

    // check commitMsg
    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc", /*file_size=*/575,
        /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Bob"), 10, 0, 12.1},
                                          {std::string("Tony"), 10, 0, 14.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    auto file_meta2 = std::make_shared<DataFileMeta>(
        "data-1c547e5f-48b2-4917-a996-71d306377661-0.orc", /*file_size=*/589,
        /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats({std::string("Alex"), 10, 0, 12.1},
                                          {std::string("Emily"), 10, 0, 16.1}, {0, 0, 0, 0},
                                          pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[1]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);
    CommitMessageImpl expected(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()), /*bucket=*/0,
                               /*total_buckets=*/-1,
                               DataIncrement({file_meta1, file_meta2}, {}, {}),
                               CompactIncrement({}, {}, {}));
    ASSERT_EQ(expected, *msg_impl) << expected.ToString() << std::endl << msg_impl->ToString();

    // check data files move to dst table
    ASSERT_OK_AND_ASSIGN(
        bool exist,
        fs_->Exists(dst_table_path +
                    "/f1=10/f2=0/bucket-0/data-1c547e5f-48b2-4917-a996-71d306377661-0.orc"));
    ASSERT_TRUE(exist);
    ASSERT_OK_AND_ASSIGN(
        exist, fs_->Exists(dst_table_path +
                           "/f1=10/f2=0/bucket-0/data-01b6a930-6564-409b-b8f4-ed1307790d72-0.orc"));
    ASSERT_TRUE(exist);
}

TEST_F(FileMetaUtilsTest, TestWithNestedType) {
    // copy a db from test/data to tmp path
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/append_complex_build_in_fieldid.db/append_complex_build_in_fieldid/schema/"
        "schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::vector<std::string> src_data_files = {
        paimon::test::GetDataDir() +
        "/orc/append_complex_build_in_fieldid.db/append_complex_build_in_fieldid/"
        "bucket-0/data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc"};

    std::vector<std::string> tmp_data_files = CopyDataFilesToTempDir(src_data_files);

    // generate commit message and check
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitMessage> msg,
                         FileMetaUtils::GenerateCommitMessage(
                             tmp_data_files, dst_table_path, /*partition_values=*/{},
                             /*options=*/{{Options::FILE_FORMAT, "orc"}}));
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
    ASSERT_TRUE(msg_impl);
    ASSERT_EQ(1, msg_impl->GetNewFilesIncrement().NewFiles().size());

    // check commitMsg
    std::vector<CommitMessageImpl> expected_msgs;
    auto file_meta1 = std::make_shared<DataFileMeta>(
        "data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc", /*file_size=*/1222,
        /*row_count=*/3,
        /*min_key=*/BinaryRow::EmptyRow(), /*max_key=*/BinaryRow::EmptyRow(),
        /*key_stats=*/SimpleStats::EmptyStats(),
        BinaryRowGenerator::GenerateStats(
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(0, 0), 9), 24,
             Decimal(2, 2, 12)},
            {NullType(), NullType(), NullType(), TimestampType(Timestamp(123123, 123000), 9), 2456,
             Decimal(2, 2, 22)},
            {0, 0, 0, 0, 0, 1}, pool_.get()),
        /*min_sequence_number=*/0, /*max_sequence_number=*/0, /*schema_id=*/0,
        /*level=*/0, /*extra_files=*/std::vector<std::optional<std::string>>(),
        /*creation_time=*/msg_impl->GetNewFilesIncrement().NewFiles()[0]->creation_time,
        /*delete_row_count=*/0, /*embedded_index=*/nullptr, FileSource::Append(),
        /*value_stats_cols=*/std::nullopt, /*external_path=*/std::nullopt,
        /*first_row_id=*/std::nullopt,
        /*write_cols=*/std::nullopt);

    CommitMessageImpl expected(BinaryRow::EmptyRow(), /*bucket=*/0, /*total_buckets=*/-1,
                               DataIncrement({file_meta1}, {}, {}), CompactIncrement({}, {}, {}));
    ASSERT_EQ(expected, *msg_impl) << expected.ToString() << std::endl << msg_impl->ToString();

    // check data files move to dst table
    ASSERT_OK_AND_ASSIGN(
        bool exist,
        fs_->Exists(dst_table_path + "/bucket-0/data-6dac9052-36d8-4950-8f74-b2bbc082e489-0.orc"));
    ASSERT_TRUE(exist);
}

TEST_F(FileMetaUtilsTest, TestNonExistTablePath) {
    // test non-exist table path
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(/*src_data_files=*/{}, "invalid_table_path",
                                             /*partition_values=*/{},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "load schema failed, no schema in invalid_table_path");
}

TEST_F(FileMetaUtilsTest, TestInvalidBucketMode) {
    // test invalid bucket count
    std::string src_schema_path =
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/schema/schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(/*src_data_files=*/{}, dst_table_path,
                                             /*partition_values=*/{{"f1", "10"}},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "migrate only support append table with unaware-bucket");
}

TEST_F(FileMetaUtilsTest, TestInvalidPKTable) {
    // test unsupported pk table
    std::string src_schema_path =
        paimon::test::GetDataDir() + "/orc/pk_09.db/pk_09/schema/schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(/*src_data_files=*/{}, dst_table_path,
                                             /*partition_values=*/{{"f1", "10"}},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "migrate only support append table with unaware-bucket");
}

TEST_F(FileMetaUtilsTest, TestInvalidExternalPath) {
    // test unsupported external path
    std::string table_schema_str = R"({
        "version" : 3,
        "id" : 0,
        "fields" : [ {
                "id" : 0,
                "name" : "f0",
                "type" : "STRING"
        }, {
                "id" : 1,
                "name" : "f1",
                "type" : "INT"
        }, {
                "id" : 2,
                "name" : "f2",
                "type" : "INT"
        }, {
                "id" : 3,
                "name" : "f3",
                "type" : "DOUBLE"
        } ],
        "highestFieldId" : 3,
        "partitionKeys" : [],
        "primaryKeys" : [],
        "options" : {
                "bucket" : "-1",
                "manifest.format" : "orc",
                "file.format" : "orc",
                "data-file.external-paths" : "FILE:///tmp/external",
                "data-file.external-paths.strategy" : "round-robin"
        },
        "timeMillis" : 1721614341162
    })";

    auto schema_tmp_dir = UniqueTestDirectory::Create();
    auto schema_tmp_path = schema_tmp_dir->Str() + "/schema-0";
    ASSERT_OK(fs_->AtomicStore(schema_tmp_path, table_schema_str));

    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, schema_tmp_path);
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(/*src_data_files=*/{}, dst_table_path,
                                             /*partition_values=*/{},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "migrate only support schema without external paths and index not in data file dir");
}

TEST_F(FileMetaUtilsTest, TestWithInvalidPartition) {
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/multi_partition_append_table.db/multi_partition_append_table/schema/"
        "schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);

    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    // invalid partition
    std::map<std::string, std::string> partition = {{"f2", "0"}};
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(/*src data files*/ {}, dst_table_path, partition,
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "can not find partition key 'f1' in input partition");
}

TEST_F(FileMetaUtilsTest, TestEmptyInput) {
    // copy a db from test/data to tmp path
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/multi_partition_append_table.db/multi_partition_append_table/schema/"
        "schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);

    // generate commit message and check
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    std::map<std::string, std::string> partition = {{"f1", "10"}, {"f2", "0"}};
    ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<CommitMessage> msg,
        FileMetaUtils::GenerateCommitMessage(/*src_data_files=*/{}, dst_table_path, partition,
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}));
    auto msg_impl = dynamic_cast<CommitMessageImpl*>(msg.get());
    ASSERT_TRUE(msg_impl);
    ASSERT_EQ(0, msg_impl->GetNewFilesIncrement().NewFiles().size());

    CommitMessageImpl expected(BinaryRowGenerator::GenerateRow({10, 0}, pool_.get()), /*bucket=*/0,
                               /*total_buckets=*/-1, DataIncrement({}, {}, {}),
                               CompactIncrement({}, {}, {}));
    ASSERT_EQ(expected, *msg_impl) << expected.ToString() << std::endl << msg_impl->ToString();

    // check data files move to dst table
    ASSERT_OK_AND_ASSIGN(bool exist, fs_->Exists(dst_table_path + "/f1=10/f2=0/bucket-0/"));
    ASSERT_FALSE(exist);
}

TEST_F(FileMetaUtilsTest, TestInvalidFile) {
    // copy a db from test/data to tmp path
    std::string src_schema_path =
        paimon::test::GetDataDir() +
        "/orc/append_10_external_path.db/append_10_external_path/schema/schema-0";
    std::string dst_table_name = "test_table";
    CreateTable(dst_table_name, src_schema_path);
    std::vector<std::string> src_data_files = {
        paimon::test::GetDataDir() +
            "/orc/append_09.db/append_09/f1=20/bucket-0/"
            "data-b913a160-a4d1-4084-af2a-18333c35668e-0.orc",
        paimon::test::GetDataDir() + "/orc/append_09.db/append_09/snapshot/snapshot-1"};

    std::vector<std::string> tmp_data_files = CopyDataFilesToTempDir(src_data_files);

    // generate commit message and check
    std::string dst_table_path = dst_table_dir_->Str() + "/" + dst_table_name;
    ASSERT_NOK_WITH_MSG(
        FileMetaUtils::GenerateCommitMessage(tmp_data_files, dst_table_path, {},
                                             /*options=*/{{Options::FILE_FORMAT, "orc"}}),
        "extract file info failed");
}

}  // namespace paimon::test
