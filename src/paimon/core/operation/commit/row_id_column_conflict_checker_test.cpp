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

#include "paimon/core/operation/commit/row_id_column_conflict_checker.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/stats/simple_stats.h"
#include "paimon/data/timestamp.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class RowIdColumnConflictCheckerTest : public testing::Test {
 protected:
    void SetUp() override {
        auto fs = std::make_shared<LocalFileSystem>();
        const std::string table_root =
            GetDataDir() + "/orc/pk_table_with_alter_table.db/pk_table_with_alter_table/";
        schema_manager_ = std::make_shared<SchemaManager>(fs, table_root);
    }

    std::shared_ptr<DataFileMeta> CreateFile(
        const std::string& file_name, std::optional<int64_t> first_row_id, int64_t row_count,
        int64_t schema_id, std::optional<std::vector<std::string>> write_cols) const {
        return std::make_shared<DataFileMeta>(
            file_name, /*file_size=*/0, row_count, DataFileMeta::EmptyMinKey(),
            DataFileMeta::EmptyMaxKey(), SimpleStats::EmptyStats(), SimpleStats::EmptyStats(),
            /*min_seq_no=*/0,
            /*max_seq_no=*/0, schema_id, /*level=*/0,
            /*extra_files=*/std::vector<std::optional<std::string>>(),
            /*creation_time=*/Timestamp(0, 0),
            /*delete_row_count=*/std::nullopt,
            /*embedded_index=*/nullptr, /*file_source=*/std::nullopt,
            /*value_stats_cols=*/std::nullopt,
            /*external_path=*/std::nullopt,
            /*first_row_id=*/first_row_id, write_cols);
    }

    Result<std::shared_ptr<RowIdColumnConflictChecker>> CreateChecker(
        const std::vector<std::shared_ptr<DataFileMeta>>& files) const {
        return RowIdColumnConflictChecker::FromDataFiles(schema_manager_, files);
    }

 private:
    std::shared_ptr<SchemaManager> schema_manager_;
};

TEST_F(RowIdColumnConflictCheckerTest, TestAllowsDisjointWriteColumns) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current", /*first_row_id=*/0, /*row_count=*/10,
                                                /*schema_id=*/0, std::vector<std::string>{"b"})}));

    auto historical = CreateFile("historical", /*first_row_id=*/0, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"c"});
    ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(historical));
    ASSERT_FALSE(conflicts);
}

TEST_F(RowIdColumnConflictCheckerTest, TestDetectsSameWriteColumns) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current", /*first_row_id=*/0, /*row_count=*/10,
                                                /*schema_id=*/0, std::vector<std::string>{"b"})}));

    auto historical = CreateFile("historical", /*first_row_id=*/0, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"b"});
    ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(historical));
    ASSERT_TRUE(conflicts);
}

TEST_F(RowIdColumnConflictCheckerTest, TestUsesFieldIdAcrossRename) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current", /*first_row_id=*/0, /*row_count=*/10,
                                                /*schema_id=*/1, std::vector<std::string>{"c"})}));

    auto historical = CreateFile("historical", /*first_row_id=*/0, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"b"});
    ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(historical));
    ASSERT_TRUE(conflicts);
}

TEST_F(RowIdColumnConflictCheckerTest, TestTreatsNullWriteColumnsAsFullSchemaWrite) {
    ASSERT_OK_AND_ASSIGN(auto checker,
                         CreateChecker({CreateFile("current", /*first_row_id=*/0, /*row_count=*/10,
                                                   /*schema_id=*/0, /*write_cols=*/std::nullopt)}));

    auto historical = CreateFile("historical", /*first_row_id=*/0, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"b"});
    ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(historical));
    ASSERT_TRUE(conflicts);
}

TEST_F(RowIdColumnConflictCheckerTest, TestMergesOverlappedDeltaRangesAndWriteColumns) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current-b", /*first_row_id=*/0, /*row_count=*/11,
                                                /*schema_id=*/0, std::vector<std::string>{"b"}),
                                     CreateFile("current-c", /*first_row_id=*/5, /*row_count=*/11,
                                                /*schema_id=*/0, std::vector<std::string>{"c"})}));

    auto historical_b = CreateFile("historical-b", /*first_row_id=*/12, /*row_count=*/1,
                                   /*schema_id=*/0, std::vector<std::string>{"b"});
    auto historical_c = CreateFile("historical-c", /*first_row_id=*/12, /*row_count=*/1,
                                   /*schema_id=*/0, std::vector<std::string>{"c"});
    ASSERT_OK_AND_ASSIGN(bool conflicts_b, checker->ConflictsWith(historical_b));
    ASSERT_OK_AND_ASSIGN(bool conflicts_c, checker->ConflictsWith(historical_c));
    ASSERT_TRUE(conflicts_b);
    ASSERT_TRUE(conflicts_c);
}

TEST_F(RowIdColumnConflictCheckerTest, TestScansAllOverlappedRangesAfterBinarySearch) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current-b", /*first_row_id=*/0, /*row_count=*/5,
                                                /*schema_id=*/0, std::vector<std::string>{"b"}),
                                     CreateFile("current-c", /*first_row_id=*/10, /*row_count=*/5,
                                                /*schema_id=*/0, std::vector<std::string>{"c"})}));

    auto historical = CreateFile("historical", /*first_row_id=*/3, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"c"});
    ASSERT_OK_AND_ASSIGN(bool conflicts, checker->ConflictsWith(historical));
    ASSERT_TRUE(conflicts);
}

TEST_F(RowIdColumnConflictCheckerTest, TestIgnoreUnknownNonSystemWriteColumn) {
    ASSERT_OK_AND_ASSIGN(
        auto checker, CreateChecker({CreateFile("current", /*first_row_id=*/0, /*row_count=*/10,
                                                /*schema_id=*/0, std::vector<std::string>{"b"})}));

    auto historical = CreateFile("historical", /*first_row_id=*/0, /*row_count=*/10,
                                 /*schema_id=*/0, std::vector<std::string>{"missing"});
    auto conflicts = checker->ConflictsWith(historical);
    ASSERT_FALSE(conflicts.ok());
}

}  // namespace paimon::test
