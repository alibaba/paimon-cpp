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

#include "paimon/core/table/source/append_count_reader.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class DummySplit final : public Split {};

}  // namespace

class AppendCountReaderTest : public testing::Test {
 protected:
    Result<std::vector<std::shared_ptr<Split>>> CreateSplits(
        const std::string& table_path, const std::optional<int64_t>& snapshot_id) {
        ScanContextBuilder scan_context_builder(table_path);
        if (snapshot_id.has_value()) {
            scan_context_builder.AddOption(Options::SCAN_SNAPSHOT_ID,
                                           std::to_string(snapshot_id.value()));
        }

        PAIMON_ASSIGN_OR_RAISE(auto scan_context, scan_context_builder.Finish());
        PAIMON_ASSIGN_OR_RAISE(auto table_scan, TableScan::Create(std::move(scan_context)));
        PAIMON_ASSIGN_OR_RAISE(auto plan, table_scan->CreatePlan());
        if (snapshot_id.has_value() &&
            (!plan->SnapshotId().has_value() || plan->SnapshotId().value() != snapshot_id)) {
            return Status::Invalid("snapshot id mismatch");
        }
        return plan->Splits();
    }

    std::shared_ptr<FileSystem> file_system_ = std::make_shared<LocalFileSystem>();
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(AppendCountReaderTest, TestCountRowsSnapshot1) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/1));
    AppendCountReader count_reader(splits, file_system_, pool_);

    ASSERT_OK_AND_ASSIGN(int64_t count, count_reader.CountRows());
    ASSERT_EQ(count, 5);
}

TEST_F(AppendCountReaderTest, TestCountRowsSnapshot5) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/5));
    AppendCountReader count_reader(splits, file_system_, pool_);

    ASSERT_OK_AND_ASSIGN(int64_t count, count_reader.CountRows());
    ASSERT_EQ(count, 11);
}

TEST_F(AppendCountReaderTest, TestCountRowsDataEvolutionTable) {
    std::string table_path =
        GetDataDir() + "/orc/data_evolution_with_dense_stats.db/data_evolution_with_dense_stats";

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/2));
    ASSERT_FALSE(splits.empty());

    bool has_non_raw_convertible_split = false;
    for (const auto& split : splits) {
        auto data_split = std::dynamic_pointer_cast<DataSplitImpl>(split);
        ASSERT_TRUE(data_split);
        has_non_raw_convertible_split |= !data_split->RawConvertible();
    }
    ASSERT_TRUE(has_non_raw_convertible_split);

    AppendCountReader count_reader(splits, file_system_, pool_);

    ASSERT_OK_AND_ASSIGN(int64_t count, count_reader.CountRows());
    ASSERT_EQ(count, 2);
}

TEST_F(AppendCountReaderTest, TestCountRowsWithEmptySplits) {
    std::vector<std::shared_ptr<Split>> empty_splits;
    AppendCountReader count_reader(empty_splits, file_system_, pool_);

    ASSERT_OK_AND_ASSIGN(int64_t count, count_reader.CountRows());
    ASSERT_EQ(count, 0);
}

TEST_F(AppendCountReaderTest, TestCountRowsWithInvalidSplit) {
    std::vector<std::shared_ptr<Split>> splits = {std::make_shared<DummySplit>()};
    AppendCountReader count_reader(splits, file_system_, pool_);

    ASSERT_NOK_WITH_MSG(count_reader.CountRows(), "split cannot be cast to DataSplitImpl");
}

TEST_F(AppendCountReaderTest, TestAppendOnlyTableReadCreateCountReaderPredicateNotSupported) {
    std::string table_path = GetDataDir() + "/orc/append_09.db/append_09";

    auto predicate = PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                                   FieldType::DOUBLE, Literal(13.0));

    ReadContextBuilder read_context_builder(table_path);
    read_context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, read_context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/5));
    ASSERT_NOK_WITH_MSG(table_read->CreateCountReader(splits),
                        "predicate pushdown is not supported");
}

}  // namespace paimon::test
