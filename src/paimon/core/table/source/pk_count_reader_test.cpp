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

#include "paimon/core/table/source/pk_count_reader.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/operation/internal_read_context.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/core/table/source/data_split_impl.h"
#include "paimon/core/utils/file_store_path_factory.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/split.h"
#include "paimon/table/source/table_scan.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

namespace {

class DummySplit final : public Split {};

}  // namespace

class PKCountReaderTest : public testing::Test {
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
        if (snapshot_id.has_value()) {
            if (!plan->SnapshotId().has_value() || plan->SnapshotId().value() != snapshot_id) {
                return Status::Invalid("snapshot id mismatch");
            }
        }
        return plan->Splits();
    }

    Result<std::shared_ptr<InternalReadContext>> CreateInternalContext(
        const std::string& table_path) {
        ReadContextBuilder read_context_builder(table_path);
        PAIMON_ASSIGN_OR_RAISE(auto read_context, read_context_builder.Finish());

        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), table_path);
        PAIMON_ASSIGN_OR_RAISE(auto table_schema, schema_manager.ReadSchema(0));
        PAIMON_ASSIGN_OR_RAISE(auto internal_context,
                               InternalReadContext::Create(std::move(read_context), table_schema,
                                                           table_schema->Options()));
        return std::shared_ptr<InternalReadContext>(std::move(internal_context));
    }

    Result<std::shared_ptr<FileStorePathFactory>> CreatePathFactory(
        const std::shared_ptr<InternalReadContext>& internal_context) {
        const auto& core_options = internal_context->GetCoreOptions();
        auto arrow_schema =
            DataField::ConvertDataFieldsToArrowSchema(internal_context->GetTableSchema()->Fields());

        PAIMON_ASSIGN_OR_RAISE(std::vector<std::string> external_paths,
                               core_options.CreateExternalPaths());
        PAIMON_ASSIGN_OR_RAISE(std::optional<std::string> global_index_external_path,
                               core_options.CreateGlobalIndexExternalPath());

        PAIMON_ASSIGN_OR_RAISE(
            auto path_factory,
            FileStorePathFactory::Create(
                internal_context->GetPath(), arrow_schema,
                internal_context->GetTableSchema()->PartitionKeys(),
                core_options.GetPartitionDefaultName(), core_options.GetFileFormat()->Identifier(),
                core_options.DataFilePrefix(), core_options.LegacyPartitionNameEnabled(),
                external_paths, global_index_external_path, core_options.IndexFileInDataFileDir(),
                pool_));

        return std::shared_ptr<FileStorePathFactory>(std::move(path_factory));
    }

 protected:
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};

TEST_F(PKCountReaderTest, TestCountRowsWithMORSnapshot5) {
    std::string table_path =
        GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/5));
    ASSERT_OK_AND_ASSIGN(auto internal_context, CreateInternalContext(table_path));
    ASSERT_OK_AND_ASSIGN(auto path_factory, CreatePathFactory(internal_context));

    ASSERT_OK_AND_ASSIGN(auto pk_count_reader,
                         PKCountReader::Create(splits, path_factory, internal_context, pool_,
                                               internal_context->GetExecutor()));
    ASSERT_OK_AND_ASSIGN(int64_t count, pk_count_reader->CountRows());

    ASSERT_EQ(count, 11);
}

TEST_F(PKCountReaderTest, TestCountRowsWithDVSnapshot6) {
    std::string table_path =
        GetDataDir() + "/orc/pk_table_scan_and_read_dv.db/pk_table_scan_and_read_dv/";

    ASSERT_OK_AND_ASSIGN(auto splits, CreateSplits(table_path, /*snapshot_id=*/6));
    ASSERT_OK_AND_ASSIGN(auto internal_context, CreateInternalContext(table_path));
    ASSERT_OK_AND_ASSIGN(auto path_factory, CreatePathFactory(internal_context));

    ASSERT_OK_AND_ASSIGN(auto pk_count_reader,
                         PKCountReader::Create(splits, path_factory, internal_context, pool_,
                                               internal_context->GetExecutor()));
    ASSERT_OK_AND_ASSIGN(int64_t count, pk_count_reader->CountRows());

    ASSERT_EQ(count, 8);
}

TEST_F(PKCountReaderTest, TestCountRowsWithEmptySplits) {
    std::string table_path =
        GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ASSERT_OK_AND_ASSIGN(auto internal_context, CreateInternalContext(table_path));
    ASSERT_OK_AND_ASSIGN(auto path_factory, CreatePathFactory(internal_context));

    std::vector<std::shared_ptr<Split>> empty_splits;
    ASSERT_OK_AND_ASSIGN(auto pk_count_reader,
                         PKCountReader::Create(empty_splits, path_factory, internal_context, pool_,
                                               internal_context->GetExecutor()));
    ASSERT_OK_AND_ASSIGN(int64_t count, pk_count_reader->CountRows());

    ASSERT_EQ(count, 0);
}

TEST_F(PKCountReaderTest, TestCountRowsWithInvalidSplit) {
    std::string table_path =
        GetDataDir() + "/orc/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/";

    ASSERT_OK_AND_ASSIGN(auto internal_context, CreateInternalContext(table_path));
    ASSERT_OK_AND_ASSIGN(auto path_factory, CreatePathFactory(internal_context));

    std::vector<std::shared_ptr<Split>> splits = {std::make_shared<DummySplit>()};
    ASSERT_OK_AND_ASSIGN(auto pk_count_reader,
                         PKCountReader::Create(splits, path_factory, internal_context, pool_,
                                               internal_context->GetExecutor()));

    ASSERT_NOK_WITH_MSG(pk_count_reader->CountRows(), "split cannot be cast to DataSplitImpl");
}

}  // namespace paimon::test
