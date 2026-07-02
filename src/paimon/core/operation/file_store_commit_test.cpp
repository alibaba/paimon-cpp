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

#include "paimon/file_store_commit.h"

#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/catalog/catalog.h"
#include "paimon/catalog/identifier.h"
#include "paimon/commit_context.h"
#include "paimon/common/utils/linked_hash_map.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/core/deletionvectors/deletion_vector.h"
#include "paimon/core/deletionvectors/deletion_vectors_index_file.h"
#include "paimon/core/index/deletion_vector_meta.h"
#include "paimon/core/index/index_file_meta.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/operation/file_store_commit_impl.h"
#include "paimon/core/table/sink/commit_message_impl.h"
#include "paimon/core/utils/snapshot_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/result.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(FileStoreCommitTest, TestCreate) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"}};

    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &arrow_schema,
                                   /*partition_keys=*/{"f1"},
                                   /*primary_keys=*/{}, options,
                                   /*ignore_if_exists=*/false));
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    CommitContextBuilder context_builder(table_path, "commit_user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));
    auto commit_impl = dynamic_cast<FileStoreCommitImpl*>(commit.get());
    ASSERT_TRUE(commit_impl);
}

TEST(FileStoreCommitTest, TestAppendDvIndexShouldUseOverwriteCommitKind) {
    auto string_field = arrow::field("f0", arrow::utf8());
    auto int_field = arrow::field("f1", arrow::int32());
    auto int_field1 = arrow::field("f2", arrow::int32());
    auto double_field = arrow::field("f3", arrow::float64());
    auto schema =
        arrow::schema(arrow::FieldVector({string_field, int_field, int_field1, double_field}));

    ::ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &arrow_schema).ok());
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    std::map<std::string, std::string> options = {{Options::FILE_FORMAT, "orc"},
                                                  {Options::TARGET_FILE_SIZE, "1024"},
                                                  {Options::FILE_SYSTEM, "local"},
                                                  {Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f2"}};

    ASSERT_OK_AND_ASSIGN(auto catalog, Catalog::Create(dir->Str(), options));
    ASSERT_OK(catalog->CreateDatabase("foo", options, /*ignore_if_exists=*/false));
    ASSERT_OK(catalog->CreateTable(Identifier("foo", "bar"), &arrow_schema,
                                   /*partition_keys=*/{"f1"},
                                   /*primary_keys=*/{}, options,
                                   /*ignore_if_exists=*/false));
    std::string table_path = PathUtil::JoinPath(dir->Str(), "foo.db/bar");

    CommitContextBuilder context_builder(table_path, "commit_user");
    ASSERT_OK_AND_ASSIGN(std::unique_ptr<CommitContext> commit_context,
                         context_builder.AddOption(Options::MANIFEST_FORMAT, "orc")
                             .AddOption(Options::MANIFEST_TARGET_FILE_SIZE, "8mb")
                             .AddOption(Options::FILE_SYSTEM, "local")
                             .Finish());
    ASSERT_OK_AND_ASSIGN(auto commit, FileStoreCommit::Create(std::move(commit_context)));

    LinkedHashMap<std::string, DeletionVectorMeta> dv_ranges;
    dv_ranges.insert_or_assign("data-file-1",
                               DeletionVectorMeta(
                                   /*data_file_name=*/"data-file-1", /*offset=*/0, /*length=*/10,
                                   /*cardinality=*/1));
    std::vector<std::shared_ptr<IndexFileMeta>> new_index_files;
    new_index_files.push_back(std::make_shared<IndexFileMeta>(
        DeletionVectorsIndexFile::DELETION_VECTORS_INDEX, "dv-index-1", 100, 1,
        /*dv_ranges=*/dv_ranges, /*external_path=*/std::nullopt));
    DataIncrement data_increment({}, {}, {}, std::move(new_index_files), {});
    std::shared_ptr<CommitMessage> msg = std::make_shared<CommitMessageImpl>(
        BinaryRowGenerator::GenerateRow({10}, GetDefaultPool().get()), /*bucket=*/0,
        /*total_bucket=*/2, data_increment, CompactIncrement({}, {}, {}));

    ASSERT_OK(commit->Commit({msg}, /*commit_identifier=*/1));

    auto fs = std::make_shared<LocalFileSystem>();
    SnapshotManager snapshot_manager(fs, table_path);
    ASSERT_OK_AND_ASSIGN(std::optional<Snapshot> snapshot, snapshot_manager.LatestSnapshot());
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(Snapshot::CommitKind::Overwrite(), snapshot.value().GetCommitKind());
}

}  // namespace paimon::test
