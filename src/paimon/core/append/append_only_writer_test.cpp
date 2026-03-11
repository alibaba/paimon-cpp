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

#include "paimon/core/append/append_only_writer.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/fs/external_path_provider.h"
#include "paimon/core/compact/noop_compact_manager.h"
#include "paimon/core/core_options.h"
#include "paimon/core/io/compact_increment.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_increment.h"
#include "paimon/core/utils/commit_increment.h"
#include "paimon/defs.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Array;
}  // namespace arrow

namespace paimon::test {

class AppendOnlyWriterTest : public testing::Test {
 public:
    void SetUp() override {
        memory_pool_ = GetDefaultPool();
        compact_manager_ = std::make_shared<NoopCompactManager>();
    }

 private:
    std::shared_ptr<MemoryPool> memory_pool_;
    std::shared_ptr<CompactManager> compact_manager_;
};

TEST_F(AppendOnlyWriterTest, TestEmptyCommits) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "mock_format";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "mock_format";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};

    auto schema = arrow::schema(fields);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    ASSERT_OK(path_factory->Init(dir->Str(), "mock_format", options.DataFilePrefix(), nullptr));

    AppendOnlyWriter writer(options, /*schema_id=*/0, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    for (int i = 0; i < 3; i++) {
        ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(true));
        ASSERT_TRUE(inc.GetNewFilesIncrement().IsEmpty());
        ASSERT_TRUE(inc.GetCompactIncrement().IsEmpty());
    }
}

TEST_F(AppendOnlyWriterTest, TestWriteAndPrepareCommit) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "mock_format";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "mock_format";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {
        arrow::field("f0", arrow::boolean()),  arrow::field("f1", arrow::uint8()),
        arrow::field("f10", arrow::float64()), arrow::field("f11", arrow::utf8()),
        arrow::field("f12", arrow::binary()),  arrow::field("non-partition-field", arrow::int32())};

    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "mock_format", options.DataFilePrefix(), nullptr));
    AppendOnlyWriter writer(options, /*schema_id=*/2, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    arrow::StringBuilder builder;
    for (size_t j = 0; j < 100; j++) {
        ASSERT_TRUE(builder.Append(std::to_string(j)).ok());
    }
    std::shared_ptr<arrow::Array> array = builder.Finish().ValueOrDie();
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());
    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch, batch_builder.Finish());
    ASSERT_OK(writer.Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK_AND_ASSIGN(CommitIncrement inc, writer.PrepareCommit(true));
    ASSERT_FALSE(inc.GetNewFilesIncrement().IsEmpty());
    const auto& data_increment = inc.GetNewFilesIncrement();
    const auto& data_file_metas = data_increment.NewFiles();
    ASSERT_EQ(1, data_file_metas.size());
    ASSERT_EQ(2, data_file_metas[0]->schema_id);
    ASSERT_TRUE(inc.GetCompactIncrement().IsEmpty());
    std::string path = path_factory->ToPath(inc.GetNewFilesIncrement().NewFiles()[0]->file_name);
    ASSERT_OK_AND_ASSIGN(bool exist, options.GetFileSystem()->Exists(path));
    ASSERT_TRUE(exist);
    ASSERT_OK(writer.Close());
}

TEST_F(AppendOnlyWriterTest, TestWriteAndClose) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "orc";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    AppendOnlyWriter writer(options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    auto struct_type = arrow::struct_(fields);
    arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::StringBuilder>()});
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
    for (size_t j = 0; j < 100; j++) {
        ASSERT_TRUE(struct_builder.Append().ok());
        ASSERT_TRUE(string_builder->Append(std::to_string(j)).ok());
    }
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    ASSERT_TRUE(array);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch, batch_builder.Finish());
    ASSERT_OK(writer.Write(std::move(record_batch)));
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer.Close());

    auto file_system = std::make_shared<LocalFileSystem>();
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(file_system->ListDir(dir->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
}

TEST_F(AppendOnlyWriterTest, TestInvalidRowKind) {
    std::map<std::string, std::string> raw_options;
    raw_options[Options::FILE_FORMAT] = "orc";
    raw_options[Options::FILE_SYSTEM] = "local";
    raw_options[Options::MANIFEST_FORMAT] = "orc";
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(raw_options));

    arrow::FieldVector fields = {arrow::field("f0", arrow::utf8())};
    auto schema = arrow::schema(fields);

    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);

    auto path_factory = std::make_shared<DataFilePathFactory>();
    ASSERT_OK(path_factory->Init(dir->Str(), "orc", options.DataFilePrefix(), nullptr));
    AppendOnlyWriter writer(options, /*schema_id=*/1, schema, /*write_cols=*/std::nullopt,
                            /*max_sequence_number=*/-1, path_factory, compact_manager_,
                            memory_pool_);
    auto struct_type = arrow::struct_(fields);
    arrow::StructBuilder struct_builder(struct_type, arrow::default_memory_pool(),
                                        {std::make_shared<arrow::StringBuilder>()});
    auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));
    ASSERT_TRUE(struct_builder.Append().ok());
    ASSERT_TRUE(string_builder->Append("row0").ok());
    std::shared_ptr<arrow::Array> array;
    ASSERT_TRUE(struct_builder.Finish(&array).ok());
    ASSERT_TRUE(array);
    ::ArrowArray arrow_array;
    ASSERT_TRUE(arrow::ExportArray(*array, &arrow_array).ok());

    RecordBatchBuilder batch_builder(&arrow_array);
    ASSERT_OK_AND_ASSIGN(auto record_batch,
                         batch_builder.SetRowKinds({RecordBatch::RowKind::DELETE}).Finish());
    ASSERT_NOK_WITH_MSG(writer.Write(std::move(record_batch)),
                        "Append only writer can not accept record batch with RowKind DELETE");
    ASSERT_TRUE(ArrowArrayIsReleased(&arrow_array));
    ASSERT_OK(writer.Close());

    auto file_system = std::make_shared<LocalFileSystem>();
    std::vector<std::unique_ptr<BasicFileStatus>> file_status_list;
    ASSERT_OK(file_system->ListDir(dir->Str(), &file_status_list));
    ASSERT_TRUE(file_status_list.empty());
}

}  // namespace paimon::test
