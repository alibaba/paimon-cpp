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

#include "paimon/core/io/single_file_writer.h"

#include <map>
#include <utility>

#include "arrow/api.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class SimpleSingleFileWriter : public SingleFileWriter<int32_t, bool> {
 public:
    SimpleSingleFileWriter(const std::string& compression,
                           std::function<Status(int32_t, ArrowArray*)> converter)
        : SingleFileWriter<int32_t, bool>(compression, converter) {}

    Result<bool> GetResult() override {
        return true;
    }
};

TEST(SingleFileWriterTest, TestSimple) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/single-file";
    auto data_type = arrow::struct_({arrow::field("col", arrow::int32())});
    auto converter = [&](int32_t value, ::ArrowArray* dest) -> Status {
        std::string value_str = "[[" + std::to_string(value) + "]]";
        auto array =
            arrow::ipc::internal::json::ArrayFromJSON(data_type, value_str.c_str()).ValueOrDie();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, dest));
        return Status::OK();
    };
    SimpleSingleFileWriter writer("zstd", converter);
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::MANIFEST_FORMAT, "orc"}, {Options::FILE_FORMAT, "orc"}}));
    auto file_format = options.GetWriteFileFormat();
    auto file_system = options.GetFileSystem();
    ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportType(*data_type, &arrow_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<WriterBuilder> writer_builder,
                         file_format->CreateWriterBuilder(&arrow_schema, /*batch_size=*/100));
    ASSERT_OK(writer.Init(file_system, file_path, writer_builder));
    ASSERT_EQ(file_path, writer.GetPath());
    ASSERT_OK(writer.Write(100));
    ASSERT_EQ(1, writer.RecordCount());
    ASSERT_NOK_WITH_MSG(writer.GetAbortExecutor(), "Writer should be closed");
    ASSERT_OK(writer.Close());
    ASSERT_OK_AND_ASSIGN(auto file_status, file_system->GetFileStatus(file_path));
    ASSERT_GT(file_status->GetLen(), 0);
    ASSERT_OK_AND_ASSIGN(auto abort_executor, writer.GetAbortExecutor());
    abort_executor.Abort();
    ASSERT_OK_AND_ASSIGN(auto exist, file_system->Exists(file_path));
    ASSERT_FALSE(exist);
}

TEST(SingleFileWriterTest, TestInvalidConvert) {
    auto dir = UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::string file_path = dir->Str() + "/single-file";
    auto data_type = arrow::struct_({arrow::field("col", arrow::int32())});
    auto converter = [&](int32_t value, ::ArrowArray* dest) -> Status {
        return Status::Invalid("");
    };
    SimpleSingleFileWriter writer("zstd", converter);
    ASSERT_OK_AND_ASSIGN(
        CoreOptions options,
        CoreOptions::FromMap({{Options::MANIFEST_FORMAT, "orc"}, {Options::FILE_FORMAT, "orc"}}));
    auto file_format = options.GetWriteFileFormat();
    auto file_system = options.GetFileSystem();
    ArrowSchema arrow_schema;
    ASSERT_TRUE(arrow::ExportType(*data_type, &arrow_schema).ok());
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<WriterBuilder> writer_builder,
                         file_format->CreateWriterBuilder(&arrow_schema, /*batch_size=*/100));
    ASSERT_OK(writer.Init(file_system, file_path, writer_builder));
    ASSERT_NOK(writer.Write(100));
    writer.Abort();
    ASSERT_OK_AND_ASSIGN(auto exist, file_system->Exists(file_path));
    ASSERT_FALSE(exist);
}

}  // namespace paimon::test
