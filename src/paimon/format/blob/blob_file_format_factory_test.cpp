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

#include "paimon/format/blob/blob_file_format_factory.h"

#include <map>
#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/data/blob.h"
#include "paimon/defs.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/status.h"
#include "paimon/testing/utils/test_helper.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::blob::test {

TEST(BlobFileFormatFactoryTest, TestIdentifier) {
    BlobFileFormatFactory factory;
    ASSERT_EQ(std::string(factory.Identifier()), "blob");
    ASSERT_OK_AND_ASSIGN(auto file_format, factory.Create({}));
    ASSERT_EQ(file_format->Identifier(), "blob");
}

TEST(BlobFileFormatFactoryTest, TestWriteNullOptionPropagation) {
    // Verifies the option flows through the production path
    // FileFormatFactory::Get -> BlobFileFormat -> BlobWriterBuilder -> BlobFormatWriter.
    std::unique_ptr<paimon::test::UniqueTestDirectory> dir =
        paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    std::shared_ptr<FileSystem> fs = std::make_shared<LocalFileSystem>();
    auto struct_type = arrow::struct_({BlobUtils::ToArrowField("blob_col", true)});
    ASSERT_OK_AND_ASSIGN(std::shared_ptr<Blob> missing_blob,
                         Blob::FromPath(dir->Str() + "/not_exist_file", /*offset=*/0,
                                        /*length=*/10));

    auto write_once = [&](const std::map<std::string, std::string>& options,
                          const std::string& file_name) -> Status {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                               FileFormatFactory::Get("blob", options));
        auto schema = arrow::schema(struct_type->fields());
        ::ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*schema, &c_schema));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<WriterBuilder> writer_builder,
                               format->CreateWriterBuilder(&c_schema, /*batch_size=*/1024));
        // The blob writer builder is a SpecificFSWriterBuilder by construction.
        static_cast<SpecificFSWriterBuilder*>(writer_builder.get())->WithFileSystem(fs);
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                               fs->Create(dir->Str() + "/" + file_name, /*overwrite=*/true));
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> writer,
                               writer_builder->Build(out, "none"));
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                               paimon::test::TestHelper::MakeBlobDescriptorArray(
                                   struct_type, missing_blob, GetDefaultPool()));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        return writer->Finish();
    };

    // Without the option, writing the missing descriptor fails.
    ASSERT_NOK_WITH_MSG(write_once({}, "no_option.blob"), "not exists");
    // The option set in the format options map reaches the writer.
    ASSERT_OK(write_once({{Options::BLOB_WRITE_NULL_ON_MISSING_FILE, "true"}}, "with_option.blob"));
}

}  // namespace paimon::blob::test
