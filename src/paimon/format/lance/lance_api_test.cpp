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

#include "lance_lib/lance_api.h"

#include <memory>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/format/lance/lance_utils.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::lance::test {
TEST(LanceApiTest, TestInvalidCaseForWrite) {
    auto dir = paimon::test::UniqueTestDirectory::Create();
    ASSERT_TRUE(dir);
    arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                 arrow::field("f2", arrow::utf8())};
    auto schema = arrow::schema(fields);
    std::string error_message;
    error_message.resize(100, '\0');
    {
        // test create_writer: invalid file path
        LanceFileWriter* file_writer = nullptr;
        int32_t ret = create_writer(/*file_path=*/nullptr, /*schema_ptr=*/nullptr, &file_writer,
                                    error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_path or schema_ptr or file_writer_ptr");
    }
    {
        // test create_writer: invalid file_schema
        LanceFileWriter* file_writer = nullptr;
        std::string file_path = dir->Str() + "/test0.lance";
        int32_t ret = create_writer(/*file_path=*/file_path.data(), /*schema_ptr=*/nullptr,
                                    &file_writer, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_path or schema_ptr or file_writer_ptr");
    }
    {
        // test create_writer: invalid file_writer_ptr
        std::string file_path = dir->Str() + "/test1.lance";
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        int32_t ret = create_writer(/*file_path=*/file_path.data(), &c_schema, nullptr,
                                    error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_path or schema_ptr or file_writer_ptr");
        ArrowSchemaRelease(&c_schema);
    }
    {
        // test create_writer: duplicate field name
        std::string file_path = dir->Str() + "/test1.lance";
        arrow::FieldVector fields = {arrow::field("f1", arrow::int32()),
                                     arrow::field("f1", arrow::utf8())};
        auto schema = arrow::schema(fields);
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        LanceFileWriter* file_writer = nullptr;
        int32_t ret = create_writer(/*file_path=*/file_path.data(), &c_schema, &file_writer,
                                    error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message), "Duplicate field name");
        ArrowSchemaRelease(&c_schema);
    }
    {
        // test create_writer: unsupported data type map
        std::string file_path = dir->Str() + "/test1.lance";
        arrow::FieldVector fields = {arrow::field("f1", arrow::map(arrow::int8(), arrow::int16()))};
        auto schema = arrow::schema(fields);
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        LanceFileWriter* file_writer = nullptr;
        int32_t ret = create_writer(/*file_path=*/file_path.data(), &c_schema, &file_writer,
                                    error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message), "Unsupported data type: Map");
        ArrowSchemaRelease(&c_schema);
    }
    {
        // test write: invalid file_writer_ptr & input_array_ptr & input_schema_ptr
        int32_t ret = write_c_arrow_array(/*file_writer_ptr=*/nullptr, /*input_array_ptr=*/nullptr,
                                          /*input_schema_ptr=*/nullptr, error_message.data(),
                                          error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_writer_ptr or "
                            "input_array_ptr or input_schema_ptr");
    }
    {
        // test write: mismatched writer schema and batch schema
        std::string file_path = dir->Str() + "/test2.lance";
        ArrowSchema c_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_schema).ok());
        LanceFileWriter* file_writer = nullptr;
        int32_t ret = create_writer(/*file_path=*/file_path.data(), /*schema_ptr=*/&c_schema,
                                    &file_writer, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
        arrow::FieldVector mismatched_fields = {arrow::field("f1", arrow::int32())};

        auto mismatched_array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(mismatched_fields), R"([
            [1], [2], [3] ])")
                .ValueOrDie());
        ArrowArray mismatched_c_array;
        ArrowSchema mismatched_c_schema;
        ASSERT_TRUE(
            arrow::ExportArray(*mismatched_array, &mismatched_c_array, &mismatched_c_schema).ok());
        ret = write_c_arrow_array(file_writer, &mismatched_c_array, &mismatched_c_schema,
                                  error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Failed to write batch: Invalid user input: Cannot write batch.  The "
                            "batch was missing the column");
        ret = release_writer(file_writer, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
    }
    {
        // test writer_tell: invalid file_writer_ptr and tell_pos
        int32_t ret = writer_tell(/*file_writer_ptr=*/nullptr, /*tell_pos=*/nullptr,
                                  error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_writer_ptr or tell_pos");
    }
    {
        // test finish: invalid file_writer_ptr
        int32_t ret =
            finish_writer(/*file_writer_ptr=*/nullptr, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_writer_ptr");
    }
    {
        // test release: invalid file_writer_ptr
        int32_t ret =
            release_writer(/*file_writer_ptr=*/nullptr, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_writer_ptr");
    }
}

TEST(LanceApiTest, TestInvalidCaseForRead) {
    std::string path = paimon::test::GetDataDir() + "/lance/test.lance";
    std::string error_message;
    error_message.resize(100, '\0');
    {
        // test create_reader: null path
        LanceFileReader* reader = nullptr;
        int32_t ret = create_reader(/*c_file_path=*/nullptr, &reader, error_message.data(),
                                    error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_path or file_reader_ptr");
    }
    {
        // test create_reader: invalid reader
        int32_t ret = create_reader(/*c_file_path=*/path.data(), nullptr, error_message.data(),
                                    error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_path or file_reader_ptr");
    }
    {
        // test create_reader: invalid path
        LanceFileReader* reader = nullptr;
        std::string non_exist_path = "test_data/lance/non-exist.lance";
        int32_t ret = create_reader(/*c_file_path=*/non_exist_path.data(), &reader,
                                    error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Failed to open file for scheduler");
    }
    {
        // test create_stream_reader: null reader
        LanceReaderAdapter* stream_reader = nullptr;
        int32_t ret = create_stream_reader(
            /*file_reader_ptr=*/nullptr, &stream_reader, /*batch_size=*/1,
            /*batch_readahead=*/1, /*projection_column_names=*/nullptr,
            /*projection_column_count=*/0, /*read_row_ids=*/nullptr,
            /*read_row_count=*/0, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_reader_ptr or stream_reader_ptr");
    }
    {
        // test create_stream_reader: null stream reader
        LanceFileReader* reader = nullptr;
        int32_t ret = create_reader(/*c_file_path=*/path.data(), &reader, error_message.data(),
                                    error_message.size());
        ASSERT_EQ(ret, 0);
        ASSERT_TRUE(reader);
        ret =
            create_stream_reader(/*file_reader_ptr=*/reader, nullptr, /*batch_size=*/1,
                                 /*batch_readahead=*/1, /*projection_column_names=*/nullptr,
                                 /*projection_column_count=*/0, /*read_row_ids=*/nullptr,
                                 /*read_row_count=*/0, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_reader_ptr or stream_reader_ptr");
        ret = release_reader(reader, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
    }
    {
        // test next_batch: null input
        int32_t ret = next_batch(nullptr, nullptr, nullptr, nullptr, error_message.data(),
                                 error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for stream_reader_ptr");
    }
    {
        // test release_reader: null input
        int32_t ret = release_reader(nullptr, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_reader_ptr");
    }
    {
        // test release_stream_reader: null input
        int32_t ret = release_stream_reader(nullptr, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for stream_reader_ptr");
    }
    {
        // test get_schema: null input
        int32_t ret = get_schema(/*file_reader_ptr=*/nullptr, /*output_schema_ptr=*/nullptr,
                                 error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for file_reader_ptr or output_schema_ptr");
    }
    {
        // test num_rows: null input
        int32_t ret = num_rows(/*file_reader_ptr=*/nullptr, /*num_rows=*/nullptr,
                               error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for file_reader_ptr or num_rows");
    }
    {
        // test create_stream_reader: project non-exist field
        LanceFileReader* reader = nullptr;
        LanceReaderAdapter* stream_reader = nullptr;
        int32_t ret = create_reader(/*c_file_path=*/path.data(), &reader, error_message.data(),
                                    error_message.size());
        ASSERT_EQ(ret, 0);
        ASSERT_TRUE(reader);
        std::string non_exist_col = "non-exist";
        std::vector<const char*> c_field_names = {non_exist_col.data()};
        ret = create_stream_reader(
            reader, &stream_reader, /*batch_size=*/1,
            /*batch_readahead=*/1, /*projection_column_names=*/c_field_names.data(),
            /*projection_column_count=*/1, /*read_row_ids=*/nullptr,
            /*read_row_count=*/0, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Column non-exist does not exist");
        ret = release_reader(reader, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
    }
    {
        // test create_stream_reader: null projection_column_names and >0 projection_column_count
        LanceFileReader* reader = nullptr;
        LanceReaderAdapter* stream_reader = nullptr;
        int32_t ret = create_reader(/*c_file_path=*/path.data(), &reader, error_message.data(),
                                    error_message.size());
        ASSERT_EQ(ret, 0);
        ASSERT_TRUE(reader);
        ret =
            create_stream_reader(reader, &stream_reader, /*batch_size=*/1,
                                 /*batch_readahead=*/1, /*projection_column_names=*/nullptr,
                                 /*projection_column_count=*/1, /*read_row_ids=*/nullptr,
                                 /*read_row_count=*/0, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(LanceToPaimonStatus(ret, error_message),
                            "Null pointer passed to function for projection_column_names, while "
                            "projection_column_count > 0");
        ret = release_reader(reader, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
    }
    {
        // test create_stream_reader: null read_row_ids and >0 read_row_count
        LanceFileReader* reader = nullptr;
        LanceReaderAdapter* stream_reader = nullptr;
        int32_t ret = create_reader(/*c_file_path=*/path.data(), &reader, error_message.data(),
                                    error_message.size());
        ASSERT_EQ(ret, 0);
        ASSERT_TRUE(reader);
        ret =
            create_stream_reader(reader, &stream_reader, /*batch_size=*/1,
                                 /*batch_readahead=*/1, /*projection_column_names=*/nullptr,
                                 /*projection_column_count=*/0, /*read_row_ids=*/nullptr,
                                 /*read_row_count=*/1, error_message.data(), error_message.size());
        ASSERT_NOK_WITH_MSG(
            LanceToPaimonStatus(ret, error_message),
            "Null pointer passed to function for read_row_ids, while read_row_count > 0");
        ret = release_reader(reader, error_message.data(), error_message.size());
        ASSERT_EQ(ret, 0);
    }
}
}  // namespace paimon::lance::test
