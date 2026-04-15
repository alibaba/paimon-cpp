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

#include "paimon/common/reader/complete_row_kind_batch_reader.h"

#include <map>
#include <optional>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

class CompleteRowKindBatchReaderTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<BatchReader> PrepareCompleteRowKindBatchReader(
        const std::string& file_name, const std::shared_ptr<arrow::Schema> read_schema,
        int32_t batch_size, const std::map<std::string, std::string>& options = {}) const {
        EXPECT_OK_AND_ASSIGN(auto file_format, FileFormatFactory::Get("orc", options));
        EXPECT_OK_AND_ASSIGN(auto reader_builder, file_format->CreateReaderBuilder(batch_size));
        std::shared_ptr<FileSystem> file_system = std::make_shared<LocalFileSystem>();
        EXPECT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(file_name));
        EXPECT_OK_AND_ASSIGN(auto orc_batch_reader, reader_builder->Build(input_stream));

        std::unique_ptr<ArrowSchema> c_schema = std::make_unique<ArrowSchema>();
        auto arrow_status = arrow::ExportSchema(*read_schema, c_schema.get());
        EXPECT_TRUE(arrow_status.ok());
        EXPECT_OK(orc_batch_reader->SetReadSchema(c_schema.get(), /*predicate=*/nullptr,
                                                  /*selection_bitmap=*/std::nullopt));
        return std::make_unique<CompleteRowKindBatchReader>(std::move(orc_batch_reader), pool_);
    }

    std::unique_ptr<BatchReader> PrepareCompleteRowKindBatchReader(
        const std::shared_ptr<arrow::Array>& src_array, int32_t batch_size) const {
        auto file_batch_reader =
            std::make_unique<MockFileBatchReader>(src_array, src_array->type(), batch_size);
        return std::make_unique<CompleteRowKindBatchReader>(std::move(file_batch_reader), pool_);
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CompleteRowKindBatchReaderTest, TestSimple) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/f1=20/"
                            "bucket-0/data-1bd5fd24-4e7d-4ea2-9436-86df0a54b14a-0.orc";
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto reader = PrepareCompleteRowKindBatchReader(file_name, read_schema, /*batch_size=*/1);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    std::vector<DataField> result_fields = read_fields;
    result_fields.insert(result_fields.begin(), SpecialFields::ValueKind());
    auto result_schema = DataField::ConvertDataFieldsToArrowSchema(result_fields);
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(
        arrow::struct_(result_schema->fields()), {R"([
        [0, "Lucy", 20, 1, 14.1],
        [0, "Paul", 20, 1, 18.1]
])"},
        &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->Equals(*result_array));
    reader->Close();
}

TEST_F(CompleteRowKindBatchReaderTest, TestInnerReaderContainsRowKind) {
    std::string file_name = paimon::test::GetDataDir() +
                            "/orc/pk_table_scan_and_read_mor.db/pk_table_scan_and_read_mor/f1=20/"
                            "bucket-0/data-1bd5fd24-4e7d-4ea2-9436-86df0a54b14a-0.orc";
    std::vector<DataField> read_fields = {SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto reader = PrepareCompleteRowKindBatchReader(file_name, read_schema, /*batch_size=*/1);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    auto array_status = arrow::ipc::internal::json::ChunkedArrayFromJSON(
        arrow::struct_(read_schema->fields()), {R"([
        [0, "Lucy", 20, 1, 14.1],
        [0, "Paul", 20, 1, 18.1]
])"},
        &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->Equals(*result_array));
    reader->Close();
}

TEST_F(CompleteRowKindBatchReaderTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::list(arrow::struct_(
                               {field("a", arrow::int64()), field("b", arrow::boolean())}))),
        arrow::field("f1", arrow::map(arrow::struct_({field("a", arrow::int64()),
                                                      field("b", arrow::boolean())}),
                                      arrow::boolean()))};
    auto src_array = arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(fields), R"([
        [[null, [1, true], null], [[[1, true], true]]],
        [[[2, false], null], null],
        [[[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        [null, null]
    ])")
                         .ValueOrDie();
    ASSERT_TRUE(src_array);
    auto reader = PrepareCompleteRowKindBatchReader(src_array, /*batch_size=*/3);
    ASSERT_OK_AND_ASSIGN(auto result_array, ReadResultCollector::CollectResult(reader.get()));

    std::shared_ptr<arrow::ChunkedArray> expected_array;
    arrow::FieldVector read_fields = fields;
    read_fields.insert(read_fields.begin(), arrow::field("_VALUE_KIND", arrow::int8()));
    auto array_status =
        arrow::ipc::internal::json::ChunkedArrayFromJSON(arrow::struct_(read_fields), {R"([
        [0, [null, [1, true], null], [[[1, true], true]]],
        [0, [[2, false], null], null],
        [0, [[2, false], [3, true], [4, null]], [[[1, true], true], [[5, false], null]]],
        [0, null, null]
])"},
                                                         &expected_array);
    ASSERT_TRUE(array_status.ok());
    ASSERT_TRUE(expected_array->Equals(*result_array));
    reader->Close();
}

}  // namespace paimon::test
