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

#include "paimon/core/io/complete_row_tracking_fields_reader.h"

#include <memory>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/status.h"
#include "paimon/testing/mock/mock_file_batch_reader.h"
#include "paimon/testing/utils/read_result_collector.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class CompleteRowTrackingFieldsBatchReaderTest : public testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }

    void CheckResult(const std::shared_ptr<arrow::Array>& src_array, int64_t first_row_id,
                     int64_t snapshot_id, const std::shared_ptr<arrow::Schema>& read_schema,
                     const std::shared_ptr<arrow::Array>& expected_array) const {
        for (auto batch_size : {1, 2, 3, 4, 7, 10}) {
            auto file_batch_reader =
                std::make_unique<MockFileBatchReader>(src_array, src_array->type(), batch_size);
            auto complete_row_tracking_fields_reader =
                std::make_shared<CompleteRowTrackingFieldsBatchReader>(
                    std::move(file_batch_reader), first_row_id, snapshot_id, pool_);
            ArrowSchema c_read_schema;
            ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_read_schema).ok());
            ASSERT_OK(complete_row_tracking_fields_reader->SetReadSchema(
                &c_read_schema,
                /*predicate=*/nullptr,
                /*selection_bitmap=*/std::nullopt));
            ASSERT_OK_AND_ASSIGN(auto result_with_special_fields,
                                 paimon::test::ReadResultCollector::CollectResult(
                                     complete_row_tracking_fields_reader.get()));
            complete_row_tracking_fields_reader->Close();
            auto expected_chunk_array = std::make_shared<arrow::ChunkedArray>(expected_array);
            ASSERT_TRUE(result_with_special_fields->Equals(expected_chunk_array));
        }
    }

    void CheckSetReadSchema(
        const std::shared_ptr<arrow::Schema>& file_schema,
        const std::shared_ptr<arrow::Schema>& read_schema,
        const std::shared_ptr<arrow::Schema>& expected_schema_for_inner_reader) const {
        auto file_batch_reader = std::make_unique<MockFileBatchReader>(
            /*data=*/nullptr, arrow::struct_(file_schema->fields()), /*read_batch_size=*/1);
        auto complete_row_tracking_fields_reader =
            std::make_shared<CompleteRowTrackingFieldsBatchReader>(
                std::move(file_batch_reader), /*first_row_id=*/10, /*snapshot_id=*/1, pool_);
        ArrowSchema c_read_schema;
        ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_read_schema).ok());
        ASSERT_OK(
            complete_row_tracking_fields_reader->SetReadSchema(&c_read_schema,
                                                               /*predicate=*/nullptr,
                                                               /*selection_bitmap=*/std::nullopt));
        auto typed_file_batch_reader =
            dynamic_cast<MockFileBatchReader*>(complete_row_tracking_fields_reader->reader_.get());
        ASSERT_TRUE(typed_file_batch_reader);
        ASSERT_TRUE(
            expected_schema_for_inner_reader->Equals(typed_file_batch_reader->read_schema_));
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestSetReadSchema) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    {
        // test file contain no special fields
        auto file_schema = arrow::schema({fields[0], fields[1]});
        auto read_schema = arrow::schema(fields);
        auto expected_schema_for_inner_reader = file_schema;
        CheckSetReadSchema(file_schema, read_schema, expected_schema_for_inner_reader);
    }
    {
        // test file contain special fields
        auto file_schema = arrow::schema(fields);
        auto read_schema = arrow::schema(fields);
        auto expected_schema_for_inner_reader = file_schema;
        CheckSetReadSchema(file_schema, read_schema, expected_schema_for_inner_reader);
    }
    {
        // test read partial fields, file contain special fields
        auto file_schema = arrow::schema(fields);
        auto read_schema = arrow::schema({fields[1], fields[2]});
        auto expected_schema_for_inner_reader = read_schema;
        CheckSetReadSchema(file_schema, read_schema, expected_schema_for_inner_reader);
    }
    {
        // test read partial fields, file contain no special fields
        auto file_schema = arrow::schema({fields[0], fields[1]});
        auto read_schema = arrow::schema({fields[1], fields[2]});
        auto expected_schema_for_inner_reader = arrow::schema({fields[1]});
        CheckSetReadSchema(file_schema, read_schema, expected_schema_for_inner_reader);
    }
    {
        // test file contain partial fields
        auto file_schema = arrow::schema({fields[1], fields[2], fields[3]});
        auto read_schema = arrow::schema({fields[1], fields[3]});
        auto expected_schema_for_inner_reader = read_schema;
        CheckSetReadSchema(file_schema, read_schema, expected_schema_for_inner_reader);
    }
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestWithNoRowTrackingFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_({fields[0], fields[1]});

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0, 30],
        [0, 31],
        [1, 32],
        [2, 33]
    ])")
            .ValueOrDie());
    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [0, 30, 100, 4],
        [0, 31, 101, 4],
        [1, 32, 102, 4],
        [2, 33, 103, 4]
    ])")
            .ValueOrDie());

    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/4, read_schema, target_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestWithAllNotNullRowTrackingFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0, 30, 100, 4],
        [0, 31, 101, 4],
        [1, 32, 102, 4],
        [2, 33, 103, 4]
    ])")
            .ValueOrDie());

    CheckResult(src_array, /*first_row_id=*/200, /*snapshot_id=*/8, read_schema, src_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestWithPartialNullRowTrackingFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0, 30, null, null],
        [0, 31, null, 3],
        [1, 32, null, 4],
        [2, 33, 200, 4]
    ])")
            .ValueOrDie());

    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [0, 30, 100, 8],
        [0, 31, 101, 3],
        [1, 32, 102, 4],
        [2, 33, 200, 4]
    ])")
            .ValueOrDie());

    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/8, read_schema, target_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestWithAllNullRowTrackingFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0, 30, null, null],
        [0, 31, null, null],
        [1, 32, null, null],
        [2, 33, null, null]
    ])")
            .ValueOrDie());

    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [0, 30, 100, 8],
        [0, 31, 101, 8],
        [1, 32, 102, 8],
        [2, 33, 103, 8]
    ])")
            .ValueOrDie());

    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/8, read_schema, target_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest,
       TestPartialNullRowTrackingFieldsWithUnorderFields) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema =
        arrow::schema({fields[0], fields[2], fields[3], fields[1]});
    std::shared_ptr<arrow::DataType> read_type =
        arrow::struct_({fields[0], fields[2], fields[3], fields[1]});
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0, 30, null, null],
        [0, 31, null, 3],
        [1, 32, null, 4],
        [2, 33, 200, 4]
    ])")
            .ValueOrDie());

    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [0, 100, 8, 30],
        [0, 101, 3, 31],
        [1, 102, 4, 32],
        [2, 200, 4, 33]
    ])")
            .ValueOrDie());
    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/8, read_schema, target_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::map(arrow::int8(), arrow::int16())),
        arrow::field("f2", arrow::list(arrow::float32())),
        arrow::field("f3", arrow::struct_({arrow::field("f0", arrow::boolean()),
                                           arrow::field("f1", arrow::int64())})),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_(fields);

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], null, null],
        [[[0, 1]], [0.1, 0.3], [true, 1], null, 3],
        [[[10, 10]], [1.1, 1.2], [false, 12], null, 4],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], 200, 4]
    ])")
            .ValueOrDie());

    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [[[0, 0]], [0.1, 0.2], [true, 2], 100, 8],
        [[[0, 1]], [0.1, 0.3], [true, 1], 101, 3],
        [[[10, 10]], [1.1, 1.2], [false, 12], 102, 4],
        [[[127, 32767], [-128, -32768]], [1.1, 1.2], [false, 2222], 200, 4]
    ])")
            .ValueOrDie());
    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/8, read_schema, target_array);
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestInvalidWithReadNonExistField) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("f1", arrow::struct_({arrow::field("field_non_exist", arrow::int32())})),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(
        {fields[0], arrow::field("field_non_exist", arrow::int32()), fields[2], fields[3]});
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(
        {fields[0], arrow::field("field_non_exist", arrow::int32()), fields[2], fields[3]});
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_({fields[0]});

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0],
        [0],
        [1],
        [2]
    ])")
            .ValueOrDie());

    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, file_type, /*batch_size=*/10);

    auto complete_row_tracking_fields_reader =
        std::make_shared<CompleteRowTrackingFieldsBatchReader>(
            std::move(file_batch_reader), /*first_row_id=*/100, /*snapshot_id=*/8, pool_);
    ArrowSchema c_read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_read_schema).ok());
    ASSERT_OK(
        complete_row_tracking_fields_reader->SetReadSchema(&c_read_schema,
                                                           /*predicate=*/nullptr,
                                                           /*selection_bitmap=*/std::nullopt));
    ASSERT_NOK_WITH_MSG(
        complete_row_tracking_fields_reader->NextBatchWithBitmap(),
        "Data file does not contain field field_non_exist in CompleteRowTrackingFieldsBatchReader");
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestInvalidNextBatchBeforeSetReadSchema) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema({fields[0], fields[1]});
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_({fields[0], fields[1]});
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_({fields[0]});

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0],
        [0],
        [1],
        [2]
    ])")
            .ValueOrDie());

    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, file_type, /*batch_size=*/10);

    auto complete_row_tracking_fields_reader =
        std::make_shared<CompleteRowTrackingFieldsBatchReader>(
            std::move(file_batch_reader), /*first_row_id=*/100, /*snapshot_id=*/8, pool_);
    ASSERT_NOK_WITH_MSG(complete_row_tracking_fields_reader->NextBatchWithBitmap(),
                        "in CompleteRowTrackingFieldsBatchReader SetReadSchema is supposed to be "
                        "called before NextBatch");
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestInvalidNullFirstRowId) {
    arrow::FieldVector fields = {
        arrow::field("f0", arrow::int32()),
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema({fields[0], fields[1]});
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_({fields[0], fields[1]});
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_({fields[0]});

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [0],
        [0],
        [1],
        [2]
    ])")
            .ValueOrDie());

    auto file_batch_reader =
        std::make_unique<MockFileBatchReader>(src_array, file_type, /*batch_size=*/10);

    auto complete_row_tracking_fields_reader =
        std::make_shared<CompleteRowTrackingFieldsBatchReader>(
            std::move(file_batch_reader), /*first_row_id=*/std::nullopt, /*snapshot_id=*/8, pool_);
    ArrowSchema c_read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*read_schema, &c_read_schema).ok());
    ASSERT_OK(
        complete_row_tracking_fields_reader->SetReadSchema(&c_read_schema,
                                                           /*predicate=*/nullptr,
                                                           /*selection_bitmap=*/std::nullopt));
    ASSERT_NOK_WITH_MSG(complete_row_tracking_fields_reader->NextBatchWithBitmap(),
                        "unexpected: read _ROW_ID special field, but first row id is null in meta");
}

TEST_F(CompleteRowTrackingFieldsBatchReaderTest, TestOnlyReadRowTrackingFields) {
    arrow::FieldVector fields = {
        arrow::field("_ROW_ID", arrow::int64()),
        arrow::field("_SEQUENCE_NUMBER", arrow::int64()),
    };
    std::shared_ptr<arrow::Schema> read_schema = arrow::schema(fields);
    std::shared_ptr<arrow::DataType> read_type = arrow::struct_(fields);
    std::shared_ptr<arrow::DataType> file_type = arrow::struct_({});

    auto src_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(file_type, R"([
        [],
        [],
        [],
        []
    ])")
            .ValueOrDie());
    auto target_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(read_type, R"([
        [100, 4],
        [101, 4],
        [102, 4],
        [103, 4]
    ])")
            .ValueOrDie());

    CheckResult(src_array, /*first_row_id=*/100, /*snapshot_id=*/4, read_schema, target_array);
}

}  // namespace paimon::test
