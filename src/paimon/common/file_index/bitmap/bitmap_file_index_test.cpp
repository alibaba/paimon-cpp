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

#include "paimon/common/file_index/bitmap/bitmap_file_index.h"

#include <utility>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
namespace paimon::test {
class BitmapIndexTest : public ::testing::Test {
 public:
    void SetUp() override {
        pool_ = GetDefaultPool();
    }
    void TearDown() override {
        pool_.reset();
    }

    std::unique_ptr<::ArrowSchema> CreateArrowSchema(
        const std::shared_ptr<arrow::DataType>& data_type) const {
        auto schema = arrow::schema({arrow::field("f0", data_type)});
        auto c_schema = std::make_unique<::ArrowSchema>();
        EXPECT_TRUE(arrow::ExportSchema(*schema, c_schema.get()).ok());
        return c_schema;
    }

    void CheckResult(const std::shared_ptr<FileIndexResult>& result,
                     const std::vector<int32_t>& expected) const {
        auto typed_result = std::dynamic_pointer_cast<BitmapIndexResult>(result);
        ASSERT_TRUE(typed_result);
        ASSERT_OK_AND_ASSIGN(const RoaringBitmap32* bitmap, typed_result->GetBitmap());
        ASSERT_TRUE(bitmap);
        ASSERT_EQ(*(typed_result->GetBitmap().value()), RoaringBitmap32::From(expected))
            << "result=" << (typed_result->GetBitmap().value())->ToString()
            << ", expected=" << RoaringBitmap32::From(expected).ToString();
    };

    Result<PAIMON_UNIQUE_PTR<Bytes>> WriteIndex(const std::shared_ptr<arrow::DataType>& type,
                                                int32_t version,
                                                const std::shared_ptr<arrow::Array>& array) const {
        auto arrow_schema = arrow::schema({arrow::field("f0", type)});
        BitmapFileIndex file_index({{"version", std::to_string(version)}});
        ArrowSchema c_schema;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*arrow_schema, &c_schema));
        PAIMON_ASSIGN_OR_RAISE(auto writer, file_index.CreateWriter(&c_schema, pool_));
        ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
        PAIMON_RETURN_NOT_OK(writer->AddBatch(&c_array));
        return writer->SerializedBytes();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BitmapIndexTest, TestStringType) {
    // data: a, null, b, null, a
    auto type = arrow::utf8();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        ["a"],
        [null],
        ["b"],
        [null],
        ["a"]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };

    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);

        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);

        Literal lit_a(FieldType::STRING, "a", 1);
        CheckResult(reader->VisitEqual(lit_a).value(), {0, 4});

        Literal lit_b(FieldType::STRING, "b", 1);
        CheckResult(reader->VisitEqual(lit_b).value(), {2});

        CheckResult(reader->VisitIsNull().value(), {1, 3});

        CheckResult(reader->VisitIn({lit_a, lit_b}).value(), {0, 2, 4});
        CheckResult(reader->VisitNotIn({lit_a, lit_b}).value(), {});

        // non-exist
        Literal lit_c(FieldType::STRING, "c", 1);
        ASSERT_FALSE(reader->VisitEqual(lit_c).value()->IsRemain().value());
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {
            1, 0, 0, 0,  5,  0,  0,  0,  2,  1,  0, 0, 0, 0, 0, 0, 0,  1, 97, 0, 0,  0, 20, 0,
            0, 0, 1, 98, -1, -1, -1, -3, 58, 48, 0, 0, 1, 0, 0, 0, 0,  0, 1,  0, 16, 0, 0,  0,
            1, 0, 3, 0,  58, 48, 0,  0,  1,  0,  0, 0, 0, 0, 1, 0, 16, 0, 0,  0, 0,  0, 4,  0};

        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0,  0,  0,  5, 0,  0, 0, 2, 1,  0, 0, 0, 0,  0,  0,  0,  20, 0,  0,  0,
            1,  0,  0,  0,  1, 97, 0, 0, 0, 0,  0, 0, 0, 30, 0,  0,  0,  2,  0,  0,  0,
            1,  97, 0,  0,  0, 20, 0, 0, 0, 20, 0, 0, 0, 1,  98, -1, -1, -1, -3, -1, -1,
            -1, -1, 58, 48, 0, 0,  1, 0, 0, 0,  0, 0, 1, 0,  16, 0,  0,  0,  1,  0,  3,
            0,  58, 48, 0,  0, 1,  0, 0, 0, 0,  0, 1, 0, 16, 0,  0,  0,  0,  0,  4,  0};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestBooleanType) {
    // data: true, false, true, false, null
    auto type = arrow::boolean();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [true],
        [false],
        [true],
        [false],
        [null]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit_true(true);
        Literal lit_false(false);
        CheckResult(reader->VisitEqual(lit_true).value(), {0, 2});
        CheckResult(reader->VisitEqual(lit_false).value(), {1, 3});
        CheckResult(reader->VisitIsNull().value(), {4});
        CheckResult(reader->VisitIn({lit_true, lit_false}).value(), {0, 1, 2, 3});
        CheckResult(reader->VisitNotIn({lit_true, lit_false}).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {1, 0, 0, 0, 5,  0, 0, 0,  2,  1,  -1, -1, -1, -5, 0, 0,
                                         0, 0, 0, 1, 0,  0, 0, 20, 58, 48, 0,  0,  1,  0,  0, 0,
                                         0, 0, 1, 0, 16, 0, 0, 0,  1,  0,  3,  0,  58, 48, 0, 0,
                                         1, 0, 0, 0, 0,  0, 1, 0,  16, 0,  0,  0,  0,  0,  2, 0};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0,  0,  0, 5,  0,  0,  0, 2, 1, -1, -1, -1, -5, 0, 0, 0, 18, 0, 0,  0, 1, 0, 0,
            0,  0,  0,  0, 0,  0,  22, 0, 0, 0, 2,  0,  0,  0,  0, 0, 0, 0,  0, 20, 1, 0, 0, 0,
            20, 0,  0,  0, 20, 58, 48, 0, 0, 1, 0,  0,  0,  0,  0, 1, 0, 16, 0, 0,  0, 1, 0, 3,
            0,  58, 48, 0, 0,  1,  0,  0, 0, 0, 0,  1,  0,  16, 0, 0, 0, 0,  0, 2,  0};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestTinyIntType) {
    // data: null, null, null, 1, 1
    auto type = arrow::int8();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [null],
        [null],
        [null],
        [1],
        [1]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit1(static_cast<int8_t>(1));
        Literal lit2(static_cast<int8_t>(2));
        CheckResult(reader->VisitEqual(lit1).value(), {3, 4});
        CheckResult(reader->VisitIsNull().value(), {0, 1, 2});
        CheckResult(reader->VisitEqual(lit2).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {1, 0, 0,  0,  5,  0,  0, 0, 1, 1,  0,  0, 0, 0, 1, 0,
                                         0, 0, 22, 58, 48, 0,  0, 1, 0, 0,  0,  0, 0, 2, 0, 16,
                                         0, 0, 0,  0,  0,  1,  0, 2, 0, 58, 48, 0, 0, 1, 0, 0,
                                         0, 0, 0,  1,  0,  16, 0, 0, 0, 3,  0,  4, 0};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0,  0, 0, 5, 0, 0, 0, 1,  1, 0, 0, 0,  0, 0, 0, 0, 22, 0, 0, 0, 1,
            1,  0,  0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 1,  1, 0, 0, 0, 22, 0, 0, 0, 20,
            58, 48, 0, 0, 1, 0, 0, 0, 0,  0, 2, 0, 16, 0, 0, 0, 0, 0,  1, 0, 2, 0,
            58, 48, 0, 0, 1, 0, 0, 0, 0,  0, 1, 0, 16, 0, 0, 0, 3, 0,  4, 0};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestSmallIntType) {
    // data: null, 1, 1, 1, 1
    auto type = arrow::int16();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [null],
        [1],
        [1],
        [1],
        [1]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit1(static_cast<int16_t>(1));
        Literal lit2(static_cast<int16_t>(2));
        CheckResult(reader->VisitEqual(lit1).value(), {1, 2, 3, 4});
        CheckResult(reader->VisitIsNull().value(), {0});
        CheckResult(reader->VisitEqual(lit2).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    // as unique non-null value cardinality = 1, can test compatible with java
    {
        // test v1 version
        std::vector<char> index_bytes = {1, 0, 0,  0,  5, 0, 0, 0, 1, 1, -1, -1, -1, -1, 0, 1, 0, 0,
                                         0, 0, 59, 48, 0, 0, 1, 0, 0, 3, 0,  1,  0,  1,  0, 3, 0};
        check_result(index_bytes.data(), index_bytes.size());

        // test compatible with java
        ASSERT_OK_AND_ASSIGN(auto index_bytes2, write_data(/*version=*/1));
        ASSERT_EQ(index_bytes, std::vector<char>(index_bytes2->data(),
                                                 index_bytes2->data() + index_bytes2->size()));
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {2, 0,  0, 0, 5, 0, 0, 0, 1, 1, -1, -1, -1, -1, 0,  0,
                                         0, 18, 0, 0, 0, 1, 0, 1, 0, 0, 0,  0,  0,  0,  0,  14,
                                         0, 0,  0, 1, 0, 1, 0, 0, 0, 0, 0,  0,  0,  15, 59, 48,
                                         0, 0,  1, 0, 0, 3, 0, 1, 0, 1, 0,  3,  0};
        check_result(index_bytes.data(), index_bytes.size());

        // test compatible with java
        ASSERT_OK_AND_ASSIGN(auto index_bytes2, write_data(/*version=*/2));
        ASSERT_EQ(index_bytes, std::vector<char>(index_bytes2->data(),
                                                 index_bytes2->data() + index_bytes2->size()));
    }
}

TEST_F(BitmapIndexTest, TestBigIntType) {
    // data: 1, 2, 3
    auto type = arrow::int64();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [1],
        [2],
        [3]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit1(static_cast<int64_t>(1));
        Literal lit3(static_cast<int64_t>(3));
        Literal lit4(static_cast<int64_t>(4));
        CheckResult(reader->VisitEqual(lit1).value(), {0});
        CheckResult(reader->VisitEqual(lit3).value(), {2});
        CheckResult(reader->VisitEqual(lit4).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {1,  0,  0,  0,  3,  0,  0, 0, 3, 0, 0,  0,  0,  0, 0,  0,
                                         0,  1,  -1, -1, -1, -1, 0, 0, 0, 0, 0,  0,  0,  2, -1, -1,
                                         -1, -2, 0,  0,  0,  0,  0, 0, 0, 3, -1, -1, -1, -3};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0,  0,  0,  3,  0,  0,  0,  3,  0, 0, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,
            1,  0,  0,  0,  0,  0,  0,  0,  52, 0, 0, 0,  3,  0,  0,  0,  0,  0,  0,  0,  1,
            -1, -1, -1, -1, -1, -1, -1, -1, 0,  0, 0, 0,  0,  0,  0,  2,  -1, -1, -1, -2, -1,
            -1, -1, -1, 0,  0,  0,  0,  0,  0,  0, 3, -1, -1, -1, -3, -1, -1, -1, -1};
        check_result(index_bytes.data(), index_bytes.size());

        // test compatible with java
        ASSERT_OK_AND_ASSIGN(auto index_bytes2, write_data(/*version=*/2));
        ASSERT_EQ(index_bytes, std::vector<char>(index_bytes2->data(),
                                                 index_bytes2->data() + index_bytes2->size()));
    }
}

TEST_F(BitmapIndexTest, TestDateType) {
    // data: 20200220, 20200220, null, null, 20220222, 20220222
    auto type = arrow::date32();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [20200220],
        [20200220],
        [null],
        [null],
        [20220222],
        [20220222]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit1(FieldType::DATE, static_cast<int32_t>(20200220));
        Literal lit2(FieldType::DATE, static_cast<int32_t>(20220222));
        Literal lit3(FieldType::DATE, static_cast<int32_t>(20250222));
        CheckResult(reader->VisitEqual(lit1).value(), {0, 1});
        CheckResult(reader->VisitEqual(lit2).value(), {4, 5});
        CheckResult(reader->VisitIsNull().value(), {2, 3});
        CheckResult(reader->VisitEqual(lit3).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {
            1,  0,    0,  0, 6,  0,  0,  0,  2,  1, 0, 0, 0, 0,  1, 52, 59, 28, 0, 0,  0, 20, 1,
            52, -119, 62, 0, 0,  0,  40, 58, 48, 0, 0, 1, 0, 0,  0, 0,  0,  1,  0, 16, 0, 0,  0,
            2,  0,    3,  0, 58, 48, 0,  0,  1,  0, 0, 0, 0, 0,  1, 0,  16, 0,  0, 0,  0, 0,  1,
            0,  58,   48, 0, 0,  1,  0,  0,  0,  0, 0, 1, 0, 16, 0, 0,  0,  4,  0, 5,  0};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,  0, 0,  0,  6,  0, 0, 0,  2, 1,  0,    0,  0,  0, 0, 0,  0, 20, 0,  0,  0,
            1,  1, 52, 59, 28, 0, 0, 0,  0, 0,  0,    0,  28, 0, 0, 0,  2, 1,  52, 59, 28,
            0,  0, 0,  20, 0,  0, 0, 20, 1, 52, -119, 62, 0,  0, 0, 40, 0, 0,  0,  20, 58,
            48, 0, 0,  1,  0,  0, 0, 0,  0, 1,  0,    16, 0,  0, 0, 2,  0, 3,  0,  58, 48,
            0,  0, 1,  0,  0,  0, 0, 0,  1, 0,  16,   0,  0,  0, 0, 0,  1, 0,  58, 48, 0,
            0,  1, 0,  0,  0,  0, 0, 1,  0, 16, 0,    0,  0,  4, 0, 5,  0};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestIntType) {
    // data: 0, 1, null
    auto type = arrow::int32();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [0],
        [1],
        [null]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit_0(static_cast<int32_t>(0));
        Literal lit_1(static_cast<int32_t>(1));
        Literal lit_2(static_cast<int32_t>(2));
        CheckResult(reader->VisitEqual(lit_0).value(), {0});
        CheckResult(reader->VisitEqual(lit_1).value(), {1});
        CheckResult(reader->VisitIsNull().value(), {2});
        CheckResult(reader->VisitIn({lit_0, lit_1, lit_2}).value(), {0, 1});
        CheckResult(reader->VisitNotIn({lit_0, lit_1, lit_2}).value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {1, 0, 0, 0,  3,  0,  0,  0, 2, 1, -1, -1, -1, -3, 0,
                                         0, 0, 0, -1, -1, -1, -1, 0, 0, 0, 1,  -1, -1, -1, -2};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {2,  0,  0, 0, 3, 0, 0,  0,  2,  1,  -1, -1, -1, -3, 0,  0,
                                         0,  18, 0, 0, 0, 1, 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
                                         0,  28, 0, 0, 0, 2, 0,  0,  0,  0,  -1, -1, -1, -1, -1, -1,
                                         -1, -1, 0, 0, 0, 1, -1, -1, -1, -2, -1, -1, -1, -1};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestTimestampType) {
    // data:
    // 1745542802000lms, 123000ns
    // 1745542902000lms, 123000ns
    // 1745542602000lms, 123000ns
    // -1745lms, 123000ns
    // -1765lms, 123000ns
    // null
    // 1745542802000lms, 123001ns
    // -1725lms, 123000ns
    auto type = arrow::timestamp(arrow::TimeUnit::NANO);
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [1745542802000123000],
        [1745542902000123000],
        [1745542602000123000],
        [-1744877000],
        [-1764877000],
        [null],
        [1745542802000123001],
        [-1724877000]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);

        CheckResult(reader->VisitIsNull().value(), {5});
        CheckResult(reader->VisitIsNotNull().value(), {0, 1, 2, 3, 4, 6, 7});
        CheckResult(reader->VisitEqual(Literal(Timestamp(1745542502000l, 123000))).value(), {});
        // as timestamp is normalized by micro seconds, there is a loss of precision in the
        // nanosecond part
        CheckResult(reader->VisitEqual(Literal(Timestamp(1745542802000l, 123000))).value(), {0, 6});
        CheckResult(reader->VisitNotEqual(Literal(Timestamp(1745542802000l, 123000))).value(),
                    {1, 2, 3, 4, 7});
        CheckResult(reader
                        ->VisitIn({Literal(Timestamp(1745542802000l, 123000)),
                                   Literal(Timestamp(-1745, 123000)),
                                   Literal(Timestamp(1745542602000, 123000))})
                        .value(),
                    {0, 2, 3, 6});
        CheckResult(reader
                        ->VisitNotIn({Literal(Timestamp(1745542802000l, 123000)),
                                      Literal(Timestamp(-1745, 123000)),
                                      Literal(Timestamp(1745542602000, 123000))})
                        .value(),
                    {1, 4, 7});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {
            1,   0,   0,  0,  8,  0,    0,   0,   6,  1,   -1, -1,   -1,  -6,  0,    6,  51, -113,
            -38, -89, 72, -5, 0,  0,    0,   0,   -1, -1,  -1, -1,   -1,  -27, -82,  51, -1, -1,
            -1,  -8,  -1, -1, -1, -1,   -1,  -27, 17, -13, -1, -1,   -1,  -5,  -1,   -1, -1, -1,
            -1,  -27, 96, 19, -1, -1,   -1,  -4,  0,  6,   51, -113, -50, -69, -122, -5, -1, -1,
            -1,  -3,  0,  6,  51, -113, -32, -99, 41, -5,  -1, -1,   -1,  -2,  58,   48, 0,  0,
            1,   0,   0,  0,  0,  0,    1,   0,   16, 0,   0,  0,    0,   0,   6,    0};
        check_result(index_bytes.data(), index_bytes.size());
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {
            2,   0,    0,   0,    8,   0,   0,    0,   6,   1,   -1, -1,  -1, -6,  0,  0,  0,  18,
            0,   0,    0,   1,    -1,  -1,  -1,   -1,  -1,  -27, 17, -13, 0,  0,   0,  0,  0,  0,
            0,   100,  0,   0,    0,   6,   -1,   -1,  -1,  -1,  -1, -27, 17, -13, -1, -1, -1, -5,
            -1,  -1,   -1,  -1,   -1,  -1,  -1,   -1,  -1,  -27, 96, 19,  -1, -1,  -1, -4, -1, -1,
            -1,  -1,   -1,  -1,   -1,  -1,  -1,   -27, -82, 51,  -1, -1,  -1, -8,  -1, -1, -1, -1,
            0,   6,    51,  -113, -50, -69, -122, -5,  -1,  -1,  -1, -3,  -1, -1,  -1, -1, 0,  6,
            51,  -113, -38, -89,  72,  -5,  0,    0,   0,   0,   0,  0,   0,  20,  0,  6,  51, -113,
            -32, -99,  41,  -5,   -1,  -1,  -1,   -2,  -1,  -1,  -1, -1,  58, 48,  0,  0,  1,  0,
            0,   0,    0,   0,    1,   0,   16,   0,   0,   0,   0,  0,   6,  0};
        check_result(index_bytes.data(), index_bytes.size());
    }
}

TEST_F(BitmapIndexTest, TestHighCardinalityForCompatibility) {
    auto type = arrow::utf8();
    auto check_result = [&](const std::string& index_file_name) {
        auto file_system = std::make_unique<LocalFileSystem>();
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<InputStream> input_stream,
                             file_system->Open(index_file_name));
        ASSERT_OK_AND_ASSIGN(uint64_t length, input_stream->Length());

        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(auto reader, file_index.CreateReader(CreateArrowSchema(type).get(),
                                                                  /*start=*/0, /*length=*/length,
                                                                  input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit_s1(FieldType::STRING, "asdfghjkl123_2500", 17);
        Literal lit_s2(FieldType::STRING, "asdfghjkl123_5000", 17);
        Literal lit_s3(FieldType::STRING, "asdfghjkl123_7500", 17);

        CheckResult(reader->VisitEqual(lit_s1).value(),
                    {3127, 18654, 23615, 43768, 62555, 63261, 66284, 74708, 99268});
        CheckResult(reader->VisitEqual(lit_s2).value(),
                    {2269, 21292, 23667, 35066, 42377, 46085, 64811, 79062, 96161});
        CheckResult(reader->VisitEqual(lit_s3).value(),
                    {14856, 50768, 60230, 68792, 72208, 73800, 78261, 81346, 86520, 87420, 88501,
                     95131, 99401});
    };

    // test v1 version
    check_result(paimon::test::GetDataDir() + "/file_index/bitmap-index-v1");

    // test v2 version
    check_result(paimon::test::GetDataDir() + "/file_index/bitmap-index-v2");
}

TEST_F(BitmapIndexTest, TestHighCardinalityForWriteAndRead) {
    int64_t seed = DateTimeUtils::GetCurrentUTCTimeUs();
    std::srand(seed);
    auto type = arrow::utf8();
    std::vector<std::string> unique_values = {
        "asdfghjkl123_0", "asdfghjkl123_2500", "asdfghjkl123_300", "asdfghjkl123_20",
        "asdfghjkl123_1", "asdfghjkl123_2",    "asdfghjkl123_3",   "asdfghjkl123_4",
        "asdfghjkl123_5", "asdfghjkl123_6"};
    std::vector<std::vector<int32_t>> expected_bitmaps(unique_values.size());

    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        // clear expected bitmaps
        for (auto& bitmap : expected_bitmaps) {
            bitmap.clear();
        }

        arrow::StructBuilder struct_builder(arrow::struct_({arrow::field("f0", type)}),
                                            arrow::default_memory_pool(),
                                            {std::make_shared<arrow::StringBuilder>()});
        auto string_builder = static_cast<arrow::StringBuilder*>(struct_builder.field_builder(0));

        for (int32_t i = 0; i < 100000; i++) {
            EXPECT_TRUE(struct_builder.Append().ok());
            int32_t idx = rand() % unique_values.size();
            EXPECT_TRUE(string_builder->Append(unique_values[idx].data()).ok());
            expected_bitmaps[idx].push_back(i);
        }
        std::shared_ptr<arrow::Array> array;
        EXPECT_TRUE(struct_builder.Finish(&array).ok());
        return WriteIndex(type, version, array);
    };

    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);

        for (size_t i = 0; i < unique_values.size(); i++) {
            Literal lit(FieldType::STRING, unique_values[i].data(), unique_values[i].size());
            CheckResult(reader->VisitEqual(lit).value(), expected_bitmaps[i]);
        }
        CheckResult(reader->VisitIsNull().value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
}

TEST_F(BitmapIndexTest, TestCompatibleWithJava) {
    // data: apple, null, apple, null, apple
    // If and only if non-null elements only contain one value (e.g., apple), index bytes can be
    // compared for compatibility.
    auto type = arrow::utf8();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        ["apple"],
        [null],
        ["apple"],
        [null],
        ["apple"]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    {
        // test v1 version
        std::vector<char> java_index_bytes = {
            1, 0,  0,  0,  5,  0,  0, 0, 1, 1, 0, 0, 0, 0,  0, 0, 0,  5, 97, 112, 112, 108, 101,
            0, 0,  0,  20, 58, 48, 0, 0, 1, 0, 0, 0, 0, 0,  1, 0, 16, 0, 0,  0,   1,   0,   3,
            0, 58, 48, 0,  0,  1,  0, 0, 0, 0, 0, 2, 0, 16, 0, 0, 0,  0, 0,  2,   0,   4,   0};
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        ASSERT_EQ(java_index_bytes, std::vector<char>(index_bytes->data(),
                                                      index_bytes->data() + index_bytes->size()));
    }
    {
        // test v2 version
        std::vector<char> java_index_bytes = {
            2, 0, 0, 0, 5, 0,  0,   0,   1,   1,   0, 0, 0, 0,  0, 0, 0, 20, 0,  0,  0,
            1, 0, 0, 0, 5, 97, 112, 112, 108, 101, 0, 0, 0, 0,  0, 0, 0, 21, 0,  0,  0,
            1, 0, 0, 0, 5, 97, 112, 112, 108, 101, 0, 0, 0, 20, 0, 0, 0, 22, 58, 48, 0,
            0, 1, 0, 0, 0, 0,  0,   1,   0,   16,  0, 0, 0, 1,  0, 3, 0, 58, 48, 0,  0,
            1, 0, 0, 0, 0, 0,  2,   0,   16,  0,   0, 0, 0, 0,  2, 0, 4, 0};
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        ASSERT_EQ(java_index_bytes, std::vector<char>(index_bytes->data(),
                                                      index_bytes->data() + index_bytes->size()));
    }
}

TEST_F(BitmapIndexTest, TestAllNull) {
    // data: null, null, null, null
    auto type = arrow::int32();
    auto write_data = [&](int32_t version) -> Result<PAIMON_UNIQUE_PTR<Bytes>> {
        auto array = std::dynamic_pointer_cast<arrow::StructArray>(
            arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_({arrow::field("f0", type)}),
                                                      R"([
        [null],
        [null],
        [null],
        [null]
    ])")
                .ValueOrDie());
        return WriteIndex(type, version, array);
    };
    auto check_result = [&](const char* index_bytes, int32_t index_length) {
        auto input_stream = std::make_shared<ByteArrayInputStream>(index_bytes, index_length);
        BitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(
            auto reader,
            file_index.CreateReader(CreateArrowSchema(type).get(),
                                    /*start=*/0, /*length=*/index_length, input_stream, pool_));
        ASSERT_TRUE(reader);
        Literal lit_0(static_cast<int32_t>(0));
        CheckResult(reader->VisitEqual(lit_0).value(), {});
        CheckResult(reader->VisitIsNull().value(), {0, 1, 2, 3});
        CheckResult(reader->VisitIsNotNull().value(), {});
    };
    {
        // test write and read, v1 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/1));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test write and read, v2 version
        ASSERT_OK_AND_ASSIGN(auto index_bytes, write_data(/*version=*/2));
        check_result(index_bytes->data(), index_bytes->size());
    }
    {
        // test v1 version
        std::vector<char> index_bytes = {1,  0, 0, 0, 4, 0, 0, 0, 0, 1, 0, 0, 0, 0, 59,
                                         48, 0, 0, 1, 0, 0, 3, 0, 1, 0, 0, 0, 3, 0};
        check_result(index_bytes.data(), index_bytes.size());

        // test compatible
        ASSERT_OK_AND_ASSIGN(auto index_bytes2, write_data(/*version=*/1));
        ASSERT_EQ(index_bytes, std::vector<char>(index_bytes2->data(),
                                                 index_bytes2->data() + index_bytes2->size()));
    }
    {
        // test v2 version
        std::vector<char> index_bytes = {2, 0, 0, 0,  4, 0, 0, 0, 0, 1, 0, 0, 0,  0,
                                         0, 0, 0, 15, 0, 0, 0, 0, 0, 0, 0, 0, 59, 48,
                                         0, 0, 1, 0,  0, 3, 0, 1, 0, 0, 0, 3, 0};
        check_result(index_bytes.data(), index_bytes.size());

        // test compatible
        ASSERT_OK_AND_ASSIGN(auto index_bytes2, write_data(/*version=*/2));
        ASSERT_EQ(index_bytes, std::vector<char>(index_bytes2->data(),
                                                 index_bytes2->data() + index_bytes2->size()));
    }
}

}  // namespace paimon::test
