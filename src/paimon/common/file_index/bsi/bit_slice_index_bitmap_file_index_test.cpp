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

#include "paimon/common/file_index/bsi/bit_slice_index_bitmap_file_index.h"

#include <utility>

#include "gtest/gtest.h"
#include "paimon/common/utils/field_type_utils.h"
#include "paimon/data/timestamp.h"
#include "paimon/defs.h"
#include "paimon/file_index/bitmap_index_result.h"
#include "paimon/io/byte_array_input_stream.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/testharness.h"
#include "paimon/utils/roaring_bitmap32.h"

namespace paimon::test {
class BitSliceIndexBitmapIndexReaderTest : public ::testing::Test {
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
            << "result=" << typed_result->GetBitmap().value()->ToString()
            << ", expected=" << RoaringBitmap32::From(expected).ToString();
    }

 private:
    std::shared_ptr<MemoryPool> pool_;
};

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestMix) {
    // data: 1, 2, null, -2, -2, -1, null, 2, 0, 5, null
    std::vector<char> index_bytes = {
        1, 0, 0, 0,  11, 1,  1,  0, 0, 0, 0,  0, 0, 0, 0, 0,  0,  0,  0,  0,  0, 0, 5, 58, 48,
        0, 0, 1, 0,  0,  0,  0,  0, 4, 0, 16, 0, 0, 0, 0, 0,  1,  0,  7,  0,  8, 0, 9, 0,  0,
        0, 0, 3, 58, 48, 0,  0,  1, 0, 0, 0,  0, 0, 1, 0, 16, 0,  0,  0,  0,  0, 9, 0, 58, 48,
        0, 0, 1, 0,  0,  0,  0,  0, 1, 0, 16, 0, 0, 0, 1, 0,  7,  0,  58, 48, 0, 0, 1, 0,  0,
        0, 0, 0, 0,  0,  16, 0,  0, 0, 9, 0,  1, 1, 0, 0, 0,  0,  0,  0,  0,  0, 0, 0, 0,  0,
        0, 0, 0, 2,  58, 48, 0,  0, 1, 0, 0,  0, 0, 0, 2, 0,  16, 0,  0,  0,  3, 0, 4, 0,  5,
        0, 0, 0, 0,  2,  58, 48, 0, 0, 1, 0,  0, 0, 0, 0, 0,  0,  16, 0,  0,  0, 5, 0, 58, 48,
        0, 0, 1, 0,  0,  0,  0,  0, 1, 0, 16, 0, 0, 0, 3, 0,  4,  0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    BitSliceIndexBitmapFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::int32()).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_));
    // test equal
    CheckResult(reader->VisitEqual(Literal(2)).value(), {1, 7});
    CheckResult(reader->VisitEqual(Literal(-2)).value(), {3, 4});
    CheckResult(reader->VisitEqual(Literal(100)).value(), {});

    // test not equal
    CheckResult(reader->VisitNotEqual(Literal(2)).value(), {0, 3, 4, 5, 8, 9});
    CheckResult(reader->VisitNotEqual(Literal(-2)).value(), {0, 1, 5, 7, 8, 9});
    CheckResult(reader->VisitNotEqual(Literal(100)).value(), {0, 1, 3, 4, 5, 7, 8, 9});

    // test in
    CheckResult(reader->VisitIn({Literal(-1), Literal(1), Literal(2), Literal(3)}).value(),
                {0, 1, 5, 7});

    // test not in
    CheckResult(reader->VisitNotIn({Literal(-1), Literal(1), Literal(2), Literal(3)}).value(),
                {3, 4, 8, 9});

    // test null
    CheckResult(reader->VisitIsNull().value(), {2, 6, 10});

    // test not null
    CheckResult(reader->VisitIsNotNull().value(), {0, 1, 3, 4, 5, 7, 8, 9});

    // test less than
    CheckResult(reader->VisitLessThan(Literal(2)).value(), {0, 3, 4, 5, 8});
    CheckResult(reader->VisitLessOrEqual(Literal(2)).value(), {0, 1, 3, 4, 5, 7, 8});
    CheckResult(reader->VisitLessThan(Literal(-1)).value(), {3, 4});
    CheckResult(reader->VisitLessOrEqual(Literal(-1)).value(), {3, 4, 5});

    // test greater than
    CheckResult(reader->VisitGreaterThan(Literal(-2)).value(), {0, 1, 5, 7, 8, 9});
    CheckResult(reader->VisitGreaterOrEqual(Literal(-2)).value(), {0, 1, 3, 4, 5, 7, 8, 9});
    CheckResult(reader->VisitGreaterThan(Literal(2)).value(), {9});
    CheckResult(reader->VisitGreaterOrEqual(Literal(2)).value(), {1, 7, 9});
}

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestPositiveOnly) {
    // data: 0, 1, null, 3, 4, 5, 6, 0, null
    std::vector<char> index_bytes = {
        1,  0,  0, 0, 9, 1, 1, 0, 0,  0, 0, 0,  0,  0, 0, 0, 0, 0, 0, 0,  0,  0, 6,
        59, 48, 0, 0, 1, 0, 0, 6, 0,  2, 0, 0,  0,  1, 0, 3, 0, 4, 0, 0,  0,  0, 3,
        58, 48, 0, 0, 1, 0, 0, 0, 0,  0, 2, 0,  16, 0, 0, 0, 1, 0, 3, 0,  5,  0, 58,
        48, 0,  0, 1, 0, 0, 0, 0, 0,  1, 0, 16, 0,  0, 0, 3, 0, 6, 0, 58, 48, 0, 0,
        1,  0,  0, 0, 0, 0, 2, 0, 16, 0, 0, 0,  4,  0, 5, 0, 6, 0, 0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    BitSliceIndexBitmapFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::int32()).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_));
    // test equal
    CheckResult(reader->VisitEqual(Literal(0)).value(), {0, 7});
    CheckResult(reader->VisitEqual(Literal(1)).value(), {1});
    CheckResult(reader->VisitEqual(Literal(-1)).value(), {});

    // test not equal
    CheckResult(reader->VisitNotEqual(Literal(2)).value(), {0, 1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitNotEqual(Literal(-2)).value(), {0, 1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitNotEqual(Literal(3)).value(), {0, 1, 4, 5, 6, 7});

    // test in
    CheckResult(reader->VisitIn({Literal(-1), Literal(1), Literal(2), Literal(3)}).value(), {1, 3});

    // test not in
    CheckResult(reader->VisitNotIn({Literal(-1), Literal(1), Literal(2), Literal(3)}).value(),
                {0, 4, 5, 6, 7});

    // test null
    CheckResult(reader->VisitIsNull().value(), {2, 8});

    // test not null
    CheckResult(reader->VisitIsNotNull().value(), {0, 1, 3, 4, 5, 6, 7});

    // test less than
    CheckResult(reader->VisitLessThan(Literal(3)).value(), {0, 1, 7});
    CheckResult(reader->VisitLessOrEqual(Literal(3)).value(), {0, 1, 3, 7});
    CheckResult(reader->VisitLessThan(Literal(-1)).value(), {});
    CheckResult(reader->VisitLessOrEqual(Literal(-1)).value(), {});

    // test greater than
    CheckResult(reader->VisitGreaterThan(Literal(-2)).value(), {0, 1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitGreaterOrEqual(Literal(-2)).value(), {0, 1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitGreaterThan(Literal(1)).value(), {3, 4, 5, 6});
    CheckResult(reader->VisitGreaterOrEqual(Literal(1)).value(), {1, 3, 4, 5, 6});
}

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestNegativeOnly) {
    // data: null, -1, null, -3, -4, -5, -6, -1, null
    std::vector<char> index_bytes = {
        1,  0,  0,  0,  9, 0, 1, 1, 0, 0, 0, 0,  0, 0,  0,  0, 0, 0, 0, 0, 0, 0, 0,
        6,  59, 48, 0,  0, 1, 0, 0, 5, 0, 2, 0,  1, 0,  0,  0, 3, 0, 4, 0, 0, 0, 0,
        3,  58, 48, 0,  0, 1, 0, 0, 0, 0, 0, 3,  0, 16, 0,  0, 0, 1, 0, 3, 0, 5, 0,
        7,  0,  58, 48, 0, 0, 1, 0, 0, 0, 0, 0,  1, 0,  16, 0, 0, 0, 3, 0, 6, 0, 58,
        48, 0,  0,  1,  0, 0, 0, 0, 0, 2, 0, 16, 0, 0,  0,  4, 0, 5, 0, 6, 0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    BitSliceIndexBitmapFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::int32()).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_));
    // test equal
    CheckResult(reader->VisitEqual(Literal(1)).value(), {});
    CheckResult(reader->VisitEqual(Literal(-2)).value(), {});
    CheckResult(reader->VisitEqual(Literal(-1)).value(), {1, 7});

    // test not equal
    CheckResult(reader->VisitNotEqual(Literal(2)).value(), {1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitNotEqual(Literal(-2)).value(), {1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitNotEqual(Literal(-3)).value(), {1, 4, 5, 6, 7});

    // test in
    CheckResult(reader->VisitIn({Literal(-1), Literal(-4), Literal(-2), Literal(3)}).value(),
                {1, 4, 7});

    // test not in
    CheckResult(reader->VisitNotIn({Literal(-1), Literal(-4), Literal(-2), Literal(3)}).value(),
                {3, 5, 6});

    // test null
    CheckResult(reader->VisitIsNull().value(), {0, 2, 8});

    // test not null
    CheckResult(reader->VisitIsNotNull().value(), {1, 3, 4, 5, 6, 7});

    // test less than
    CheckResult(reader->VisitLessThan(Literal(-3)).value(), {4, 5, 6});
    CheckResult(reader->VisitLessOrEqual(Literal(-3)).value(), {3, 4, 5, 6});
    CheckResult(reader->VisitLessThan(Literal(1)).value(), {1, 3, 4, 5, 6, 7});
    CheckResult(reader->VisitLessOrEqual(Literal(1)).value(), {1, 3, 4, 5, 6, 7});

    // test greater than
    CheckResult(reader->VisitGreaterThan(Literal(-3)).value(), {1, 7});
    CheckResult(reader->VisitGreaterOrEqual(Literal(-3)).value(), {1, 3, 7});
    CheckResult(reader->VisitGreaterThan(Literal(1)).value(), {});
    CheckResult(reader->VisitGreaterOrEqual(Literal(1)).value(), {});
}

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestPrimitiveType) {
    // data: null, 1, null, 2, -1
    auto check_result = [&](const std::shared_ptr<arrow::DataType>& type, const Literal& literal) {
        std::vector<char> index_bytes = {
            1,  0, 0, 0,  5,  1,  1,  0, 0, 0, 0, 0,  0, 0, 0, 0,  0, 0,  0,  0, 0, 0, 2, 58,
            48, 0, 0, 1,  0,  0,  0,  0, 0, 1, 0, 16, 0, 0, 0, 1,  0, 3,  0,  0, 0, 0, 2, 58,
            48, 0, 0, 1,  0,  0,  0,  0, 0, 0, 0, 16, 0, 0, 0, 1,  0, 58, 48, 0, 0, 1, 0, 0,
            0,  0, 0, 0,  0,  16, 0,  0, 0, 3, 0, 1,  1, 0, 0, 0,  0, 0,  0,  0, 0, 0, 0, 0,
            0,  0, 0, 0,  1,  58, 48, 0, 0, 1, 0, 0,  0, 0, 0, 0,  0, 16, 0,  0, 0, 4, 0, 0,
            0,  0, 1, 58, 48, 0,  0,  1, 0, 0, 0, 0,  0, 0, 0, 16, 0, 0,  0,  4, 0};
        auto input_stream =
            std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
        BitSliceIndexBitmapFileIndex file_index({});
        ASSERT_OK_AND_ASSIGN(auto reader,
                             file_index.CreateReader(CreateArrowSchema(type).get(),
                                                     /*start=*/0, /*length=*/index_bytes.size(),
                                                     input_stream, pool_));
        CheckResult(reader->VisitIsNull().value(), {0, 2});
        CheckResult(reader->VisitIsNotNull().value(), {1, 3, 4});
        CheckResult(reader->VisitGreaterThan(literal).value(), {3});
        CheckResult(reader->VisitLessOrEqual(literal).value(), {1, 4});
        // test invalid case
        ASSERT_OK_AND_ASSIGN(auto field_type, FieldTypeUtils::ConvertToFieldType(type->id()));
        ASSERT_NOK_WITH_MSG(reader->VisitEqual(Literal(field_type)).value()->IsRemain(),
                            "literal cannot be null when GetValue in BitSliceIndexBitmapFileIndex");
    };
    check_result(arrow::int8(), Literal(static_cast<int8_t>(1)));
    check_result(arrow::int16(), Literal(static_cast<int16_t>(1)));
    check_result(arrow::int32(), Literal(static_cast<int32_t>(1)));
    check_result(arrow::date32(), Literal(FieldType::DATE, 1));
    check_result(arrow::int64(), Literal(static_cast<int64_t>(1)));
    check_result(arrow::timestamp(arrow::TimeUnit::NANO), Literal(Timestamp(0, 1000)));
}

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestTimestampType) {
    std::vector<char> index_bytes = {
        1,  0,  0,  0,  8,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  6,  51,  -113, -32, -99, 41,
        -5, 58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,   0,    1,   0,   2,
        0,  6,  0,  0,  0,  0,  51, 58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,   0,    16,  0,   0,
        0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    3,   0,   16,
        0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  0,  0,  0,   0,    58,  48,  0,
        0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,   0,    6,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,   0,    2,   0,   6,
        0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,   0,    1,   0,   2,
        0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,   0,    0,   0,   1,
        0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16,  0,    0,   0,   0,
        0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  0,   0,    16,  0,   0,
        0,  1,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,   0,    2,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,  0,  2,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  0,  0,  1,  0,  6,  0,  58,  48,   0,   0,   0,
        0,  0,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,   0,    1,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  1,  0,  16, 0,  0,  0,  0,  0,  6,   0,    58,  48,  0,
        0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,  0,  2,  0,  58, 48, 0,   0,    1,   0,   0,
        0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  0,  0,  2,  0,  6,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  0,  0,  1,  0,  6,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  1,  0,  16, 0,  0,  0,  1,  0,  2,  0,  58, 48, 0,   0,    1,   0,   0,
        0,  0,  0,  1,  0,  16, 0,  0,  0,  1,  0,  2,  0,  58, 48, 0,  0,  1,   0,    0,   0,   0,
        0,  2,  0,  16, 0,  0,  0,  0,  0,  2,  0,  6,  0,  58, 48, 0,  0,  0,   0,    0,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,   0,    2,   0,   6,
        0,  58, 48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    2,   0,   16,
        0,  0,  0,  0,  0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    0,   0,   16,
        0,  0,  0,  2,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  2,  0,  16,  0,    0,   0,   0,
        0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  1,  0,  16,  0,    0,   0,   0,
        0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,   0,    1,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,   0,    2,   0,   6,
        0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,   0,    1,   0,   2,
        0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,   0,    0,   0,   1,
        0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16,  0,    0,   0,   0,
        0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  3,   0,    16,  0,   0,
        0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    3,   0,   16,
        0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,  0,  0,  0,  0,   0,    58,  48,  0,
        0,  0,  0,  0,  0,  58, 48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  1,   0,    0,   0,   0,
        0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,   0,    1,   0,   0,
        0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,   0,    58,  48,  0,
        0,  0,  0,  0,  0,  58, 48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  1,   0,    0,   0,   0,
        0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58, 48, 0,   0,    1,   0,   0,
        0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,  0,  6,  0,  58,  48,   0,   0,   0,
        0,  0,  0,  58, 48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  0,  0,  0,   0,    58,  48,  0,
        0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,  0,  2,   0,    6,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  3,  0,  16, 0,  0,  0,  0,  0,  1,   0,    2,   0,   6,
        0,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  26, -18, 13,   58,  48,  0,
        0,  1,  0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  3,  0,  4,  0,  7,   0,    0,   0,   0,
        21, 58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  3,   0,    4,   0,   7,
        0,  58, 48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    2,   0,   16,
        0,  0,  0,  3,  0,  4,  0,  7,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    2,   0,   16,
        0,  0,  0,  3,  0,  4,  0,  7,  0,  58, 48, 0,  0,  0,  0,  0,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  0,  0,  16, 0,  0,  0,  3,  0,  58, 48, 0,  0,  1,   0,    0,   0,   0,
        0,  1,  0,  16, 0,  0,  0,  3,  0,  7,  0,  58, 48, 0,  0,  1,  0,  0,   0,    0,   0,   1,
        0,  16, 0,  0,  0,  3,  0,  7,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,   0,    1,   0,   16,
        0,  0,  0,  3,  0,  7,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  1,   0,    16,  0,   0,
        0,  3,  0,  4,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  1,  0,  16,  0,    0,   0,   3,
        0,  4,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  1,  0,  16, 0,  0,   0,    3,   0,   4,
        0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  1,  0,  16, 0,  0,  0,  3,   0,    7,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  16, 0,  0,  0,  4,  0,  58,  48,   0,   0,   1,
        0,  0,  0,  0,  0,  1,  0,  16, 0,  0,  0,  4,  0,  7,  0,  58, 48, 0,   0,    1,   0,   0,
        0,  0,  0,  1,  0,  16, 0,  0,  0,  3,  0,  4,  0,  58, 48, 0,  0,  0,   0,    0,   0,   58,
        48, 0,  0,  1,  0,  0,  0,  0,  0,  2,  0,  16, 0,  0,  0,  3,  0,  4,   0,    7,   0,   58,
        48, 0,  0,  0,  0,  0,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  2,   0,    16,  0,   0,
        0,  3,  0,  4,  0,  7,  0,  58, 48, 0,  0,  1,  0,  0,  0,  0,  0,  2,   0,    16,  0,   0,
        0,  3,  0,  4,  0,  7,  0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    BitSliceIndexBitmapFileIndex file_index({});
    ASSERT_OK_AND_ASSIGN(
        auto reader,
        file_index.CreateReader(CreateArrowSchema(arrow::timestamp(arrow::TimeUnit::NANO)).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_));
    // data:
    // 1745542802000lms, 123000ns
    // 1745542902000lms, 123000ns
    // 1745542602000lms, 123000ns
    // -1745lms, 123000ns
    // -1765lms, 123000ns
    // null
    // 1745542802000lms, 123001ns
    // -1725lms, 123000ns
    CheckResult(reader->VisitIsNull().value(), {5});
    CheckResult(reader->VisitIsNotNull().value(), {0, 1, 2, 3, 4, 6, 7});
    // as timestamp is normalized by micro seconds, there is a loss of precision in the nanosecond
    // part
    CheckResult(reader->VisitGreaterThan(Literal(Timestamp(1745542802000l, 123000))).value(), {1});
    CheckResult(reader->VisitGreaterOrEqual(Literal(Timestamp(1745542802000l, 123000))).value(),
                {0, 1, 6});
    CheckResult(reader->VisitLessThan(Literal(Timestamp(-1745, 123000))).value(), {4});
    CheckResult(reader->VisitLessOrEqual(Literal(Timestamp(0, 123000))).value(), {3, 4, 7});
    CheckResult(reader->VisitEqual(Literal(Timestamp(1745542502000l, 123000))).value(), {});
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

    // test invalid case
    ASSERT_NOK_WITH_MSG(reader->VisitEqual(Literal(FieldType::TIMESTAMP)).value()->IsRemain(),
                        "literal cannot be null when GetValue in BitSliceIndexBitmapFileIndex");
}

TEST_F(BitSliceIndexBitmapIndexReaderTest, TestUnInvalidType) {
    std::vector<char> index_bytes = {
        1,  0, 0, 0,  5,  1,  1,  0, 0, 0, 0, 0,  0, 0, 0, 0,  0, 0,  0,  0, 0, 0, 2, 58,
        48, 0, 0, 1,  0,  0,  0,  0, 0, 1, 0, 16, 0, 0, 0, 1,  0, 3,  0,  0, 0, 0, 2, 58,
        48, 0, 0, 1,  0,  0,  0,  0, 0, 0, 0, 16, 0, 0, 0, 1,  0, 58, 48, 0, 0, 1, 0, 0,
        0,  0, 0, 0,  0,  16, 0,  0, 0, 3, 0, 1,  1, 0, 0, 0,  0, 0,  0,  0, 0, 0, 0, 0,
        0,  0, 0, 0,  1,  58, 48, 0, 0, 1, 0, 0,  0, 0, 0, 0,  0, 16, 0,  0, 0, 4, 0, 0,
        0,  0, 1, 58, 48, 0,  0,  1, 0, 0, 0, 0,  0, 0, 0, 16, 0, 0,  0,  4, 0};
    auto input_stream =
        std::make_shared<ByteArrayInputStream>(index_bytes.data(), index_bytes.size());
    BitSliceIndexBitmapFileIndex file_index({});
    ASSERT_NOK_WITH_MSG(
        file_index.CreateReader(CreateArrowSchema(arrow::boolean()).get(),
                                /*start=*/0, /*length=*/index_bytes.size(), input_stream, pool_),
        "BitSliceIndexBitmapFileIndex only support TINYINT/SMALLINT/INT/BIGINT/DATE");
}

}  // namespace paimon::test
