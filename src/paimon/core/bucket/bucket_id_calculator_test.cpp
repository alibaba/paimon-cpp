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

#include "paimon/bucket/bucket_id_calculator.h"

#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/ipc/json_simple.h"
#include "arrow/util/checked_cast.h"
#include "gtest/gtest.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/date_time_utils.h"
#include "paimon/core/bucket/default_bucket_function.h"
#include "paimon/core/bucket/mod_bucket_function.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class BucketIdCalculatorTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
    Result<std::vector<int32_t>> CalculateBucketIds(
        bool is_pk_table, int32_t num_buckets, const std::shared_ptr<arrow::Schema>& bucket_schema,
        const std::shared_ptr<arrow::Array>& bucket_array) const {
        ::ArrowArray c_bucket_array;
        EXPECT_TRUE(arrow::ExportArray(*bucket_array, &c_bucket_array).ok());
        ::ArrowSchema c_bucket_schema;
        EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_bucket_schema).ok());
        std::vector<int32_t> bucket_ids(bucket_array->length());
        EXPECT_OK_AND_ASSIGN(auto bucket_id_cal, BucketIdCalculator::Create(
                                                     is_pk_table, num_buckets, GetDefaultPool()));
        PAIMON_RETURN_NOT_OK(bucket_id_cal->CalculateBucketIds(
            /*bucket_keys=*/&c_bucket_array, /*bucket_schema=*/&c_bucket_schema,
            /*bucket_ids=*/bucket_ids.data()));
        return bucket_ids;
    }

    Result<std::vector<int32_t>> CalculateBucketIds(
        bool is_pk_table, int32_t num_buckets, const std::shared_ptr<arrow::Schema>& bucket_schema,
        const std::string& data_str) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto bucket_array,
                                          arrow::ipc::internal::json::ArrayFromJSON(
                                              arrow::struct_(bucket_schema->fields()), data_str));
        return CalculateBucketIds(is_pk_table, num_buckets, bucket_schema, bucket_array);
    }

    Result<std::vector<int32_t>> CalculateBucketIds(
        bool is_pk_table, int32_t num_buckets, std::unique_ptr<BucketFunction> bucket_function,
        const std::shared_ptr<arrow::Schema>& bucket_schema,
        const std::shared_ptr<arrow::Array>& bucket_array) const {
        ::ArrowArray c_bucket_array;
        EXPECT_TRUE(arrow::ExportArray(*bucket_array, &c_bucket_array).ok());
        ::ArrowSchema c_bucket_schema;
        EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_bucket_schema).ok());
        std::vector<int32_t> bucket_ids(bucket_array->length());
        EXPECT_OK_AND_ASSIGN(auto bucket_id_cal, BucketIdCalculator::Create(
                                                     is_pk_table, num_buckets,
                                                     std::move(bucket_function), GetDefaultPool()));
        PAIMON_RETURN_NOT_OK(bucket_id_cal->CalculateBucketIds(
            /*bucket_keys=*/&c_bucket_array, /*bucket_schema=*/&c_bucket_schema,
            /*bucket_ids=*/bucket_ids.data()));
        return bucket_ids;
    }

    Result<std::vector<int32_t>> CalculateBucketIds(
        bool is_pk_table, int32_t num_buckets, std::unique_ptr<BucketFunction> bucket_function,
        const std::shared_ptr<arrow::Schema>& bucket_schema, const std::string& data_str) const {
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(auto bucket_array,
                                          arrow::ipc::internal::json::ArrayFromJSON(
                                              arrow::struct_(bucket_schema->fields()), data_str));
        return CalculateBucketIds(is_pk_table, num_buckets, std::move(bucket_function),
                                  bucket_schema, bucket_array);
    }
};

TEST_F(BucketIdCalculatorTest, TestCompatibleWithJava) {
    // 5000 random records, first 12 column is record data, the last column is the bucket id
    // calculated by Java FixedBucketRowKeyExtractor
    std::string data_path = paimon::test::GetDataDir() + "/record_for_bucket_id.data";
    std::string content;
    auto fs = std::make_unique<LocalFileSystem>();
    ASSERT_OK(fs->ReadFile(data_path, &content));
    content = content.substr(0, content.length() - 2);
    content = "[" + content + "]";

    arrow::FieldVector raw_bucket_fields = {
        arrow::field("v0", arrow::boolean()),
        arrow::field("v1", arrow::int8()),
        arrow::field("v2", arrow::int16()),
        arrow::field("v3", arrow::int32()),
        arrow::field("v4", arrow::int64()),
        arrow::field("v5", arrow::float32()),
        arrow::field("v6", arrow::float64()),
        arrow::field("v7", arrow::date32()),
        arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("v9", arrow::decimal128(30, 20)),
        arrow::field("v10", arrow::utf8()),
        arrow::field("v11", arrow::binary())};
    auto bucket_schema = arrow::schema(raw_bucket_fields);

    arrow::FieldVector bucket_fields_with_id = bucket_schema->fields();
    bucket_fields_with_id.push_back(arrow::field("bucket_id", arrow::int32()));
    auto bucket_array_with_id = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_fields_with_id), content)
            .ValueOrDie());

    // exclude bucket id array
    arrow::ArrayVector bucket_fields(bucket_array_with_id->fields().begin(),
                                     bucket_array_with_id->fields().end() - 1);
    auto bucket_array =
        arrow::StructArray::Make(bucket_fields, bucket_schema->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result,
                         CalculateBucketIds(/*is_pk_table=*/true, /*num_buckets=*/12345,
                                            bucket_schema, bucket_array));

    auto bucket_id_array = arrow::internal::checked_cast<arrow::Int32Array*>(
        bucket_array_with_id->field(bucket_schema->num_fields()).get());
    ASSERT_TRUE(bucket_id_array);
    // test compatible with java
    for (int32_t i = 0; i < bucket_array->length(); i++) {
        ASSERT_EQ(bucket_id_array->Value(i), result[i]);
    }
}

TEST_F(BucketIdCalculatorTest, TestCompatibleWithJavaWithNull) {
    // 5000 random records, first 13 column is record data, the last column is the bucket id
    // calculated by Java FixedBucketRowKeyExtractor. Besides, the first row is all null.
    std::string data_path = paimon::test::GetDataDir() + "/record_with_null_for_bucket_id.data";
    std::string content;
    auto fs = std::make_unique<LocalFileSystem>();
    ASSERT_OK(fs->ReadFile(data_path, &content));
    content = content.substr(0, content.length() - 2);
    content = "[" + content + "]";

    arrow::FieldVector raw_bucket_fields = {
        arrow::field("v0", arrow::boolean()),
        arrow::field("v1", arrow::int8()),
        arrow::field("v2", arrow::int16()),
        arrow::field("v3", arrow::int32()),
        arrow::field("v4", arrow::int64()),
        arrow::field("v5", arrow::float32()),
        arrow::field("v6", arrow::float64()),
        arrow::field("v7", arrow::date32()),
        arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("v9", arrow::decimal128(5, 2)),
        arrow::field("v10", arrow::decimal128(30, 2)),
        arrow::field("v11", arrow::utf8()),
        arrow::field("v12", arrow::binary())};
    auto bucket_schema = arrow::schema(raw_bucket_fields);

    arrow::FieldVector bucket_fields_with_id = bucket_schema->fields();
    bucket_fields_with_id.push_back(arrow::field("bucket_id", arrow::int32()));
    auto bucket_array_with_id = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_fields_with_id), content)
            .ValueOrDie());

    // exclude bucket id array
    arrow::ArrayVector bucket_fields(bucket_array_with_id->fields().begin(),
                                     bucket_array_with_id->fields().end() - 1);
    auto bucket_array =
        arrow::StructArray::Make(bucket_fields, bucket_schema->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result,
                         CalculateBucketIds(/*is_pk_table=*/false, /*num_buckets=*/12345,
                                            bucket_schema, bucket_array));

    auto bucket_id_array = arrow::internal::checked_cast<arrow::Int32Array*>(
        bucket_array_with_id->field(bucket_schema->num_fields()).get());
    ASSERT_TRUE(bucket_id_array);
    // test compatible with java
    for (int32_t i = 0; i < bucket_array->length(); i++) {
        ASSERT_EQ(bucket_id_array->Value(i), result[i]);
    }
}

TEST_F(BucketIdCalculatorTest, TestCompatibleWithJavaWithTimestamp) {
    // 5000 random records, first 8 column is record data, the last column is the bucket id
    // calculated by Java FixedBucketRowKeyExtractor. Besides, the first row is all null.
    std::string data_path = paimon::test::GetDataDir() + "record_with_timestamp_for_bucket_id.data";
    std::string content;
    auto fs = std::make_unique<LocalFileSystem>();
    ASSERT_OK(fs->ReadFile(data_path, &content));
    content = content.substr(0, content.length() - 2);
    content = "[" + content + "]";
    auto timezone = DateTimeUtils::GetLocalTimezoneName();
    arrow::FieldVector raw_bucket_fields = {
        arrow::field("ts_sec", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("ts_milli", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("ts_micro", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("ts_nano", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("ts_tz_sec", arrow::timestamp(arrow::TimeUnit::SECOND, timezone)),
        arrow::field("ts_tz_milli", arrow::timestamp(arrow::TimeUnit::MILLI, timezone)),
        arrow::field("ts_tz_micro", arrow::timestamp(arrow::TimeUnit::MICRO, timezone)),
        arrow::field("ts_tz_nano", arrow::timestamp(arrow::TimeUnit::NANO, timezone)),
    };
    auto bucket_schema = std::make_shared<arrow::Schema>(raw_bucket_fields);

    arrow::FieldVector bucket_fields_with_id = bucket_schema->fields();
    bucket_fields_with_id.push_back(arrow::field("bucket_id", arrow::int32()));
    auto bucket_array_with_id = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_fields_with_id), content)
            .ValueOrDie());

    // exclude bucket id array
    arrow::ArrayVector bucket_fields(bucket_array_with_id->fields().begin(),
                                     bucket_array_with_id->fields().end() - 1);
    auto bucket_array =
        arrow::StructArray::Make(bucket_fields, bucket_schema->fields()).ValueOrDie();

    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result,
                         CalculateBucketIds(/*is_pk_table=*/false, /*num_buckets=*/12345,
                                            bucket_schema, bucket_array));

    auto bucket_id_array = arrow::internal::checked_cast<arrow::Int32Array*>(
        bucket_array_with_id->field(bucket_schema->num_fields()).get());
    ASSERT_TRUE(bucket_id_array);
    // test compatible with java
    for (int32_t i = 0; i < bucket_array->length(); i++) {
        ASSERT_EQ(bucket_id_array->Value(i), result[i]);
    }
}

TEST_F(BucketIdCalculatorTest, TestInvalidCase) {
    {
        // test invalid bucket id
        ASSERT_NOK_WITH_MSG(
            BucketIdCalculator::Create(/*is_pk_table=*/true, /*num_buckets=*/0, GetDefaultPool()),
            "num buckets must be -1 or -2 or greater than 0");
    }
    {
        // test invalid bucket mode with pk table
        ASSERT_NOK_WITH_MSG(
            BucketIdCalculator::Create(/*is_pk_table=*/true, /*num_buckets=*/-1, GetDefaultPool()),
            "DynamicBucketMode or CrossPartitionBucketMode cannot calculate bucket id");
    }
    {
        // test invalid bucket mode with append table
        ASSERT_NOK_WITH_MSG(
            BucketIdCalculator::Create(/*is_pk_table=*/false, /*num_buckets=*/-2, GetDefaultPool()),
            "Append table not support PostponeBucketMode");
    }
    {
        // test invalid bucket_keys
        auto bucket_schema =
            arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
        auto bucket_array =
            arrow::ipc::internal::json::ArrayFromJSON(arrow::int32(), "[10, 11, 12, 13]")
                .ValueOrDie();
        ASSERT_NOK_WITH_MSG(
            CalculateBucketIds(/*is_pk_table=*/false, 10, bucket_schema, bucket_array),
            "ArrowArray struct has 0 children");
    }
    {
        // test invalid data type
        auto bucket_schema = arrow::schema(arrow::FieldVector(
            {arrow::field("b0", arrow::int32()), arrow::field("b1", arrow::list(arrow::int64()))}));
        ASSERT_NOK_WITH_MSG(
            CalculateBucketIds(/*is_pk_table=*/true, 10, bucket_schema, "[[10, [1, 1, 2]]]"),
            "type list<item: int64> not support in write bucket row");
    }
}

TEST_F(BucketIdCalculatorTest, TestUnawareBucket) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result,
        CalculateBucketIds(/*is_pk_table=*/false, -1, bucket_schema, "[[10], [-1], [50]]"));
    std::vector<int32_t> expected = {0, 0, 0};
    ASSERT_EQ(expected, result);
}

TEST_F(BucketIdCalculatorTest, TestPostponeBucket) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result,
        CalculateBucketIds(/*is_pk_table=*/true, -2, bucket_schema, "[[10], [-1], [50]]"));
    std::vector<int32_t> expected = {-2, -2, -2};
    ASSERT_EQ(expected, result);
}

TEST_F(BucketIdCalculatorTest, TestVariantType) {
    arrow::FieldVector raw_bucket_fields = {
        arrow::field("v0", arrow::boolean()),
        arrow::field("v1", arrow::int8()),
        arrow::field("v2", arrow::int16()),
        arrow::field("v3", arrow::int32()),
        arrow::field("v4", arrow::int64()),
        arrow::field("v5", arrow::float32()),
        arrow::field("v6", arrow::float64()),
        arrow::field("v7", arrow::date32()),
        arrow::field("v8", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("v9", arrow::decimal128(30, 20)),
        arrow::field("v10", arrow::utf8()),
        arrow::field("v11", arrow::binary())};
    auto bucket_schema = arrow::schema(raw_bucket_fields);

    auto bucket_array = std::dynamic_pointer_cast<arrow::StructArray>(
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_schema->fields()), R"([
        [true, 10, 200, 65536, 123456789, 0.0, 0.0, 2000, -86399999999500, "2134.48690000000000000009", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference.", "Alice"],
        [false, -128, -32768, -2147483648, -9223372036854775808, -3.4028235E38, -1.7976931348623157E308, -719528, -9223372036854775808, "-999999999999999999.99999999999999999999", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."],
        [true, 127, 32767, 2147483647, 9223372036854775807, 3.4028235E38, 1.7976931348623157E308, 2932896, 9223372036854775807, "999999999999999999.99999999999999999999", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."],
        [true, 0, 0, 0, 0, 1.4E-45, 4.9E-324, 0, 0, "0.00000000000000000000", "Alice", "olá mundo，你好世界。Two roads diverged in a wood, and I took the one less traveled by, And that has made all the difference."]
])")
            .ValueOrDie());
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result,
        CalculateBucketIds(/*is_pk_table=*/true, 12345, bucket_schema, bucket_array));
    std::vector<int32_t> expected = {11275, 12272, 6549, 11795};
    ASSERT_EQ(expected, result);
    // test calculate multiple times
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result2,
        CalculateBucketIds(/*is_pk_table=*/true, 12345, bucket_schema, bucket_array));
    ASSERT_EQ(expected, result2);
}

TEST_F(BucketIdCalculatorTest, TestWithModBucketFunction) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    ASSERT_OK_AND_ASSIGN(auto mod_func, ModBucketFunction::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result,
        CalculateBucketIds(/*is_pk_table=*/true, /*num_buckets=*/10, std::move(mod_func),
                           bucket_schema, "[[10], [-1], [50], [-13], [0]]"));
    // Java Math.floorMod semantics:
    // floorMod(10, 10) = 0
    // floorMod(-1, 10) = 9
    // floorMod(50, 10) = 0
    // floorMod(-13, 10) = 7
    // floorMod(0, 10) = 0
    std::vector<int32_t> expected = {0, 9, 0, 7, 0};
    ASSERT_EQ(expected, result);
}

TEST_F(BucketIdCalculatorTest, TestWithDefaultBucketFunctionExplicit) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    // Calculate with explicit DefaultBucketFunction
    auto default_func = std::make_unique<DefaultBucketFunction>();
    ASSERT_OK_AND_ASSIGN(
        std::vector<int32_t> result_explicit,
        CalculateBucketIds(/*is_pk_table=*/true, /*num_buckets=*/10, std::move(default_func),
                           bucket_schema, "[[10], [-1], [50], [-13], [0]]"));
    // Calculate with default (no BucketFunction passed)
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result_default,
                         CalculateBucketIds(/*is_pk_table=*/true, /*num_buckets=*/10, bucket_schema,
                                            "[[10], [-1], [50], [-13], [0]]"));
    ASSERT_EQ(result_default, result_explicit);
}

TEST_F(BucketIdCalculatorTest, TestCreateWithDefaultBucketFunction) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    std::string data_str = "[[10], [-1], [50], [-13], [0]]";

    // Calculate with explicit DefaultBucketFunction via Create
    auto default_func = std::make_unique<DefaultBucketFunction>();
    ASSERT_OK_AND_ASSIGN(auto calc_explicit,
                         BucketIdCalculator::Create(/*is_pk_table=*/true, /*num_buckets=*/10,
                                                    std::move(default_func), GetDefaultPool()));

    // Calculate with the default Create (no BucketFunction)
    ASSERT_OK_AND_ASSIGN(
        auto calc_default,
        BucketIdCalculator::Create(/*is_pk_table=*/true, /*num_buckets=*/10, GetDefaultPool()));

    auto bucket_array1 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_schema->fields()), data_str)
            .ValueOrDie();
    ::ArrowArray c_array1;
    EXPECT_TRUE(arrow::ExportArray(*bucket_array1, &c_array1).ok());
    ::ArrowSchema c_schema1;
    EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_schema1).ok());
    std::vector<int32_t> result_explicit(bucket_array1->length());
    ASSERT_OK(calc_explicit->CalculateBucketIds(&c_array1, &c_schema1, result_explicit.data()));

    auto bucket_array2 =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_schema->fields()), data_str)
            .ValueOrDie();
    ::ArrowArray c_array2;
    EXPECT_TRUE(arrow::ExportArray(*bucket_array2, &c_array2).ok());
    ::ArrowSchema c_schema2;
    EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_schema2).ok());
    std::vector<int32_t> result_default(bucket_array2->length());
    ASSERT_OK(calc_default->CalculateBucketIds(&c_array2, &c_schema2, result_default.data()));

    ASSERT_EQ(result_default, result_explicit);
}

TEST_F(BucketIdCalculatorTest, TestCreateWithModBucketFunction) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    std::string data_str = "[[10], [-1], [50], [-13], [0]]";

    // Calculate with CreateMod
    ASSERT_OK_AND_ASSIGN(auto calc_mod,
                         BucketIdCalculator::CreateMod(/*is_pk_table=*/true, /*num_buckets=*/10,
                                                       FieldType::INT, GetDefaultPool()));

    // Calculate with explicit ModBucketFunction
    ASSERT_OK_AND_ASSIGN(auto mod_func, ModBucketFunction::Create(FieldType::INT));
    ASSERT_OK_AND_ASSIGN(std::vector<int32_t> result_explicit,
                         CalculateBucketIds(/*is_pk_table=*/true, /*num_buckets=*/10,
                                            std::move(mod_func), bucket_schema, data_str));

    auto bucket_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_schema->fields()), data_str)
            .ValueOrDie();
    ::ArrowArray c_array;
    EXPECT_TRUE(arrow::ExportArray(*bucket_array, &c_array).ok());
    ::ArrowSchema c_schema;
    EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_schema).ok());
    std::vector<int32_t> result_mod(bucket_array->length());
    ASSERT_OK(calc_mod->CalculateBucketIds(&c_array, &c_schema, result_mod.data()));

    ASSERT_EQ(result_explicit, result_mod);
    // Verify expected values (Java Math.floorMod semantics)
    std::vector<int32_t> expected = {0, 9, 0, 7, 0};
    ASSERT_EQ(expected, result_mod);
}

TEST_F(BucketIdCalculatorTest, TestCreateWithHiveBucketFunction) {
    auto bucket_schema = arrow::schema(arrow::FieldVector({arrow::field("b0", arrow::int32())}));
    std::string data_str = "[[42], [0], [100]]";

    std::vector<HiveFieldInfo> field_infos = {HiveFieldInfo(FieldType::INT)};

    // Calculate with CreateHive
    ASSERT_OK_AND_ASSIGN(auto calc_hive,
                         BucketIdCalculator::CreateHive(/*is_pk_table=*/true, /*num_buckets=*/5,
                                                        field_infos, GetDefaultPool()));

    auto bucket_array =
        arrow::ipc::internal::json::ArrayFromJSON(arrow::struct_(bucket_schema->fields()), data_str)
            .ValueOrDie();
    ::ArrowArray c_array;
    EXPECT_TRUE(arrow::ExportArray(*bucket_array, &c_array).ok());
    ::ArrowSchema c_schema;
    EXPECT_TRUE(arrow::ExportSchema(*bucket_schema, &c_schema).ok());
    std::vector<int32_t> result(bucket_array->length());
    ASSERT_OK(calc_hive->CalculateBucketIds(&c_array, &c_schema, result.data()));

    // Verify all bucket ids are in valid range
    for (auto bucket_id : result) {
        ASSERT_GE(bucket_id, 0);
        ASSERT_LT(bucket_id, 5);
    }
}

}  // namespace paimon::test
