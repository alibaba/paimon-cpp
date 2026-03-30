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

#include "paimon/core/schema/schema_validation.h"

#include <map>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/result.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {

TEST(SchemaValidationTest, TestSimple) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
}

TEST(SchemaValidationTest, TestRowTracking) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {
        {Options::BUCKET, "-1"},
        {Options::ROW_TRACKING_ENABLED, "true"},
        {Options::DATA_EVOLUTION_ENABLED, "true"},
    };
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
}

TEST(SchemaValidationTest, TestWithBlobField) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    std::shared_ptr<arrow::Field> f3 = BlobUtils::ToArrowField("f3", false);
    std::shared_ptr<arrow::Field> f4 = BlobUtils::ToArrowField("f4", false);
    {
        arrow::FieldVector fields = {f0, f1, f2, f3};
        auto schema = arrow::schema(fields);
        std::vector<std::string> primary_keys = {};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "-1"},
            {Options::ROW_TRACKING_ENABLED, "true"},
            {Options::DATA_EVOLUTION_ENABLED, "true"},
        };
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
    }
    {
        arrow::FieldVector fields = {f0, f1, f2, f3};
        auto schema = arrow::schema(fields);
        std::vector<std::string> primary_keys = {};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "-1"},
            {Options::ROW_TRACKING_ENABLED, "true"},
            {Options::DATA_EVOLUTION_ENABLED, "false"},
        };
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "Data evolution config must be enabled for table with BLOB type column.");
    }
    {
        arrow::FieldVector fields = {f0, f1, f2, f3, f4};
        auto schema = arrow::schema(fields);
        std::vector<std::string> primary_keys = {};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "-1"},
            {Options::ROW_TRACKING_ENABLED, "true"},
            {Options::DATA_EVOLUTION_ENABLED, "true"},
        };
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Table with BLOB type column only supports one BLOB column.");
    }
    {
        arrow::FieldVector fields = {f3};
        auto schema = arrow::schema(fields);
        std::vector<std::string> primary_keys = {};
        std::vector<std::string> partition_keys = {};
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "-1"},
            {Options::ROW_TRACKING_ENABLED, "true"},
            {Options::DATA_EVOLUTION_ENABLED, "true"},
        };
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Table with BLOB type column must have other normal columns.");
    }
}

TEST(SchemaValidationTest, TestDuplicateField) {
    auto f0 = arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32()));
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    {
        // duplicate primary keys
        std::vector<std::string> dup_primary_keys = {"f0", "f1", "f1"};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, partition_keys,
                                                 dup_primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "primary key [f0, f1, f1] must not contain duplicate fields. Found: [f1]");
    }
    {
        // duplicate partition keys
        std::vector<std::string> dup_partition_keys = {"f1", "f1"};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, dup_partition_keys,
                                                 primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "partition key [f1, f1] must not contain duplicate fields. Found: [f1]");
    }
    {
        // duplicate bucket keys
        std::map<std::string, std::string> dup_options = {{Options::BUCKET, "2"},
                                                          {Options::BUCKET_KEY, "f0,f0"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, partition_keys,
                                                 primary_keys, dup_options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "bucket key [f0, f0] must not contain duplicate fields. Found: [f0]");
    }
}

TEST(SchemaValidationTest, TestNonExistField) {
    auto f0 = arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32()));
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    {
        // non-exist primary keys
        std::vector<std::string> non_exist_primary_keys = {"f0", "f1", "non-exist"};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, partition_keys,
                                                 non_exist_primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            R"(Table column ["f0", "f1", "f2"] should include all primary key constraint ["f0", "f1", "non-exist"])");
    }
    {
        // non-exist partition keys
        std::vector<std::string> non_exist_partition_keys = {"f1", "non-exist"};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, non_exist_partition_keys,
                                                 primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            R"(Table column ["f0", "f1", "f2"] should include all partition fields ["f1", "non-exist"])");
    }
}

TEST(SchemaValidationTest, NonPrimitivePrimaryKeyList) {
    auto value_field = arrow::field("values", arrow::int32());
    auto f0 = arrow::field("f0", arrow::list(value_field));
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                        "field f0 is unsupported");
}

TEST(SchemaValidationTest, NonPrimitivePrimaryKeyMap) {
    auto f0 = arrow::field("f0", arrow::map(arrow::utf8(), arrow::int32()));
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                        "field f0 is unsupported");
}

TEST(SchemaValidationTest, NonPrimitivePartitionKeyStruct) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto child1 = arrow::field("inner1", arrow::int32());
    auto child2 = arrow::field("inner2", arrow::float64());
    auto f1 = arrow::field("f1", arrow::struct_({child1, child2}));
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                  {Options::BUCKET_KEY, "f0"}};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
    ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                        "field f1 is unsupported");
}

TEST(SchemaValidationTest, TestComplexPartitionKey) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::decimal128(5, 2));
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, {}));
    ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                        "partition field f1 is unsupported");
}

TEST(SchemaValidationTest, TestComplexPartitionKeyWithBlob) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = BlobUtils::ToArrowField("f1");
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> partition_keys = {"f1"};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, /*primary_keys=*/{}, {}));
    ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                        "partition field f1 is unsupported");
}

TEST(SchemaValidationTest, TestDateTypePartitionKey) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::date32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    ASSERT_OK_AND_ASSIGN(
        std::shared_ptr<TableSchema> table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, {}));
    ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
}

TEST(SchemaValidationTest, ValidateFieldsPrefix) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    {
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"}, {Options::BUCKET_KEY, "f0"}, {"fields.f0,f1,f3", "some_value"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "f3 can not be found in table schema.");
    }
    {
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"}, {Options::BUCKET_KEY, "f0"}, {"fields.f0,f1,f2", "some_value"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
    }
    {
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"},
            {Options::BUCKET_KEY, "f0"},
            {Options::FIELDS_DEFAULT_AGG_FUNC, "some_value"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
    }
    {
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"}, {Options::BUCKET_KEY, "f0"}, {"fields.", "f1"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "invalid options key fields.");
    }
}

TEST(SchemaValidationTest, ValidateBucket) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "-1"},
                                                      {Options::BUCKET_KEY, "f0"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "please specify a bucket number.");
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "0"},
                                                      {Options::BUCKET_KEY, "f0"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "The number of buckets needs to be greater than 0.");
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f2"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "You should use dynamic bucket (bucket = -1) mode in cross partition update case");
    }
    {
        std::vector<std::string> primary_keys = {};
        std::vector<std::string> partition_keys = {"f2"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "You should define a 'bucket-key' for bucketed append mode");
    }
    {
        std::vector<std::string> partition_keys = {"f2"};
        std::map<std::string, std::string> options = {{"full-compaction.delta-commits", "2"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, partition_keys,
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "AppendOnlyTable of unware or dynamic bucket does not support "
                            "'full-compaction.delta-commits");
    }
    {
        auto f3 = arrow::field("f3", arrow::map(arrow::utf8(), arrow::int32()));
        arrow::FieldVector new_fields = {f0, f1, f2, f3};
        auto new_schema = arrow::schema(new_fields);
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f3"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, new_schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "Nested type cannot be in bucket-key, in your table these keys are: f3");
    }
}

TEST(SchemaValidationTest, ValidateDeletionVector) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    {
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"},
            {Options::BUCKET_KEY, "f0"},
            {Options::DELETION_VECTORS_ENABLED, "true"},
            {Options::CHANGELOG_PRODUCER, "full-compaction"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "C++ Paimon does not support changelog-producer yet");
    }
    {
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {Options::DELETION_VECTORS_ENABLED, "true"},
                                                      {Options::MERGE_ENGINE, "first-row"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "First row merge engine does not need deletion vectors because there "
                            "is no deletion of old data in this merge engine.");
    }
}

TEST(SchemaValidationTest, ValidateSequenceField) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    std::vector<std::string> primary_keys = {"f0", "f1"};
    std::vector<std::string> partition_keys = {"f1"};
    {
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {Options::SEQUENCE_FIELD, "f0,f1,f2"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
    }
    {
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {Options::SEQUENCE_FIELD, "f0,f1,f3"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "cannot be found in table schema.");
    }
    {
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {Options::SEQUENCE_FIELD, "f0,f1,f2"},
                                                      {Options::MERGE_ENGINE, "first-row"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Do not support using sequence field on FIRST_ROW merge engine.");
    }
    {
        std::map<std::string, std::string> options = {{Options::BUCKET, "-1"},
                                                      {Options::SEQUENCE_FIELD, "f0,f1,f2"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{"f2"},
                                                 /*primary_keys=*/{"f0", "f1"}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "You cannot use sequence.field in cross partition update case (Primary "
                            "key constraint 'f0, f1'  not including all partition fields 'f2').");
    }
}

TEST(SchemaValidationTest, ValidateSequenceGroup) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {"fields.f0,f1.sequence-group", "f2"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_OK(SchemaValidation::ValidateTableSchema(*table_schema));
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {"fields.f0,f3.sequence-group", "f2"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Field f3 can not be found in table schema.");
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {"fields.f0,f1.sequence-group", "f3"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Field f3 can not be found in table schema.");
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {{Options::BUCKET, "2"},
                                                      {Options::BUCKET_KEY, "f0"},
                                                      {"fields.f0,f1.sequence-group", "f0,f1"},
                                                      {"fields.f2.sequence-group", "f0,f1"}};
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "defined repeatedly by multiple groups");
    }
    {
        std::vector<std::string> primary_keys = {"f0", "f1"};
        std::vector<std::string> partition_keys = {"f1"};
        std::map<std::string, std::string> options = {
            {Options::BUCKET, "2"},
            {Options::BUCKET_KEY, "f0"},
            {"fields.f0,f1.sequence-group", "f2"},
            {"fields.f0.aggregate-function", "min"},
        };
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, schema, partition_keys, primary_keys, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Should not define aggregation function on sequence group");
    }
}

TEST(SchemaValidationTest, ValidateInvalidConfiguration) {
    auto f0 = arrow::field("f0", arrow::utf8());
    auto f1 = arrow::field("f1", arrow::int32());
    auto f2 = arrow::field("f2", arrow::float64());
    arrow::FieldVector fields = {f0, f1, f2};
    auto schema = arrow::schema(fields);
    {
        std::map<std::string, std::string> options = {{Options::CHANGELOG_PRODUCER, "input"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Can not set changelog-producer on table without primary keys, please "
                            "define primary keys.");
    }
    {
        auto invalid_field = arrow::field("_SEQUENCE_NUMBER", arrow::int64());
        arrow::FieldVector invalid_fields = fields;
        invalid_fields.push_back(invalid_field);
        auto invalid_schema = arrow::schema(invalid_fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, invalid_schema, /*partition_keys=*/{},
                                /*primary_keys=*/{}, /*options=*/{}));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "field name '_SEQUENCE_NUMBER' in schema cannot be special field.");
    }
    {
        auto invalid_field = arrow::field("_KEY_a", arrow::int64());
        arrow::FieldVector invalid_fields = fields;
        invalid_fields.push_back(invalid_field);
        auto invalid_schema = arrow::schema(invalid_fields);
        ASSERT_OK_AND_ASSIGN(
            std::shared_ptr<TableSchema> table_schema,
            TableSchema::Create(/*schema_id=*/0, invalid_schema, /*partition_keys=*/{},
                                /*primary_keys=*/{}, /*options=*/{}));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "field name '_KEY_a' in schema cannot start with '_KEY_'");
    }
    {
        std::map<std::string, std::string> options = {{Options::CHANGELOG_PRODUCER, "input"},
                                                      {Options::MERGE_ENGINE, "first-row"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{"f0"}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "C++ Paimon does not support changelog-producer yet");
    }
    {
        std::map<std::string, std::string> options = {{Options::CHANGELOG_PRODUCER, "lookup"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{"f0"}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "C++ Paimon does not support changelog-producer yet");
    }
    // test for row tracking
    {
        std::map<std::string, std::string> options = {{Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::BUCKET, "1"},
                                                      {Options::BUCKET_KEY, "f0"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(
            SchemaValidation::ValidateTableSchema(*table_schema),
            "Cannot define bucket for row tracking table, it only support bucket = -1");
    }
    {
        std::map<std::string, std::string> options = {{Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::BUCKET, "-1"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{"f0"}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Cannot define primary key for row tracking table");
    }
    {
        std::map<std::string, std::string> options = {{Options::DATA_EVOLUTION_ENABLED, "true"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Data evolution config must enabled with row-tracking.enabled");
    }
    {
        std::map<std::string, std::string> options = {{Options::ROW_TRACKING_ENABLED, "true"},
                                                      {Options::DATA_EVOLUTION_ENABLED, "true"},
                                                      {Options::DELETION_VECTORS_ENABLED, "true"}};
        ASSERT_OK_AND_ASSIGN(std::shared_ptr<TableSchema> table_schema,
                             TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{},
                                                 /*primary_keys=*/{}, options));
        ASSERT_NOK_WITH_MSG(SchemaValidation::ValidateTableSchema(*table_schema),
                            "Data evolution config must disabled with deletion-vectors.enabled");
    }
}
}  // namespace paimon::test
