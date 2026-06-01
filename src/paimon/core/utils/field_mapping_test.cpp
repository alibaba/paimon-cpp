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

#include "paimon/core/utils/field_mapping.h"

#include <algorithm>

#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/predicate_filter.h"
#include "paimon/data/decimal.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon::test {
class FieldMappingTest : public ::testing::Test {
 public:
    void SetUp() override {}
    void TearDown() override {}
    void CheckPartitionInfo(const PartitionInfo& result, const PartitionInfo& expected) const {
        ASSERT_EQ(result.partition_read_schema, expected.partition_read_schema);
        ASSERT_EQ(result.idx_in_target_read_schema, expected.idx_in_target_read_schema);
        ASSERT_EQ(result.idx_in_partition, expected.idx_in_partition);
        if (expected.partition_filter == nullptr) {
            ASSERT_FALSE(result.partition_filter);
        } else {
            ASSERT_TRUE(result.partition_filter);
            ASSERT_EQ(*result.partition_filter, *expected.partition_filter);
        }
    }

    void CheckNonPartitionInfo(const NonPartitionInfo& result,
                               const NonPartitionInfo& expected) const {
        ASSERT_EQ(result.non_partition_read_schema, expected.non_partition_read_schema);
        ASSERT_EQ(result.non_partition_data_schema, expected.non_partition_data_schema);
        ASSERT_EQ(result.idx_in_target_read_schema, expected.idx_in_target_read_schema);
        if (expected.non_partition_filter == nullptr) {
            ASSERT_FALSE(result.non_partition_filter);
        } else {
            ASSERT_TRUE(result.non_partition_filter);
            ASSERT_EQ(result.non_partition_filter->ToString(),
                      expected.non_partition_filter->ToString());
            ASSERT_EQ(*result.non_partition_filter, *expected.non_partition_filter);
        }
    }
    void CheckNonExistFieldInfo(const NonExistFieldInfo& result,
                                const NonExistFieldInfo& expected) const {
        ASSERT_EQ(result.non_exist_read_schema, expected.non_exist_read_schema);
        ASSERT_EQ(result.idx_in_target_read_schema, expected.idx_in_target_read_schema);
    }

 private:
    std::vector<DataField> fields_ = {DataField(0, arrow::field("f0", arrow::utf8())),
                                      DataField(1, arrow::field("f1", arrow::int32())),
                                      DataField(2, arrow::field("f2", arrow::int32())),
                                      DataField(3, arrow::field("f3", arrow::float64()))};
    std::shared_ptr<arrow::Schema> schema_ = DataField::ConvertDataFieldsToArrowSchema(fields_);
};

TEST_F(FieldMappingTest, TestEmptyPartitionKeys) {
    // test no partition keys in schema
    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, not_equal}));
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(
                             schema_, /*partition_keys=*/std::vector<std::string>(), predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    ASSERT_EQ(mapping->partition_info, std::nullopt);
    ASSERT_EQ(mapping->non_exist_field_info, std::nullopt);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = fields_;
    expected_non_part_info.non_partition_data_schema = fields_;
    expected_non_part_info.idx_in_target_read_schema = {0, 1, 2, 3};
    expected_non_part_info.non_partition_filter =
        std::dynamic_pointer_cast<PredicateFilter>(predicate);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestCompoundPartitionPredicate) {
    // test partition fields equals schema fields
    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(auto and_predicate, PredicateBuilder::And({equal, not_equal}));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f2",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::DOUBLE, Literal(20.1));
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({and_predicate, or_predicate}));

    std::vector<std::string> partition_keys = {"f0", "f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(schema_, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {fields_[0], fields_[1], fields_[2]};
    expected_part_info.idx_in_target_read_schema = {0, 1, 2};
    expected_part_info.idx_in_partition = {0, 1, 2};
    // less_than predicate includes non-partition-field, therefore, or_predicate is removed from
    // partition_filter
    expected_part_info.partition_filter = std::dynamic_pointer_cast<PredicateFilter>(and_predicate);
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {fields_[3]};
    expected_non_part_info.non_partition_data_schema = {fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {3};
    // or_predicate and and_predicate both include partition fields, therefore, non_partition_filter
    // is null
    expected_non_part_info.non_partition_filter = nullptr;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestPartitionKeysEqualSchema) {
    // test partition fields equals schema fields
    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, not_equal}));

    std::vector<std::string> partition_keys = {"f0", "f1", "f2", "f3"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(schema_, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = fields_;
    expected_part_info.idx_in_target_read_schema = {0, 1, 2, 3};
    expected_part_info.idx_in_partition = {0, 1, 2, 3};
    expected_part_info.partition_filter = std::dynamic_pointer_cast<PredicateFilter>(predicate);
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {};
    expected_non_part_info.idx_in_target_read_schema = {};
    expected_non_part_info.non_partition_filter = nullptr;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestAllPartitionKeysInSchema) {
    // test all partition keys in schema, with no predicate in partition keys
    std::string val("hello");
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, val.data(), val.size()));

    std::vector<std::string> partition_keys = {"f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(schema_, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {fields_[1], fields_[2]};
    expected_part_info.idx_in_target_read_schema = {1, 2};
    expected_part_info.idx_in_partition = {0, 1};
    expected_part_info.partition_filter = nullptr;
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {fields_[0], fields_[3]};
    expected_non_part_info.non_partition_data_schema = {fields_[0], fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {0, 3};
    expected_non_part_info.non_partition_filter = predicate;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestAllPartitionKeysInSchema2) {
    // test all partition keys in schema, with all predicates in partition field
    std::string val("hello");
    auto predicate = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));

    std::vector<std::string> partition_keys = {"f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(schema_, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {fields_[1], fields_[2]};
    expected_part_info.idx_in_target_read_schema = {1, 2};
    expected_part_info.idx_in_partition = {0, 1};
    auto new_predicate = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
    expected_part_info.partition_filter = new_predicate;
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_data_schema = {fields_[0], fields_[3]};
    expected_non_part_info.non_partition_read_schema = {fields_[0], fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {0, 3};
    expected_non_part_info.non_partition_filter = nullptr;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestAllPartitionKeysInSchema3) {
    // test all partition keys in schema, with predicates both in partition field and
    // non-partition field
    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/1, /*field_name=*/"f1",
                                                FieldType::INT, Literal(20));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f2",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::DOUBLE, Literal(10.4));
    ASSERT_OK_AND_ASSIGN(auto predicate,
                         PredicateBuilder::And({equal, not_equal, greater_than, less_than}));

    std::vector<std::string> partition_keys = {"f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(schema_, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {fields_[1], fields_[2]};
    expected_part_info.idx_in_target_read_schema = {1, 2};
    expected_part_info.idx_in_partition = {0, 1};
    auto not_equal_new = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"f1",
                                                    FieldType::INT, Literal(20));
    auto greater_than_new = PredicateBuilder::GreaterThan(/*field_index=*/1, /*field_name=*/"f2",
                                                          FieldType::INT, Literal(30));
    expected_part_info.partition_filter =
        PredicateBuilder::And({not_equal_new, greater_than_new}).value_or(nullptr);
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {fields_[0], fields_[3]};
    expected_non_part_info.non_partition_data_schema = {fields_[0], fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {0, 3};
    expected_non_part_info.non_partition_filter =
        PredicateBuilder::And({equal, less_than}).value_or(nullptr);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestPartialPartitionKeysInSchema) {
    // test partial partition keys in schema, with predicates both in partition field and
    // non-partition field
    std::vector<DataField> fields = {DataField(2, arrow::field("f2", arrow::int32())),
                                     DataField(3, arrow::field("f3", arrow::float64())),
                                     DataField(0, arrow::field("f0", arrow::utf8()))};
    std::shared_ptr<arrow::Schema> read_schema = DataField::ConvertDataFieldsToArrowSchema(fields);

    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/0, /*field_name=*/"f2",
                                                      FieldType::INT, Literal(30));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f3",
                                                FieldType::DOUBLE, Literal(10.4));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, greater_than, less_than}));

    std::vector<std::string> partition_keys = {"f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {fields_[2]};
    expected_part_info.idx_in_target_read_schema = {0};
    expected_part_info.idx_in_partition = {1};
    expected_part_info.partition_filter = PredicateBuilder::GreaterThan(
        /*field_index=*/1, /*field_name=*/"f2", FieldType::INT, Literal(30));
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);
    ASSERT_EQ(std::dynamic_pointer_cast<LeafPredicateImpl>(
                  mapping->partition_info.value().partition_filter)
                  ->FieldIndex(),
              1);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {fields_[0], fields_[3]};
    expected_non_part_info.non_partition_data_schema = {fields_[0], fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {2, 1};
    auto equal_new =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, val.data(), val.size()));
    auto less_than_new = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                    FieldType::DOUBLE, Literal(10.4));
    expected_non_part_info.non_partition_filter =
        PredicateBuilder::And({equal_new, less_than_new}).value_or(nullptr);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

TEST_F(FieldMappingTest, TestNoPartitionKeysInReadSchema) {
    // test no partition keys in read schema, with predicates in non-partition field
    std::vector<DataField> fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                     DataField(0, arrow::field("f0", arrow::utf8()))};
    std::shared_ptr<arrow::Schema> read_schema = DataField::ConvertDataFieldsToArrowSchema(fields);

    std::string val("hello");
    auto equal = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f0", FieldType::STRING,
                                         Literal(FieldType::STRING, val.data(), val.size()));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/1, /*field_name=*/"f3",
                                                FieldType::DOUBLE, Literal(10.4));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, less_than}));

    std::vector<std::string> partition_keys = {"f1", "f2"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(schema_));

    ASSERT_EQ(mapping->partition_info, std::nullopt);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {fields_[0], fields_[3]};
    expected_non_part_info.non_partition_data_schema = {fields_[0], fields_[3]};
    expected_non_part_info.idx_in_target_read_schema = {1, 0};
    auto equal_new =
        PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::STRING,
                                Literal(FieldType::STRING, val.data(), val.size()));
    auto less_than_new = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                    FieldType::DOUBLE, Literal(10.4));
    expected_non_part_info.non_partition_filter =
        PredicateBuilder::And({equal_new, less_than_new}).value_or(nullptr);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

// test generate mapping with schema evolution
TEST_F(FieldMappingTest, TestSchemaEvolution) {
    // add field / delete field / rename / casting
    // without predicate
    std::vector<DataField> data_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::float64())),
                                          DataField(2, arrow::field("a", arrow::int32())),
                                          DataField(3, arrow::field("b", arrow::int32())),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(5, arrow::field("d", arrow::int32()))};
    std::shared_ptr<arrow::Schema> data_schema =
        DataField::ConvertDataFieldsToArrowSchema(data_fields);

    std::vector<DataField> read_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::float64())),
                                          DataField(3, arrow::field("c", arrow::int64())),
                                          DataField(5, arrow::field("a", arrow::float32())),
                                          DataField(7, arrow::field("d", arrow::int32())),
                                          DataField(8, arrow::field("e", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    std::vector<std::string> partition_keys = {"key0", "key1"};
    ASSERT_OK_AND_ASSIGN(
        auto mapping_builder,
        FieldMappingBuilder::Create(read_schema, partition_keys, /*predicate=*/nullptr));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_fields));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::float64()))};
    expected_part_info.idx_in_target_read_schema = {0, 1};
    expected_part_info.idx_in_partition = {0, 1};
    expected_part_info.partition_filter = nullptr;
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {
        DataField(3, arrow::field("c", arrow::int64())),
        DataField(5, arrow::field("a", arrow::float32()))};
    expected_non_part_info.non_partition_data_schema = {
        DataField(3, arrow::field("b", arrow::int32())),
        DataField(5, arrow::field("d", arrow::int32()))};
    expected_non_part_info.idx_in_target_read_schema = {2, 3};
    expected_non_part_info.non_partition_filter = nullptr;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);

    NonExistFieldInfo expected_non_exist_field_info;
    expected_non_exist_field_info.non_exist_read_schema = {
        DataField(7, arrow::field("d", arrow::int32())),
        DataField(8, arrow::field("e", arrow::int32()))};
    expected_non_exist_field_info.idx_in_target_read_schema = {4, 5};
    CheckNonExistFieldInfo(mapping->non_exist_field_info.value(), expected_non_exist_field_info);
}

TEST_F(FieldMappingTest, TestSchemaEvolutionWithPredicate) {
    // add field / delete field / rename / reverse order / casting
    // with predicate
    // field_0 and field_1 is partition key.
    // simulate schema evolution:
    // field_2 is deleted;
    // field_3 is renamed (b->c) and change type (Decimal(5, 2)->Decimal(6, 3));
    // field_4 is deleted;
    // field_5 is renamed (d->a) and change type (INT->BIGINT);
    // field_6 order reversed;
    // field_7 is added to the middle;
    // field_8 is added to the last field.
    std::vector<DataField> data_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::float64())),
                                          DataField(2, arrow::field("a", arrow::int32())),
                                          DataField(3, arrow::field("b", arrow::decimal128(5, 2))),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(5, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32()))};

    std::vector<DataField> table_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                           DataField(1, arrow::field("key1", arrow::float64())),
                                           DataField(6, arrow::field("k", arrow::int32())),
                                           DataField(3, arrow::field("c", arrow::decimal128(6, 3))),
                                           DataField(7, arrow::field("d", arrow::int32())),
                                           DataField(5, arrow::field("a", arrow::int64())),
                                           DataField(8, arrow::field("e", arrow::int32()))};
    // the field order of read schema and table schema is consistent
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(table_fields);

    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"key0", FieldType::INT, Literal(4));
    auto equal = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"key1",
                                         FieldType::DOUBLE, Literal(3.0));
    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/2, /*field_name=*/"k",
                                                       FieldType::INT, Literal(10));
    // greater_than will not be pushed down, as with casting, only integer predicates can be pushed
    // down
    auto greater_than = PredicateBuilder::GreaterThan(
        /*field_index=*/3, /*field_name=*/"c", FieldType::DECIMAL, Literal(Decimal(6, 3, 12345)));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/4, /*field_name=*/"d",
                                                FieldType::INT, Literal(40));
    // in can be pushed down
    auto in = PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"a", FieldType::BIGINT,
                                   {Literal(100l)});
    auto not_in =
        PredicateBuilder::In(/*field_index=*/6, /*field_name=*/"e", FieldType::INT, {Literal(50)});

    ASSERT_OK_AND_ASSIGN(
        auto predicate, PredicateBuilder::And({greater_or_equal, equal, less_or_equal, greater_than,
                                               not_equal, in, not_in}));

    std::vector<std::string> partition_keys = {"key0", "key1"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_fields));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::float64()))};
    expected_part_info.idx_in_target_read_schema = {0, 1};
    expected_part_info.idx_in_partition = {0, 1};
    expected_part_info.partition_filter =
        PredicateBuilder::And({greater_or_equal, equal}).value_or(nullptr);
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {
        DataField(3, arrow::field("c", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("a", arrow::int64())),
        DataField(6, arrow::field("k", arrow::int32()))};
    expected_non_part_info.non_partition_data_schema = {
        DataField(3, arrow::field("b", arrow::decimal128(5, 2))),
        DataField(5, arrow::field("d", arrow::int32())),
        DataField(6, arrow::field("k", arrow::int32()))};
    expected_non_part_info.idx_in_target_read_schema = {3, 5, 2};
    auto less_or_equal_new = PredicateBuilder::LessOrEqual(/*field_index=*/6, /*field_name=*/"k",
                                                           FieldType::INT, Literal(10));
    auto in_new =
        PredicateBuilder::In(/*field_index=*/5, /*field_name=*/"d", FieldType::INT, {Literal(100)});
    expected_non_part_info.non_partition_filter =
        PredicateBuilder::And({less_or_equal_new, in_new}).value_or(nullptr);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);

    NonExistFieldInfo expected_non_exist_field_info;
    expected_non_exist_field_info.non_exist_read_schema = {
        DataField(7, arrow::field("d", arrow::int32())),
        DataField(8, arrow::field("e", arrow::int32()))};
    expected_non_exist_field_info.idx_in_target_read_schema = {4, 6};
    CheckNonExistFieldInfo(mapping->non_exist_field_info.value(), expected_non_exist_field_info);
}

TEST_F(FieldMappingTest, TestSchemaEvolutionWithPredicate2) {
    // add field / delete field / rename / reverse order / casting
    // with predicate
    // field_0 and field_1 is partition key.
    // simulate schema evolution:
    // field_2 is deleted;
    // field_3 is renamed (b->c) and change type (Decimal(5, 2)->Decimal(6, 3));
    // field_4 is deleted;
    // field_5 is renamed (d->a) and change type (INT->BIGINT);
    // field_6 order reversed;
    // field_7 is added to the middle;
    // field_8 is added to the last field.
    std::vector<DataField> data_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::float64())),
                                          DataField(2, arrow::field("a", arrow::int32())),
                                          DataField(3, arrow::field("b", arrow::decimal128(5, 2))),
                                          DataField(4, arrow::field("c", arrow::int32())),
                                          DataField(5, arrow::field("d", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32()))};

    std::vector<DataField> table_fields = {DataField(0, arrow::field("key0", arrow::int32())),
                                           DataField(1, arrow::field("key1", arrow::float64())),
                                           DataField(6, arrow::field("k", arrow::int32())),
                                           DataField(3, arrow::field("c", arrow::decimal128(6, 3))),
                                           DataField(7, arrow::field("d", arrow::int32())),
                                           DataField(5, arrow::field("a", arrow::int64())),
                                           DataField(8, arrow::field("e", arrow::int32()))};

    // the field order of read schema and table schema is inconsistent
    std::vector<DataField> read_fields = {DataField(7, arrow::field("d", arrow::int32())),
                                          DataField(1, arrow::field("key1", arrow::float64())),
                                          DataField(5, arrow::field("a", arrow::int64())),
                                          DataField(8, arrow::field("e", arrow::int32())),
                                          DataField(6, arrow::field("k", arrow::int32())),
                                          DataField(3, arrow::field("c", arrow::decimal128(6, 3))),
                                          DataField(0, arrow::field("key0", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto greater_or_equal = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/6, /*field_name=*/"key0", FieldType::INT, Literal(4));
    auto equal = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"key1",
                                         FieldType::DOUBLE, Literal(3.0));
    auto less_or_equal = PredicateBuilder::LessOrEqual(/*field_index=*/4, /*field_name=*/"k",
                                                       FieldType::INT, Literal(10));
    // greater_than will not be pushed down, as with casting, only integer predicates can be pushed
    // down
    auto greater_than = PredicateBuilder::GreaterThan(
        /*field_index=*/5, /*field_name=*/"c", FieldType::DECIMAL, Literal(Decimal(6, 3, 12345)));
    auto not_equal = PredicateBuilder::NotEqual(/*field_index=*/0, /*field_name=*/"d",
                                                FieldType::INT, Literal(40));
    // in will not be pushed down, as with casting, literal from BIGINT to INT is overflow
    auto in = PredicateBuilder::In(/*field_index=*/2, /*field_name=*/"a", FieldType::BIGINT,
                                   {Literal(9223372036854775807l)});
    auto not_in =
        PredicateBuilder::In(/*field_index=*/3, /*field_name=*/"e", FieldType::INT, {Literal(50)});

    ASSERT_OK_AND_ASSIGN(
        auto predicate, PredicateBuilder::And({greater_or_equal, equal, less_or_equal, greater_than,
                                               not_equal, in, not_in}));

    std::vector<std::string> partition_keys = {"key0", "key1"};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_fields));

    PartitionInfo expected_part_info;
    expected_part_info.partition_read_schema = {
        DataField(0, arrow::field("key0", arrow::int32())),
        DataField(1, arrow::field("key1", arrow::float64()))};
    expected_part_info.idx_in_target_read_schema = {6, 1};
    expected_part_info.idx_in_partition = {0, 1};
    auto greater_or_equal_new = PredicateBuilder::GreaterOrEqual(
        /*field_index=*/0, /*field_name=*/"key0", FieldType::INT, Literal(4));
    auto equal_new = PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"key1",
                                             FieldType::DOUBLE, Literal(3.0));
    expected_part_info.partition_filter =
        PredicateBuilder::And({greater_or_equal_new, equal_new}).value_or(nullptr);
    CheckPartitionInfo(mapping->partition_info.value(), expected_part_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = {
        DataField(3, arrow::field("c", arrow::decimal128(6, 3))),
        DataField(5, arrow::field("a", arrow::int64())),
        DataField(6, arrow::field("k", arrow::int32()))};
    expected_non_part_info.non_partition_data_schema = {
        DataField(3, arrow::field("b", arrow::decimal128(5, 2))),
        DataField(5, arrow::field("d", arrow::int32())),
        DataField(6, arrow::field("k", arrow::int32()))};
    expected_non_part_info.idx_in_target_read_schema = {5, 2, 4};
    auto less_or_equal_new = PredicateBuilder::LessOrEqual(/*field_index=*/6, /*field_name=*/"k",
                                                           FieldType::INT, Literal(10));
    expected_non_part_info.non_partition_filter =
        PredicateBuilder::And({less_or_equal_new}).value_or(nullptr);
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);

    NonExistFieldInfo expected_non_exist_field_info;
    expected_non_exist_field_info.non_exist_read_schema = {
        DataField(7, arrow::field("d", arrow::int32())),
        DataField(8, arrow::field("e", arrow::int32()))};
    expected_non_exist_field_info.idx_in_target_read_schema = {0, 3};
    CheckNonExistFieldInfo(mapping->non_exist_field_info.value(), expected_non_exist_field_info);
}

TEST_F(FieldMappingTest, TestCompoundPredicateWithoutPushDown) {
    std::vector<DataField> data_fields = {DataField(0, arrow::field("f0", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::float64())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::utf8()))};

    // the field type of read schema and data schema are inconsistent (f2, f3)
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::int32())),
                                          DataField(1, arrow::field("f1", arrow::float64())),
                                          DataField(2, arrow::field("f2", arrow::int64())),
                                          DataField(3, arrow::field("f3", arrow::int32()))};
    std::shared_ptr<arrow::Schema> read_schema =
        DataField::ConvertDataFieldsToArrowSchema(read_fields);

    auto equal = PredicateBuilder::Equal(/*field_index=*/0, /*field_name=*/"f0", FieldType::INT,
                                         Literal(55));
    auto greater_than = PredicateBuilder::GreaterThan(/*field_index=*/2, /*field_name=*/"f2",
                                                      FieldType::BIGINT, Literal(30l));
    auto less_than = PredicateBuilder::LessThan(/*field_index=*/3, /*field_name=*/"f3",
                                                FieldType::INT, Literal(55));
    ASSERT_OK_AND_ASSIGN(auto or_predicate, PredicateBuilder::Or({greater_than, less_than}));
    ASSERT_OK_AND_ASSIGN(auto predicate, PredicateBuilder::And({equal, or_predicate}));

    std::vector<std::string> partition_keys = {};
    ASSERT_OK_AND_ASSIGN(auto mapping_builder,
                         FieldMappingBuilder::Create(read_schema, partition_keys, predicate));
    ASSERT_TRUE(mapping_builder);
    ASSERT_OK_AND_ASSIGN(auto mapping, mapping_builder->CreateFieldMapping(data_fields));
    ASSERT_FALSE(mapping->partition_info);

    NonPartitionInfo expected_non_part_info;
    expected_non_part_info.non_partition_read_schema = read_fields;
    expected_non_part_info.non_partition_data_schema = data_fields;
    expected_non_part_info.idx_in_target_read_schema = {0, 1, 2, 3};
    // or_predicate includes int to string predicate converter, therefore it is removed
    expected_non_part_info.non_partition_filter = equal;
    CheckNonPartitionInfo(mapping->non_partition_info, expected_non_part_info);
}

}  // namespace paimon::test
