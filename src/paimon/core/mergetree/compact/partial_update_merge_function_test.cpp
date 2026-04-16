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

#include "paimon/core/mergetree/compact/partial_update_merge_function.h"

#include <algorithm>
#include <ostream>
#include <variant>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/data/data_define.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/core_options.h"
#include "paimon/core/mergetree/compact/aggregate/field_last_non_null_value_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_min_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_primary_key_agg.h"
#include "paimon/core/mergetree/compact/aggregate/field_sum_agg.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/defs.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/testing/utils/binary_row_generator.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
class PartialUpdateMergeFunctionTest : public testing::Test {
 public:
    void SetUp() override {
        sequence_ = 0;
        pool_ = GetDefaultPool();
    }
    void TearDown() override {}

    void Add(const std::unique_ptr<PartialUpdateMergeFunction>& mfunc,
             const BinaryRowGenerator::ValueType& values) {
        Add(mfunc, RowKind::Insert(), values);
    }
    void Add(const std::unique_ptr<PartialUpdateMergeFunction>& mfunc, const RowKind* row_kind,
             const BinaryRowGenerator::ValueType& values) {
        std::shared_ptr<InternalRow> key = BinaryRowGenerator::GenerateRowPtr({1}, pool_.get());
        ASSERT_OK(
            mfunc->Add(KeyValue(row_kind, sequence_++, KeyValue::UNKNOWN_LEVEL, std::move(key),
                                BinaryRowGenerator::GenerateRowPtr(values, pool_.get()))));
    }

    std::vector<DataField> CreateDataFields(int32_t value_arity) {
        std::vector<DataField> data_fields;
        for (int32_t i = 0; i < value_arity; i++) {
            data_fields.emplace_back(i, arrow::field("f" + std::to_string(i), arrow::int32()));
        }
        return data_fields;
    }

    void CheckResult(const std::unique_ptr<PartialUpdateMergeFunction>& mfunc,
                     const std::vector<VariantType>& expected) {
        auto typed_row = mfunc->row_.get();
        ASSERT_TRUE(typed_row);
        auto expected_row = GenericRow::Of(expected);
        ASSERT_EQ(*expected_row, *typed_row) << "expect:" << expected_row->ToString() << std::endl
                                             << "result:" << typed_row->ToString();
    }

    Result<std::unique_ptr<PartialUpdateMergeFunction>> CreateMergeFunctionResult(
        int32_t value_arity, const std::map<std::string, std::string>& options_map) {
        std::vector<DataField> data_fields = CreateDataFields(value_arity);
        auto value_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
        PAIMON_ASSIGN_OR_RAISE(CoreOptions options, CoreOptions::FromMap(options_map));
        std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
        std::set<std::string> seq_group_key_set;
        PAIMON_RETURN_NOT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
            options, &value_field_to_seq_group_field, &seq_group_key_set));
        return PartialUpdateMergeFunction::Create(value_schema, /*primary_keys=*/{"f0"}, options,
                                                  value_field_to_seq_group_field,
                                                  seq_group_key_set);
    }

    std::string CreateMergeFunctionWithInvalidOptions(
        int32_t value_arity, const std::map<std::string, std::string>& options_map) {
        auto mfunc = CreateMergeFunctionResult(value_arity, options_map);
        EXPECT_FALSE(mfunc.ok());
        return mfunc.status().ToString();
    }

    std::unique_ptr<PartialUpdateMergeFunction> CreateMergeFunction(
        int32_t value_arity, const std::map<std::string, std::string>& options_map) {
        EXPECT_OK_AND_ASSIGN(std::unique_ptr<PartialUpdateMergeFunction> mfunc,
                             CreateMergeFunctionResult(value_arity, options_map));
        return mfunc;
    }

    std::unique_ptr<PartialUpdateMergeFunction> CreateMergeFunctionWithProjection(
        const std::vector<DataField>& table_fields, const std::vector<DataField>& value_fields,
        const std::map<std::string, std::string>& options_map,
        const std::vector<DataField>& expected_completed_value_fields) {
        TableSchema table_schema;
        table_schema.fields_ = table_fields;

        std::vector<DataField> copy_value_fields = value_fields;
        EXPECT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(options_map));
        std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
        std::set<std::string> seq_group_key_set;
        EXPECT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
            options, &value_field_to_seq_group_field, &seq_group_key_set));
        EXPECT_OK(PartialUpdateMergeFunction::CompleteSequenceGroupFields(
            table_schema, value_field_to_seq_group_field, &copy_value_fields));
        EXPECT_EQ(copy_value_fields, expected_completed_value_fields);
        auto value_schema = DataField::ConvertDataFieldsToArrowSchema(copy_value_fields);
        EXPECT_OK_AND_ASSIGN(
            std::unique_ptr<PartialUpdateMergeFunction> mfunc,
            PartialUpdateMergeFunction::Create(value_schema, {"f0"}, options,
                                               value_field_to_seq_group_field, seq_group_key_set));
        return mfunc;
    }

 private:
    int64_t sequence_ = 0;
    std::shared_ptr<MemoryPool> pool_ = GetDefaultPool();
};
TEST_F(PartialUpdateMergeFunctionTest, TestNonNull) {
    auto mfunc = CreateMergeFunction(/*value_arity=*/7, {});
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2, 2, NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 2, 1});
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroup) {
    std::map<std::string, std::string> options = {{"fields.f3.sequence-group", "f1,f2"},
                                                  {"fields.f6.sequence-group", "f4,f5"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/7, options);
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2, 2, NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 1, 1, 1});
    Add(mfunc, {1, 3, 3, 1, 3, 3, 3});
    CheckResult(mfunc, {1, 2, 2, 2, 3, 3, 3});

    // test delete
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 1, 1, NullType()});
    CheckResult(mfunc, {1, NullType(), NullType(), 3, 3, 3, 3});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 1, 1, 4});
    CheckResult(mfunc, {1, NullType(), NullType(), 3, NullType(), NullType(), 4});
    Add(mfunc, {1, 4, 4, 4, 5, 5, 5});
    CheckResult(mfunc, {1, 4, 4, 4, 5, 5, 5});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 6, 1, 1, 6});
    CheckResult(mfunc, {1, NullType(), NullType(), 6, NullType(), NullType(), 6});
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroupPartialDelete) {
    std::map<std::string, std::string> options = {
        {"fields.f3.sequence-group", "f1,f2"},
        {"fields.f6.sequence-group", "f4,f5"},
        {"partial-update.remove-record-on-sequence-group", "f6"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/7, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2, 2, NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 1, 1, 1});
    Add(mfunc, {1, 3, 3, 1, 3, 3, 3});
    CheckResult(mfunc, {1, 2, 2, 2, 3, 3, 3});

    // delete
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 1, 1, NullType()});
    CheckResult(mfunc, {1, NullType(), NullType(), 3, 3, 3, 3});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 1, 1, 4});
    CheckResult(mfunc, {1, 1, 1, 3, 1, 1, 4});
    Add(mfunc, {1, 4, 4, 4, 5, 5, 5});
    CheckResult(mfunc, {1, 4, 4, 4, 5, 5, 5});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 6, 1, 1, 6});
    CheckResult(mfunc, {1, 1, 1, 6, 1, 1, 6});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFields) {
    std::map<std::string, std::string> options = {{"fields.f3,f4.sequence-group", "f1,f2"},
                                                  {"fields.f7,f8.sequence-group", "f5,f6"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/9, options);
    mfunc->Reset();

    // test null sequence field
    Add(mfunc, {1, NullType(), NullType(), NullType(), NullType(), 1, 1, 1, 3});
    Add(mfunc, {1, 2, 2, NullType(), NullType(), 2, 2, 1, 3});
    CheckResult(mfunc, {1, NullType(), NullType(), NullType(), NullType(), 2, 2, 1, 3});
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1, 3});
    Add(mfunc, {1, 2, 2, 2, 2, 2, 1, 1, NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 1, 1, 1, 3});
    Add(mfunc, {1, 1, 3, 1, 3, 3, 3, 3, 2});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 3, 3, 3, 2});

    // delete
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 3, 1, 1, NullType(), NullType()});
    CheckResult(mfunc, {1, NullType(), NullType(), 3, 3, 3, 3, 3, 2});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3, 1, 1, 1, 4, 4});
    CheckResult(mfunc, {1, NullType(), NullType(), 3, 3, NullType(), NullType(), 4, 4});
    Add(mfunc, {1, 4, 4, 4, 4, 5, 5, 5, 5});
    CheckResult(mfunc, {1, 4, 4, 4, 4, 5, 5, 5, 5});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 6, 1, 1, 1, 6, 1});
    CheckResult(mfunc, {1, NullType(), NullType(), 6, 1, NullType(), NullType(), 6, 1});
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroupDefaultAggFunc) {
    std::map<std::string, std::string> options = {
        {"fields.f3.sequence-group", "f1,f2"},
        {"fields.f6.sequence-group", "f4,f5"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "last_non_null_value"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/7, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2, 2, NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 1, 1, 1});
    Add(mfunc, {1, 3, 3, 1, 3, 3, 3});
    CheckResult(mfunc, {1, 2, 2, 2, 3, 3, 3});
    Add(mfunc, {1, 4, NullType(), 4, 5, NullType(), 5});
    CheckResult(mfunc, {1, 4, 2, 4, 5, 3, 5});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsDefaultAggFunc) {
    std::map<std::string, std::string> options = {
        {"fields.f3,f4.sequence-group", "f1,f2"},
        {"fields.f7,f8.sequence-group", "f5,f6"},
        {Options::FIELDS_DEFAULT_AGG_FUNC, "last_non_null_value"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/9, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2, 2, 2, NullType(), NullType()});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 1, 1, 1, 1});
    Add(mfunc, {1, 3, 3, 1, 1, 3, 3, 3, 3});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 3, 3, 3, 3});
    Add(mfunc, {1, 4, NullType(), 4, 4, 5, NullType(), 5, 5});
    CheckResult(mfunc, {1, 4, 2, 4, 4, 5, 3, 5, 5});
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroupDefinedNoField) {
    std::map<std::string, std::string> options = {{"fields.f7.sequence-group", "f1,f2"},
                                                  {"fields.f6.sequence-group", "f4,f5"}};
    auto error_msg = CreateMergeFunctionWithInvalidOptions(/*value_arity=*/7, options);
    ASSERT_TRUE(
        error_msg.find("cannot find sequence group field f7 in value schema, unexpected.") !=
        std::string::npos);
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroupRepeatDefine) {
    std::map<std::string, std::string> options_map = {{"fields.f3.sequence-group", "f1,f2"},
                                                      {"fields.f4.sequence-group", "f1,f2"}};
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(options_map));

    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;
    ASSERT_NOK_WITH_MSG(PartialUpdateMergeFunction::ParseSequenceGroupFields(
                            options, &value_field_to_seq_group_field, &seq_group_key_set),
                        "field f1 is defined repeatedly by multiple groups: f4 and f3");
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsRepeatDefine) {
    std::map<std::string, std::string> options_map = {{"fields.f3,f4.sequence-group", "f1,f2"},
                                                      {"fields.f5,f6.sequence-group", "f1,f2"}};
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(options_map));
    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;
    ASSERT_NOK_WITH_MSG(PartialUpdateMergeFunction::ParseSequenceGroupFields(
                            options, &value_field_to_seq_group_field, &seq_group_key_set),
                        "field f1 is defined repeatedly by multiple groups: f5,f6 and f3,f4");
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjection) {
    std::map<std::string, std::string> options_map = {{"fields.f4.sequence-group", "f1,f3"},
                                                      {"fields.f5.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    std::vector<DataField> value_fields = {table_fields[1], table_fields[3], table_fields[7]};

    std::vector<DataField> expected_completed_value_fields = {
        table_fields[1], table_fields[3], table_fields[7], table_fields[4], table_fields[5]};

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, {2, 6, 2, 2, 2});
    CheckResult(mfunc, {2, 6, 2, 2, 2});

    // enable field updated by null
    Add(mfunc, {3, NullType(), 7, 4, NullType()});
    CheckResult(mfunc, {3, NullType(), 2, 4, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsAdjustProjection) {
    std::map<std::string, std::string> options_map = {{"fields.f2,f4.sequence-group", "f1,f3"},
                                                      {"fields.f5,f6.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    std::vector<DataField> value_fields = {table_fields[1], table_fields[3], table_fields[7]};

    std::vector<DataField> expected_completed_value_fields = {
        table_fields[1], table_fields[3], table_fields[7], table_fields[2],
        table_fields[4], table_fields[5], table_fields[6]};

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {2, 6, 2, 2, 2, 2, 6});
    CheckResult(mfunc, {2, 6, 2, 2, 2, 2, 6});

    // update first sequence group
    Add(mfunc, {3, NullType(), 7, 4, NullType(), 1, 8});
    CheckResult(mfunc, {3, NullType(), 2, 4, NullType(), 2, 6});

    // update second sequence group
    Add(mfunc, {5, 3, 3, 3, 5, 5, 6});
    CheckResult(mfunc, {3, NullType(), 3, 4, NullType(), 5, 6});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjectionSequenceFieldsProject) {
    std::map<std::string, std::string> options_map = {{"fields.f4.sequence-group", "f1,f3"},
                                                      {"fields.f5.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    // the sequence field 'f4' is projected too
    std::vector<DataField> value_fields = {table_fields[1], table_fields[4], table_fields[3],
                                           table_fields[7]};

    std::vector<DataField> expected_completed_value_fields = {
        table_fields[1], table_fields[4], table_fields[3], table_fields[7], table_fields[5]};

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    // if sequence field is null, the related fields should not be updated
    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, {1, NullType(), 1, 2, 2});
    CheckResult(mfunc, {1, 1, 1, 2, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsAdjustProjectionProject) {
    std::map<std::string, std::string> options_map = {{"fields.f2,f4.sequence-group", "f1,f3"},
                                                      {"fields.f5,f6.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    // the sequence field 'f4' is projected too
    std::vector<DataField> value_fields = {table_fields[1], table_fields[4], table_fields[3],
                                           table_fields[7]};

    std::vector<DataField> expected_completed_value_fields = {
        table_fields[1], table_fields[4], table_fields[3], table_fields[7],
        table_fields[2], table_fields[5], table_fields[6]};

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, NullType(), 1, 3, 2, 2, 2});
    CheckResult(mfunc, {1, NullType(), 1, 3, 2, 2, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjectionAllFieldsProject) {
    std::map<std::string, std::string> options_map = {{"fields.f4.sequence-group", "f1,f3"},
                                                      {"fields.f5.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    // all fields are projected
    std::vector<DataField> value_fields = table_fields;
    std::vector<DataField> expected_completed_value_fields = table_fields;

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    // 'f6' has no sequence group, it should not be updated by null
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {4, 2, 4, 2, 2, 0, NullType(), 3});
    CheckResult(mfunc, {4, 2, 4, 2, 2, 1, 1, 1});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsAdjustProjectionAllFieldsProject) {
    std::map<std::string, std::string> options_map = {{"fields.f2,f4.sequence-group", "f1,f3"},
                                                      {"fields.f5,f6.sequence-group", "f7"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    // all fields are projected
    std::vector<DataField> value_fields = table_fields;
    std::vector<DataField> expected_completed_value_fields = table_fields;

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    // 'f6' has no sequence group, it should not be updated by null
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {4, 2, 4, 2, 2, 0, NullType(), 3});
    CheckResult(mfunc, {4, 2, 4, 2, 2, 1, 1, 1});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjectionNoSequenceGroup) {
    std::map<std::string, std::string> options_map = {};
    auto table_fields = CreateDataFields(/*value_arity=*/8);
    std::vector<DataField> value_fields = {table_fields[0], table_fields[1], table_fields[3],
                                           table_fields[4], table_fields[7]};
    std::vector<DataField> expected_completed_value_fields = value_fields;
    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    // Without sequence group, all the fields should not be updated by null
    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, {3, 3, NullType(), 3, 3});
    CheckResult(mfunc, {3, 3, 1, 3, 3});
    Add(mfunc, {2, 2, 2, 2, 2});
    CheckResult(mfunc, {2, 2, 2, 2, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjectionCreateDirectly) {
    std::map<std::string, std::string> options_map = {{"fields.f4.sequence-group", "f1,f3"},
                                                      {"fields.f5.sequence-group", "f7"}};
    auto table_fields = CreateDataFields(/*value_arity=*/8);
    std::vector<DataField> value_fields = {table_fields[1], table_fields[7]};
    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(value_fields);
    ASSERT_OK_AND_ASSIGN(auto options, CoreOptions::FromMap(options_map));

    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;

    ASSERT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
        options, &value_field_to_seq_group_field, &seq_group_key_set));
    ASSERT_NOK_WITH_MSG(
        PartialUpdateMergeFunction::Create(value_schema, {"f0"}, options,
                                           value_field_to_seq_group_field, seq_group_key_set),
        "cannot find sequence group field f4 in value schema, unexpected.");
}

TEST_F(PartialUpdateMergeFunctionTest, TestFirstValue) {
    std::map<std::string, std::string> options = {{"fields.f1.sequence-group", "f2,f3"},
                                                  {"fields.f2.aggregate-function", "first_value"},
                                                  {"fields.f3.aggregate-function", "last_value"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/4, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2});
    CheckResult(mfunc, {1, 2, 1, 2});
    Add(mfunc, {1, 0, 3, 3});
    CheckResult(mfunc, {1, 2, 3, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsFirstValue) {
    std::map<std::string, std::string> options = {{"fields.f1,f2.sequence-group", "f3,f4"},
                                                  {"fields.f3.aggregate-function", "first_value"},
                                                  {"fields.f4.aggregate-function", "last_value"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/5, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2, 2});
    CheckResult(mfunc, {1, 2, 2, 1, 2});
    Add(mfunc, {1, 0, 1, 3, 3});
    CheckResult(mfunc, {1, 2, 2, 3, 2});
}

TEST_F(PartialUpdateMergeFunctionTest, TestPartialUpdateWithAggregation) {
    std::map<std::string, std::string> options = {
        {"fields.f1.sequence-group", "f2,f3,f4"},
        {"fields.f7.sequence-group", "f6"},
        {"fields.f0.aggregate-function", "listagg"},
        {"fields.f2.aggregate-function", "sum"},
        {"fields.f4.aggregate-function", "last_value"},
        {"fields.f6.aggregate-function", "last_non_null_value"},
        {"fields.f4.ignore-retract", "true"},
        {"fields.f6.ignore-retract", "true"}};

    auto mfunc = CreateMergeFunction(/*value_arity=*/8, options);
    mfunc->Reset();

    // f0 pk
    // f1 sequence group
    // f2 in f1 group with sum agg
    // f3 in f1 group without agg
    // f4 in f1 group with last_value agg
    // f5 not in group
    // f6 in f7 group with last_not_null agg
    // f7 sequence group 2
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 2, 1, 2, 2, 2, 2, 0});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 2, 1, 1});

    // g_1, g_2 not advanced
    Add(mfunc, {1, 1, 1, 1, 1, 1, 2, 0});
    CheckResult(mfunc, {1, 2, 3, 2, 2, 1, 1, 1});
    Add(mfunc, {1, 1, -1, 1, 1, 2, 2, 0});
    CheckResult(mfunc, {1, 2, 2, 2, 2, 2, 1, 1});

    // test null
    Add(mfunc, {1, 3, NullType(), NullType(), NullType(), NullType(), NullType(), 2});
    CheckResult(mfunc, {1, 3, 2, NullType(), NullType(), 2, 1, 2});

    // test retract
    Add(mfunc, {1, 3, 1, 1, 1, 1, 1, 3});
    CheckResult(mfunc, {1, 3, 3, 1, 1, 1, 1, 3});
    Add(mfunc, RowKind::UpdateBefore(), {1, 3, 2, 1, 1, 1, 1, 3});
    CheckResult(mfunc, {1, 3, 1, NullType(), 1, 1, 1, 3});
    Add(mfunc, RowKind::Delete(), {1, 3, 2, 1, 1, 1, 1, 3});
    CheckResult(mfunc, {1, 3, -1, NullType(), 1, 1, 1, 3});
    // retract for old sequence
    Add(mfunc, RowKind::Delete(), {1, 2, 2, 1, 1, 1, 1, 3});
    CheckResult(mfunc, {1, 3, -3, NullType(), 1, 1, 1, 3});
}

TEST_F(PartialUpdateMergeFunctionTest, TestMultiSequenceFieldsPartialUpdateWithAggregation) {
    std::map<std::string, std::string> options = {
        {"fields.f1,f2.sequence-group", "f3,f4,f5"},
        {"fields.f7,f8.sequence-group", "f6"},
        {"fields.f0.aggregate-function", "listagg"},
        {"fields.f3.aggregate-function", "sum"},
        {"fields.f4.aggregate-function", "first_value"},
        {"fields.f5.aggregate-function", "last_value"},
        {"fields.f6.aggregate-function", "last_non_null_value"},
        {"fields.f4.ignore-retract", "true"},
        {"fields.f6.ignore-retract", "true"}};

    auto mfunc = CreateMergeFunction(/*value_arity=*/9, options);
    mfunc->Reset();

    // f0 pk
    // f1, f2 sequence group 1
    // f3 in f1, f2 group with sum agg
    // f4 in f1, f2 group with first_value agg
    // f5 in f1, f2 group with last_value agg
    // f6 in f7, f8 group with last_not_null agg
    // f7, f8 sequence group 2

    // test null retract
    Add(mfunc, {1, NullType(), NullType(), 1, 1, 1, 1, 1, 1});
    CheckResult(mfunc, {1, NullType(), NullType(), NullType(), NullType(), NullType(), 1, 1, 1});

    Add(mfunc, RowKind::Delete(), {1, NullType(), NullType(), 1, 1, 1, 0, 1, 1});
    CheckResult(mfunc, {1, NullType(), NullType(), NullType(), NullType(), NullType(), 1, 1, 1});

    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    CheckResult(mfunc, {1, 1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {1, 1, 2, 1, 2, 2, NullType(), 2, 0});
    CheckResult(mfunc, {1, 1, 2, 2, 1, 2, 1, 2, 0});

    // sequence group not advanced
    Add(mfunc, {1, 1, 1, 1, 3, 1, 1, 2, 0});
    CheckResult(mfunc, {1, 1, 2, 3, 3, 2, 1, 2, 0});

    // test null
    Add(mfunc, {1, 1, 3, NullType(), NullType(), NullType(), NullType(), 4, 2});
    CheckResult(mfunc, {1, 1, 3, 3, 3, NullType(), 1, 4, 2});

    // test retract
    Add(mfunc, {1, 2, 3, 1, 1, 1, 1, 4, 3});
    CheckResult(mfunc, {1, 2, 3, 4, 3, 1, 1, 4, 3});
    Add(mfunc, RowKind::UpdateBefore(), {1, 2, 3, 2, 1, 2, 1, 4, 3});
    CheckResult(mfunc, {1, 2, 3, 2, 3, NullType(), 1, 4, 3});
    Add(mfunc, RowKind::Delete(), {1, 3, 2, 3, 1, 1, 4, 3, 1});
    CheckResult(mfunc, {1, 3, 2, -1, 3, NullType(), 1, 4, 3});
    // retract for old sequence
    Add(mfunc, RowKind::Delete(), {1, 2, 2, 2, 1, 1, 1, 1, 3});
    CheckResult(mfunc, {1, 3, 2, -3, 3, NullType(), 1, 4, 3});
}

TEST_F(PartialUpdateMergeFunctionTest, TestPartialUpdateWithAggregationProjectPushDown) {
    std::map<std::string, std::string> options_map = {
        {"fields.f1.sequence-group", "f2,f3,f4"},
        {"fields.f7.sequence-group", "f6"},
        {"fields.f0.aggregate-function", "listagg"},
        {"fields.f2.aggregate-function", "sum"},
        {"fields.f4.aggregate-function", "last_value"},
        {"fields.f6.aggregate-function", "last_non_null_value"},
        {"fields.f4.ignore-retract", "true"},
        {"fields.f6.ignore-retract", "true"}};

    auto table_fields = CreateDataFields(/*value_arity=*/8);
    std::vector<DataField> value_fields = {table_fields[3], table_fields[2], table_fields[5]};

    std::vector<DataField> expected_completed_value_fields = {table_fields[3], table_fields[2],
                                                              table_fields[5], table_fields[1]};
    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();

    // f0 pk
    // f1 sequence group
    // f2 in f1 group with sum agg
    // f3 in f1 group without agg
    // f4 in f1 group with last_value agg
    // f5 not in group
    // f6 in f7 group with last_not_null agg
    // f7 sequence group 2
    Add(mfunc, {1, 1, 1, 1});
    Add(mfunc, {2, 1, 2, 2});
    CheckResult(mfunc, {2, 2, 2, 2});

    Add(mfunc, RowKind::Insert(), {NullType(), NullType(), NullType(), 3});
    CheckResult(mfunc, {NullType(), 2, 2, 3});

    // test retract
    Add(mfunc, RowKind::UpdateBefore(), {1, 2, 1, 3});
    CheckResult(mfunc, {NullType(), 0, 2, 3});
    Add(mfunc, RowKind::Delete(), {1, 2, 1, 3});
    CheckResult(mfunc, {NullType(), -2, 2, 3});
}

TEST_F(PartialUpdateMergeFunctionTest,
       TestMultiSequenceFieldsPartialUpdateWithAggregationProjectPushDown) {
    std::map<std::string, std::string> options_map = {
        {"fields.f1,f8.sequence-group", "f2,f3,f4"},
        {"fields.f7,f9.sequence-group", "f6"},
        {"fields.f0.aggregate-function", "listagg"},
        {"fields.f2.aggregate-function", "sum"},
        {"fields.f4.aggregate-function", "last_value"},
        {"fields.f6.aggregate-function", "last_non_null_value"},
        {"fields.f4.ignore-retract", "true"},
        {"fields.f6.ignore-retract", "true"}};

    auto table_fields = CreateDataFields(/*value_arity=*/10);
    std::vector<DataField> value_fields = {table_fields[3], table_fields[2], table_fields[5]};

    std::vector<DataField> expected_completed_value_fields = {
        table_fields[3], table_fields[2], table_fields[5], table_fields[1], table_fields[8]};
    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();

    // f0 pk
    // f1, f8 sequence group
    // f2 in f1, f8 group with sum agg
    // f3 in f1, f8 group without agg
    // f4 in f1, f8 group with last_value agg
    // f5 not in group
    // f6 in f7, f9 group with last_not_null agg
    // f7, f9 sequence group 2
    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, {2, 1, 2, 2, 2});
    CheckResult(mfunc, {2, 2, 2, 2, 2});

    Add(mfunc, RowKind::Insert(), {NullType(), NullType(), NullType(), 3, 3});
    CheckResult(mfunc, {NullType(), 2, 2, 3, 3});

    // test retract
    Add(mfunc, RowKind::UpdateBefore(), {1, 2, 1, 3, 3});
    CheckResult(mfunc, {NullType(), 0, 2, 3, 3});
    Add(mfunc, RowKind::Delete(), {1, 2, 1, 3, 3});
    CheckResult(mfunc, {NullType(), -2, 2, 3, 3});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAggregationWithoutSequenceGroup) {
    std::map<std::string, std::string> options = {{"fields.f1.aggregate-function", "sum"}};
    auto error_msg = CreateMergeFunctionWithInvalidOptions(/*value_arity=*/2, options);
    ASSERT_TRUE(
        error_msg.find(
            "Must use sequence group for aggregation functions but not found for field f1") !=
        std::string::npos)
        << error_msg;
}

TEST_F(PartialUpdateMergeFunctionTest, TestConflictsInDeleteConfig) {
    {
        std::map<std::string, std::string> options = {
            {"ignore-delete", "true"}, {"partial-update.remove-record-on-delete", "true"}};
        auto error_msg = CreateMergeFunctionWithInvalidOptions(/*value_arity=*/2, options);
        ASSERT_TRUE(
            error_msg.find("ignore-delete and partial-update.remove-record-on-delete have "
                           "conflicting behavior so should not be enabled at the same time.") !=
            std::string::npos)
            << error_msg;
    }
    {
        std::map<std::string, std::string> options = {
            {"fields.f1.sequence-group", "f2"}, {"partial-update.remove-record-on-delete", "true"}};
        auto error_msg = CreateMergeFunctionWithInvalidOptions(/*value_arity=*/3, options);
        ASSERT_TRUE(
            error_msg.find("sequence group and partial-update.remove-record-on-delete have "
                           "conflicting behavior so should not be enabled at the same time.") !=
            std::string::npos)
            << error_msg;
    }
    {
        std::map<std::string, std::string> options = {
            {"fields.f1.sequence-group", "f2"},
            {"partial-update.remove-record-on-sequence-group", "f2,f3"}};
        auto error_msg = CreateMergeFunctionWithInvalidOptions(/*value_arity=*/3, options);
        ASSERT_TRUE(
            error_msg.find("field f2,f3 defined in partial-update.remove-record-on-sequence-group "
                           "option must be part of sequence groups.") != std::string::npos)
            << error_msg;
    }
}

TEST_F(PartialUpdateMergeFunctionTest, TestInvalidDeleteRecord) {
    auto mfunc = CreateMergeFunction(/*value_arity=*/3, {});
    mfunc->Reset();
    std::shared_ptr<InternalRow> key = BinaryRowGenerator::GenerateRowPtr({1}, pool_.get());
    ASSERT_NOK_WITH_MSG(
        mfunc->Add(KeyValue(RowKind::Delete(), sequence_++, KeyValue::UNKNOWN_LEVEL, std::move(key),
                            BinaryRowGenerator::GenerateRowPtr({1, 2, 3}, pool_.get()))),
        "By default, Partial update can not accept delete records, "
        "you can choose one of the following solutions");
}

TEST_F(PartialUpdateMergeFunctionTest, TestRemoveRecordOnSequenceGroupNotRecalled) {
    // fields f0-f6, only recall f0-f3, f6 is not recalled therefore, remove config will be ignored
    std::map<std::string, std::string> options = {
        {"fields.f3.sequence-group", "f1,f2"},
        {"fields.f6.sequence-group", "f4,f5"},
        {"partial-update.remove-record-on-sequence-group", "f6"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/4, options);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1});
    Add(mfunc, {1, 2, 2, 2});
    CheckResult(mfunc, {1, 2, 2, 2});
    // delete
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 3});
    CheckResult(mfunc, {1, NullType(), NullType(), 3});
}

TEST_F(PartialUpdateMergeFunctionTest, TestCreateFieldAggregatorsWithDefaultAgg) {
    auto value_schema = arrow::schema(
        arrow::FieldVector({arrow::field("p0", arrow::int32()), arrow::field("f0", arrow::int32()),
                            arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::int32()),
                            arrow::field("f3", arrow::int32())}));

    ASSERT_OK_AND_ASSIGN(CoreOptions options,
                         CoreOptions::FromMap({
                             {Options::FIELDS_DEFAULT_AGG_FUNC, "sum"},
                             {"fields.f1.aggregate-function", "min"},
                             {"fields.f0.sequence-group", "f1,f2"},
                             {"fields.f3.aggregate-function", "last_non_null_value"},
                         }));
    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;
    ASSERT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
        options, &value_field_to_seq_group_field, &seq_group_key_set));
    std::map<int32_t, std::shared_ptr<FieldAggregator>> aggs;
    ASSERT_OK_AND_ASSIGN(aggs, PartialUpdateMergeFunction::CreateFieldAggregators(
                                   value_schema,
                                   /*primary_keys=*/{"p0"}, options, value_field_to_seq_group_field,
                                   seq_group_key_set));
    ASSERT_EQ(4, aggs.size());
    // test primary key: p0
    ASSERT_TRUE(dynamic_cast<FieldPrimaryKeyAgg*>(aggs[0].get()));
    // test sequence group field without agg: f0
    ASSERT_TRUE(aggs.find(1) == aggs.end());
    // test specify agg: f1
    ASSERT_TRUE(dynamic_cast<FieldMinAgg*>(aggs[2].get()));
    // test default agg: f2
    ASSERT_TRUE(dynamic_cast<FieldSumAgg*>(aggs[3].get()));
    // test last_non_null_value agg without sequence group: f3
    ASSERT_TRUE(dynamic_cast<FieldLastNonNullValueAgg*>(aggs[4].get()));
}

TEST_F(PartialUpdateMergeFunctionTest, TestCreateFieldAggregatorsWithoutDefaultAgg) {
    auto value_schema = arrow::schema(arrow::FieldVector(
        {arrow::field("p0", arrow::int32()), arrow::field("f0", arrow::int32()),
         arrow::field("f1", arrow::int32()), arrow::field("f2", arrow::int32())}));

    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap({
                                                  {"fields.f1.aggregate-function", "min"},
                                                  {"fields.f0.sequence-group", "f1,f2"},
                                              }));
    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;
    ASSERT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
        options, &value_field_to_seq_group_field, &seq_group_key_set));
    std::map<int32_t, std::shared_ptr<FieldAggregator>> aggs;
    ASSERT_OK_AND_ASSIGN(aggs, PartialUpdateMergeFunction::CreateFieldAggregators(
                                   value_schema,
                                   /*primary_keys=*/{"p0"}, options, value_field_to_seq_group_field,
                                   seq_group_key_set));
    ASSERT_EQ(2, aggs.size());
    // test primary key: p0
    ASSERT_TRUE(dynamic_cast<FieldPrimaryKeyAgg*>(aggs[0].get()));
    // test sequence group field without agg: f0
    ASSERT_TRUE(aggs.find(1) == aggs.end());
    // test specify agg: f1
    ASSERT_TRUE(dynamic_cast<FieldMinAgg*>(aggs[2].get()));
    // test no agg: f2
    ASSERT_TRUE(aggs.find(3) == aggs.end());
}

TEST_F(PartialUpdateMergeFunctionTest, TestSequenceGroupPartialDeleteWithProjection) {
    std::map<std::string, std::string> options_map = {
        {"fields.f3.sequence-group", "f1,f2"},
        {"fields.f6.sequence-group", "f4,f5"},
        {"partial-update.remove-record-on-sequence-group", "f3,f6"}};

    auto table_fields = CreateDataFields(/*value_arity=*/7);
    // Reordered fields: f3, f6, f0, f1, f2, f4, f5
    std::vector<DataField> value_fields = {table_fields[3], table_fields[6], table_fields[0],
                                           table_fields[1], table_fields[2], table_fields[4],
                                           table_fields[5]};
    std::vector<DataField> expected_completed_value_fields = value_fields;

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    // Reordered: f3=11, f6=22, f0=100, f1=200, f2=1, f4=12, f5=21
    Add(mfunc, {11, 22, 100, 200, 1, 12, 21});
    Add(mfunc, RowKind::Delete(), {11, 22, 100, 200, 1, 12, 21});
    CheckResult(mfunc, {11, 22, 100, 200, 1, 12, 21});
}

TEST_F(PartialUpdateMergeFunctionTest, TestAdjustProjectionNonProject) {
    std::map<std::string, std::string> options_map = {{"fields.f4.sequence-group", "f1,f3"},
                                                      {"fields.f5.sequence-group", "f7"}};
    auto table_fields = CreateDataFields(/*value_arity=*/8);
    // Non-projection: use all fields
    std::vector<DataField> value_fields = table_fields;
    std::vector<DataField> expected_completed_value_fields = table_fields;

    auto mfunc = CreateMergeFunctionWithProjection(table_fields, value_fields, options_map,
                                                   expected_completed_value_fields);
    mfunc->Reset();
    Add(mfunc, {1, 1, 1, 1, 1, 1, 1, 1});
    Add(mfunc, {4, 2, 4, 2, 2, 0, NullType(), 3});
    CheckResult(mfunc, {4, 2, 4, 2, 2, 1, 1, 1});
}

TEST_F(PartialUpdateMergeFunctionTest, TestDeleteReproduceCorrectSequenceNumber) {
    std::map<std::string, std::string> options_map = {
        {"partial-update.remove-record-on-delete", "true"}};
    auto mfunc = CreateMergeFunction(/*value_arity=*/5, options_map);
    mfunc->Reset();

    Add(mfunc, {1, 1, 1, 1, 1});
    Add(mfunc, RowKind::Delete(), {1, 1, 1, 1, 1});

    ASSERT_OK_AND_ASSIGN(auto result, mfunc->GetResult());
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->sequence_number, 1);
}

TEST_F(PartialUpdateMergeFunctionTest, TestInitRowWithNullableFieldOnDelete) {
    std::map<std::string, std::string> options_map = {
        {"partial-update.remove-record-on-delete", "true"}};
    // f0 and f1 are not nullable, f2 and f3 are nullable
    std::vector<DataField> data_fields = {
        DataField(0, arrow::field("f0", arrow::int32(), /*nullable=*/false)),
        DataField(1, arrow::field("f1", arrow::int32(), /*nullable=*/false)),
        DataField(2, arrow::field("f2", arrow::int32(), /*nullable=*/true)),
        DataField(3, arrow::field("f3", arrow::int32(), /*nullable=*/true))};
    auto value_schema = DataField::ConvertDataFieldsToArrowSchema(data_fields);
    ASSERT_OK_AND_ASSIGN(CoreOptions options, CoreOptions::FromMap(options_map));
    std::map<std::string, std::vector<std::string>> value_field_to_seq_group_field;
    std::set<std::string> seq_group_key_set;
    ASSERT_OK(PartialUpdateMergeFunction::ParseSequenceGroupFields(
        options, &value_field_to_seq_group_field, &seq_group_key_set));
    ASSERT_OK_AND_ASSIGN(auto mfunc, PartialUpdateMergeFunction::Create(
                                         value_schema, /*primary_keys=*/{"f0"}, options,
                                         value_field_to_seq_group_field, seq_group_key_set));
    mfunc->Reset();

    // insert some data first
    Add(mfunc, {1, 3, 5, 7});
    // send a DELETE with nullable field as null, triggers initRow
    Add(mfunc, RowKind::Delete(), {1, 2, 2, NullType()});
    // after delete with removeRecordOnDelete, row is re-initialized via initRow
    CheckResult(mfunc, {1, 2, 2, NullType()});
}
}  // namespace paimon::test
