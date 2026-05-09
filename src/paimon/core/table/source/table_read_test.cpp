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

#include "paimon/table/source/table_read.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "paimon/core/core_options.h"
#include "paimon/core/operation/abstract_split_read.h"
#include "paimon/core/operation/split_read.h"
#include "paimon/core/table/source/append_only_table_read.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/defs.h"
#include "paimon/predicate/literal.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/read_context.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(TableReadTest, TestReadWithInvalidContext) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_10.db/append_10/";
    {
        // read with non-exist field
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f0", "f1", "non-exist"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "Get field non-exist failed: not exist in table schema");
    }
    {
        // field type and literal type mismatch
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(15l));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(std::move(read_context)),
            "field f3 has field type BIGINT in literal, mismatch field type DOUBLE in predicate");
    }
    {
        // field type in predicate mismatch schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::BIGINT, Literal(15l));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "schema type double mismatches predicate field type BIGINT");
    }
    {
        // field idx in predicate mismatch in schema
        auto predicate = PredicateBuilder::Equal(/*field_index=*/2, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(15.0));
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f0", "f1"});
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(
            TableRead::Create(std::move(read_context)),
            "field f3 has field idx 0 in input schema, mismatch field idx 2 in predicate");
    }
    {
        // literal cannot be null
        auto predicate = PredicateBuilder::Equal(/*field_index=*/3, /*field_name=*/"f3",
                                                 FieldType::DOUBLE, Literal(FieldType::DOUBLE));
        ReadContextBuilder context_builder(path);
        context_builder.SetPredicate(predicate);
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "literal cannot be null in predicate, field name f3");
    }
    {
        // schema with duplicate field f3
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f1", "f3"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                            "validate schema failed: read schema has duplicate field f3");
    }
}

TEST(TableReadTest, TestReadWithSpecifiedInvalidSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"field_no_exist"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_NOK_WITH_MSG(TableRead::Create(std::move(read_context)),
                        "Get field field_no_exist failed: not exist in table schema");
}

TEST(TableReadTest, TestCreateKeyValueTableRead) {
    std::string path = paimon::test::GetDataDir() +
                       "/orc/pk_table_with_dv_cardinality.db/pk_table_with_dv_cardinality/";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto key_value_table_read = dynamic_cast<KeyValueTableRead*>(table_read.get());
    ASSERT_TRUE(key_value_table_read);
}

TEST(TableReadTest, TestCreateAppendOnlyTableRead) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
}

TEST(TableReadTest, TestMergeOptions) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f0", "f1", "f2", "f3"});
    context_builder.AddOption("read.batch-size", "2");
    context_builder.AddOption("orc.read.enable-lazy-decoding", "true");
    context_builder.AddOption("bucket", "10");
    context_builder.AddOption("bucket-key", "f3");
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    ASSERT_OK_AND_ASSIGN(auto table_read, TableRead::Create(std::move(read_context)));
    auto append_only_table_read = dynamic_cast<AppendOnlyTableRead*>(table_read.get());
    ASSERT_TRUE(append_only_table_read);
    auto* typed_split_read =
        dynamic_cast<AbstractSplitRead*>(append_only_table_read->split_reads_[0].get());
    ASSERT_TRUE(typed_split_read);
    const auto& core_options = typed_split_read->options_;
    std::map<std::string, std::string> expected_options = {
        {"read.batch-size", "2"},   {"orc.read.enable-lazy-decoding", "true"},
        {"bucket", "10"},           {"bucket-key", "f3"},
        {"manifest.format", "orc"}, {"file.format", "orc"}};
    ASSERT_EQ(expected_options, core_options.ToMap());
}

}  // namespace paimon::test
