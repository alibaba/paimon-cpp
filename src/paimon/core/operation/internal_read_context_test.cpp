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

#include "paimon/core/operation/internal_read_context.h"

#include <utility>

#include "arrow/type.h"
#include "gtest/gtest.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/schema_manager.h"
#include "paimon/defs.h"
#include "paimon/fs/local/local_file_system.h"
#include "paimon/predicate/compound_predicate.h"
#include "paimon/predicate/leaf_predicate.h"
#include "paimon/predicate/predicate_builder.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::test {
TEST(InternalReadContext, TestReadWithUnspecifiedSchema) {
    // no read schema is specified, read all fields
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(0, arrow::field("f0", arrow::utf8())),
                                          DataField(1, arrow::field("f1", arrow::int32())),
                                          DataField(2, arrow::field("f2", arrow::int32())),
                                          DataField(3, arrow::field("f3", arrow::float64()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldId) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithSpecifiedFieldIdAndSchema) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    // read schema is specified, read fields in schema
    // will use field ids instead of field names.
    context_builder.SetReadSchema({"f0"});
    context_builder.SetReadFieldIds({3, 0});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithRowTrackingAndScoreFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
    {
        // test invalid case: disable row tracking while read row tracking fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f0", "_ROW_ID", "_SEQUENCE_NUMBER"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _ROW_ID failed: not exist in table schema");
    }
    {
        // test invalid case: disable data evolution while read score fields
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        context_builder.SetReadSchema({"f3", "f0", "_INDEX_SCORE"});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        ASSERT_NOK_WITH_MSG(InternalReadContext::Create(std::move(read_context), table_schema,
                                                        table_schema->Options()),
                            "Get field _INDEX_SCORE failed: not exist in table schema");
    }
}

TEST(InternalReadContext, TestReadWithValueKindField) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "_VALUE_KIND", "f0"});
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    std::vector<DataField> read_fields = {DataField(3, arrow::field("f3", arrow::float64())),
                                          SpecialFields::ValueKind(),
                                          DataField(0, arrow::field("f0", arrow::utf8()))};
    auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
    ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
}

TEST(InternalReadContext, TestReadWithFieldIdsAndSpecialFields) {
    {
        // test simple
        std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
        ReadContextBuilder context_builder(path);
        // here we use field ids instead of field names, and specify special ids for row id,
        // sequence number and index score.
        context_builder.SetReadFieldIds({3, 0, SpecialFieldIds::ROW_ID,
                                         SpecialFieldIds::SEQUENCE_NUMBER,
                                         SpecialFieldIds::INDEX_SCORE});
        ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
        SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
        ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
        auto new_options = table_schema->Options();
        new_options[Options::ROW_TRACKING_ENABLED] = "true";
        new_options[Options::DATA_EVOLUTION_ENABLED] = "true";
        ASSERT_OK_AND_ASSIGN(
            auto internal_context,
            InternalReadContext::Create(std::move(read_context), table_schema, new_options));
        std::vector<DataField> read_fields = {
            DataField(3, arrow::field("f3", arrow::float64())),
            DataField(0, arrow::field("f0", arrow::utf8())), SpecialFields::RowId(),
            SpecialFields::SequenceNumber(), SpecialFields::IndexScore()};
        auto expected_schema = DataField::ConvertDataFieldsToArrowSchema(read_fields);
        ASSERT_TRUE(internal_context->GetReadSchema()->Equals(expected_schema));
    }
}

// Upstream predicates are constructed against the latest table schema. When the query
// projects a subset of columns, the read schema built inside InternalReadContext::Create
// lays those columns out at different positions and the original field_index no longer
// matches. The remapping path in Create() must rewrite each leaf predicate so its
// field_index points to the column's position in the projected read schema.
TEST(InternalReadContext, TestPredicateFieldIdxRemappedWhenProjected) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    // read_schema lays out (f3, f0) — f3 ends up at position 0, f0 at position 1, while
    // the latest table schema has them at 3 and 0 respectively.
    context_builder.SetReadSchema({"f3", "f0"});
    // Predicate was constructed against the latest schema, so f3 carries field_index 3.
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/3, "f3", FieldType::DOUBLE, Literal(1.5));
    context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    auto leaf = std::dynamic_pointer_cast<LeafPredicate>(internal_context->GetPredicate());
    ASSERT_NE(leaf, nullptr);
    ASSERT_EQ(leaf->FieldName(), "f3");
    // f3 is the first column in the projected read schema.
    ASSERT_EQ(leaf->FieldIndex(), 0);
}

// When the predicate already aligns with the read schema, remapping should be a no-op
// and return the original shared_ptr without reconstructing the leaf.
TEST(InternalReadContext, TestPredicateUnchangedWhenAligned) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    // No projection -> read schema matches the latest schema: f0(0), f1(1), f2(2), f3(3).
    auto predicate =
        PredicateBuilder::Equal(/*field_index=*/3, "f3", FieldType::DOUBLE, Literal(1.5));
    context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    // shared_ptr equality: remap returned the input unchanged.
    ASSERT_EQ(predicate.get(), internal_context->GetPredicate().get());
}

// CompoundPredicate is recursively remapped: every nested leaf must point to the
// column's position in the projected read schema.
TEST(InternalReadContext, TestCompoundPredicateRemap) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0"});
    // AND(f3 == 1.5, f0 == "x") with field indices from the latest schema.
    auto left = PredicateBuilder::Equal(/*field_index=*/3, "f3", FieldType::DOUBLE, Literal(1.5));
    auto right = PredicateBuilder::Equal(/*field_index=*/0, "f0", FieldType::STRING,
                                         Literal(FieldType::STRING, "x", 1));
    ASSERT_OK_AND_ASSIGN(auto compound, PredicateBuilder::And({left, right}));
    context_builder.SetPredicate(compound);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    auto remapped_compound =
        std::dynamic_pointer_cast<CompoundPredicate>(internal_context->GetPredicate());
    ASSERT_NE(remapped_compound, nullptr);
    ASSERT_EQ(remapped_compound->Children().size(), 2);
    auto remapped_left = std::dynamic_pointer_cast<LeafPredicate>(remapped_compound->Children()[0]);
    auto remapped_right =
        std::dynamic_pointer_cast<LeafPredicate>(remapped_compound->Children()[1]);
    ASSERT_NE(remapped_left, nullptr);
    ASSERT_NE(remapped_right, nullptr);
    ASSERT_EQ(remapped_left->FieldName(), "f3");
    ASSERT_EQ(remapped_left->FieldIndex(), 0);
    ASSERT_EQ(remapped_right->FieldName(), "f0");
    ASSERT_EQ(remapped_right->FieldIndex(), 1);
}

// A leaf whose field is not in the projected read schema is dropped silently
// (inclusive projection — same as paimon Java's PredicateProjectionConverter).
// The caller's request still succeeds; the predicate is simply unavailable for
// pushdown at this read.
TEST(InternalReadContext, TestPredicateOnFieldOutsideProjectionDropped) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0"});
    auto predicate = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(7));
    context_builder.SetPredicate(predicate);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    ASSERT_EQ(internal_context->GetPredicate(), nullptr);
}

// AND is inclusive: a child whose field is not in the read schema is dropped, the rest
// of the AND survives. Verifies the same semantic paimon Java applies in
// PredicateProjectionConverter (necessary, not sufficient — projection is a superset).
TEST(InternalReadContext, TestAndChildOutsideProjectionDropped) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0"});
    // AND(f3 == 1.5, f1 == 7): f1 is outside the projection; only f3 should survive.
    auto projectable =
        PredicateBuilder::Equal(/*field_index=*/3, "f3", FieldType::DOUBLE, Literal(1.5));
    auto dropped = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(7));
    ASSERT_OK_AND_ASSIGN(auto compound, PredicateBuilder::And({projectable, dropped}));
    context_builder.SetPredicate(compound);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    auto surviving_leaf =
        std::dynamic_pointer_cast<LeafPredicate>(internal_context->GetPredicate());
    ASSERT_NE(surviving_leaf, nullptr);
    ASSERT_EQ(surviving_leaf->FieldName(), "f3");
    ASSERT_EQ(surviving_leaf->FieldIndex(), 0);
}

// OR is strict: if any branch is not projectable, the whole OR is dropped, otherwise
// rows that only satisfied the dropped branch would falsely pass through the projected
// predicate. Verifies the strict half of paimon Java's PredicateProjectionConverter.
TEST(InternalReadContext, TestOrChildOutsideProjectionDropsWholePredicate) {
    std::string path = paimon::test::GetDataDir() + "/orc/append_09.db/append_09";
    ReadContextBuilder context_builder(path);
    context_builder.SetReadSchema({"f3", "f0"});
    auto projectable =
        PredicateBuilder::Equal(/*field_index=*/3, "f3", FieldType::DOUBLE, Literal(1.5));
    auto dropped = PredicateBuilder::Equal(/*field_index=*/1, "f1", FieldType::INT, Literal(7));
    ASSERT_OK_AND_ASSIGN(auto compound, PredicateBuilder::Or({projectable, dropped}));
    context_builder.SetPredicate(compound);
    ASSERT_OK_AND_ASSIGN(auto read_context, context_builder.Finish());
    SchemaManager schema_manager(std::make_shared<LocalFileSystem>(), read_context->GetPath());
    ASSERT_OK_AND_ASSIGN(auto table_schema, schema_manager.ReadSchema(0));
    ASSERT_OK_AND_ASSIGN(auto internal_context,
                         InternalReadContext::Create(std::move(read_context), table_schema,
                                                     table_schema->Options()));
    ASSERT_EQ(internal_context->GetPredicate(), nullptr);
}

}  // namespace paimon::test
