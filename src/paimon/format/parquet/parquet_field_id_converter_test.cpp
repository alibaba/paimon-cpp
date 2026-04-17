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

#include "paimon/format/parquet/parquet_field_id_converter.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "gtest/gtest.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/status.h"
#include "paimon/testing/utils/testharness.h"

namespace paimon::parquet::test {

class ParquetFieldIdConverterTest : public ::testing::Test {
 public:
    class FieldInfo {
     public:
        std::string field_name;
        arrow::Type::type field_type;
        std::string field_id;

        std::string ToString() const {
            return field_name + " " + arrow::internal::ToString(field_type) + " " + field_id;
        }

        bool operator==(const FieldInfo& rhs) const {
            return field_name == rhs.field_name && field_type == rhs.field_type &&
                   field_id == rhs.field_id;
        }
    };

    void SetUp() override {
        print_ = false;
    }

    void PrintFieldMetadata(const std::shared_ptr<arrow::Schema>& schema,
                            ParquetFieldIdConverter::IdConvertType convert_type,
                            std::vector<FieldInfo>* field_infos) const {
        for (const auto& field : schema->fields()) {
            PrintFieldMetadataRecursive(field, /*indent=*/2, convert_type, field_infos);
        }
    }

    void PrintFieldMetadataRecursive(const std::shared_ptr<arrow::Field>& field, int32_t indent,
                                     ParquetFieldIdConverter::IdConvertType convert_type,
                                     std::vector<FieldInfo>* field_infos) const {
        std::string prefix(indent * 2, ' ');
        if (print_) {
            std::cout << prefix << "Field: " << field->name() << " (" << field->type()->ToString()
                      << ")" << std::endl;
        }

        if (field->HasMetadata() && field->metadata()) {
            auto meta = field->metadata();
            std::string field_id;
            if (convert_type == ParquetFieldIdConverter::IdConvertType::PAIMON_TO_PARQUET_ID) {
                field_id =
                    field->metadata()->Get(ParquetFieldIdConverter::PARQUET_FIELD_ID).ValueOrDie();
            } else {
                field_id = field->metadata()->Get(DataField::FIELD_ID).ValueOrDie();
            }
            field_infos->push_back({field->name(), field->type()->id(), field_id});
            if (print_) {
                for (int32_t i = 0; i < meta->size(); ++i) {
                    std::cout << prefix << "  [meta] " << meta->key(i) << " = " << meta->value(i)
                              << std::endl;
                }
            }
        }

        auto type_id = field->type()->id();
        if (type_id == arrow::Type::STRUCT) {
            auto struct_type = std::static_pointer_cast<arrow::StructType>(field->type());
            for (const auto& child : struct_type->fields()) {
                PrintFieldMetadataRecursive(child, indent + 1, convert_type, field_infos);
            }
        } else if (type_id == arrow::Type::LIST) {
            auto list_type = std::static_pointer_cast<arrow::ListType>(field->type());
            PrintFieldMetadataRecursive(list_type->value_field(), indent + 1, convert_type,
                                        field_infos);
        } else if (type_id == arrow::Type::MAP) {
            auto map_type = std::static_pointer_cast<arrow::MapType>(field->type());
            PrintFieldMetadataRecursive(map_type->key_field(), indent + 1, convert_type,
                                        field_infos);
            PrintFieldMetadataRecursive(map_type->item_field(), indent + 1, convert_type,
                                        field_infos);
        }
    }

 private:
    bool print_ = false;
};

TEST_F(ParquetFieldIdConverterTest, TestSimple) {
    arrow::FieldVector fields = {
        arrow::field("f1", arrow::boolean()),
        arrow::field("f2", arrow::int8()),
        arrow::field("f3", arrow::int16()),
        arrow::field("f4", arrow::int32()),
        arrow::field("f5", arrow::int64()),
        arrow::field("f6", arrow::float32()),
        arrow::field("f7", arrow::float64()),
        arrow::field("f8", arrow::utf8()),
        arrow::field("f9", arrow::binary()),
        arrow::field("f10", arrow::map(arrow::list(arrow::float32()),
                                       arrow::struct_({arrow::field("k0", arrow::boolean()),
                                                       arrow::field("v0", arrow::int64())}))),
        arrow::field("f11", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("f12", arrow::date32()),
        arrow::field("f13", arrow::decimal128(2, 2))};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                            /*options=*/{}));
    auto schema_with_field_id = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(auto new_schema,
                         ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(schema_with_field_id));
    // convert to PARQUET:field_id
    std::vector<FieldInfo> field_infos;
    PrintFieldMetadata(new_schema, ParquetFieldIdConverter::IdConvertType::PAIMON_TO_PARQUET_ID,
                       &field_infos);
    std::vector<FieldInfo> expected_field_infos = {
        {"f1", arrow::Type::BOOL, "0"},        {"f2", arrow::Type::INT8, "1"},
        {"f3", arrow::Type::INT16, "2"},       {"f4", arrow::Type::INT32, "3"},
        {"f5", arrow::Type::INT64, "4"},       {"f6", arrow::Type::FLOAT, "5"},
        {"f7", arrow::Type::DOUBLE, "6"},      {"f8", arrow::Type::STRING, "7"},
        {"f9", arrow::Type::BINARY, "8"},      {"f10", arrow::Type::MAP, "9"},
        {"k0", arrow::Type::BOOL, "10"},       {"v0", arrow::Type::INT64, "11"},
        {"f11", arrow::Type::TIMESTAMP, "12"}, {"f12", arrow::Type::DATE32, "13"},
        {"f13", arrow::Type::DECIMAL128, "14"}};
    ASSERT_EQ(expected_field_infos, field_infos);
    // convert to paimon.id
    ASSERT_OK_AND_ASSIGN(auto old_schema,
                         ParquetFieldIdConverter::GetPaimonIdsFromParquetIds(new_schema));
    std::vector<FieldInfo> old_field_infos;
    PrintFieldMetadata(old_schema, ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID,
                       &old_field_infos);
    ASSERT_EQ(expected_field_infos, old_field_infos);
}

TEST_F(ParquetFieldIdConverterTest, TestNestedType) {
    arrow::FieldVector fields = {
        arrow::field("f0",
                     arrow::struct_({arrow::field("sub1", arrow::date32()),
                                     arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::NANO)),
                                     arrow::field("sub3", arrow::decimal128(23, 5)),
                                     arrow::field("sub4", arrow::binary()),
                                     arrow::field("sub5", arrow::binary())})),
        arrow::field("f1", arrow::list(arrow::struct_(
                               {arrow::field("sub1", arrow::date32()),
                                arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::NANO)),
                                arrow::field("sub3", arrow::decimal128(23, 5)),
                                arrow::field("sub4", arrow::binary()),
                                arrow::field("sub5", arrow::binary())}))),
        arrow::field(
            "f2", arrow::map(
                      arrow::struct_({arrow::field("sub1", arrow::date32()),
                                      arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::NANO)),
                                      arrow::field("sub3", arrow::decimal128(23, 5)),
                                      arrow::field("sub4", arrow::binary()),
                                      arrow::field("sub5", arrow::binary())}),
                      arrow::struct_({arrow::field("sub1", arrow::date32()),
                                      arrow::field("sub2", arrow::timestamp(arrow::TimeUnit::NANO)),
                                      arrow::field("sub3", arrow::decimal128(23, 5)),
                                      arrow::field("sub4", arrow::binary()),
                                      arrow::field("sub5", arrow::binary())})))};
    auto schema = arrow::schema(fields);
    ASSERT_OK_AND_ASSIGN(
        auto table_schema,
        TableSchema::Create(/*schema_id=*/0, schema, /*partition_keys=*/{}, /*primary_keys=*/{},
                            /*options=*/{}));
    auto schema_with_field_id = DataField::ConvertDataFieldsToArrowSchema(table_schema->Fields());
    ASSERT_OK_AND_ASSIGN(auto new_schema,
                         ParquetFieldIdConverter::AddParquetIdsFromPaimonIds(schema_with_field_id));
    // convert to PARQUET:field_id
    std::vector<FieldInfo> field_infos;
    PrintFieldMetadata(new_schema, ParquetFieldIdConverter::IdConvertType::PAIMON_TO_PARQUET_ID,
                       &field_infos);
    std::vector<FieldInfo> expected_field_infos = {
        {"f0", arrow::Type::STRUCT, "0"},        {"sub1", arrow::Type::DATE32, "1"},
        {"sub2", arrow::Type::TIMESTAMP, "2"},   {"sub3", arrow::Type::DECIMAL128, "3"},
        {"sub4", arrow::Type::BINARY, "4"},      {"sub5", arrow::Type::BINARY, "5"},
        {"f1", arrow::Type::LIST, "6"},          {"sub1", arrow::Type::DATE32, "7"},
        {"sub2", arrow::Type::TIMESTAMP, "8"},   {"sub3", arrow::Type::DECIMAL128, "9"},
        {"sub4", arrow::Type::BINARY, "10"},     {"sub5", arrow::Type::BINARY, "11"},
        {"f2", arrow::Type::MAP, "12"},          {"sub1", arrow::Type::DATE32, "13"},
        {"sub2", arrow::Type::TIMESTAMP, "14"},  {"sub3", arrow::Type::DECIMAL128, "15"},
        {"sub4", arrow::Type::BINARY, "16"},     {"sub5", arrow::Type::BINARY, "17"},
        {"sub1", arrow::Type::DATE32, "18"},     {"sub2", arrow::Type::TIMESTAMP, "19"},
        {"sub3", arrow::Type::DECIMAL128, "20"}, {"sub4", arrow::Type::BINARY, "21"},
        {"sub5", arrow::Type::BINARY, "22"}};
    ASSERT_EQ(expected_field_infos, field_infos);
    // convert to paimon.id
    ASSERT_OK_AND_ASSIGN(auto old_schema,
                         ParquetFieldIdConverter::GetPaimonIdsFromParquetIds(new_schema));
    std::vector<FieldInfo> old_field_infos;
    PrintFieldMetadata(old_schema, ParquetFieldIdConverter::IdConvertType::PARQUET_TO_PAIMON_ID,
                       &old_field_infos);
    ASSERT_EQ(expected_field_infos, old_field_infos);
}

}  // namespace paimon::parquet::test
