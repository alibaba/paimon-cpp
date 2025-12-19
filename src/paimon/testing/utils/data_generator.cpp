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

#include "paimon/testing/utils/data_generator.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdlib>
#include <map>
#include <utility>

#include "arrow/api.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "fmt/format.h"
#include "paimon/common/data/binary_row_writer.h"
#include "paimon/common/data/binary_string.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/utils/file_store_path_factory.h"

namespace arrow {
class Array;
class ArrayBuilder;
}  // namespace arrow
namespace paimon {
class MemoryPool;
}  // namespace paimon

namespace paimon::test {

DataGenerator::DataGenerator(const std::shared_ptr<TableSchema>& table_schema,
                             const std::shared_ptr<MemoryPool>& memory_pool)
    : table_schema_(table_schema), memory_pool_(memory_pool) {
    assert(table_schema->Id() == 0);
}

Status DataGenerator::WriteBinaryRow(const BinaryRow& src_row, int32_t src_field_id,
                                     const std::shared_ptr<arrow::DataType>& src_type,
                                     int32_t target_field_id, BinaryRowWriter* target_row_writer) {
    arrow::Type::type type_id = src_type->id();
    switch (type_id) {
        case arrow::Type::type::BOOL: {
            target_row_writer->WriteBoolean(target_field_id, src_row.GetBoolean(src_field_id));
            break;
        }
        case arrow::Type::type::INT8: {
            target_row_writer->WriteByte(target_field_id, src_row.GetByte(src_field_id));
            break;
        }
        case arrow::Type::type::INT16: {
            target_row_writer->WriteShort(target_field_id, src_row.GetShort(src_field_id));
            break;
        }
        case arrow::Type::type::INT32: {
            target_row_writer->WriteInt(target_field_id, src_row.GetInt(src_field_id));
            break;
        }
        case arrow::Type::type::INT64: {
            target_row_writer->WriteLong(target_field_id, src_row.GetLong(src_field_id));
            break;
        }
        case arrow::Type::type::FLOAT: {
            target_row_writer->WriteFloat(target_field_id, src_row.GetFloat(src_field_id));
            break;
        }
        case arrow::Type::type::DOUBLE: {
            target_row_writer->WriteDouble(target_field_id, src_row.GetDouble(src_field_id));
            break;
        }
        case arrow::Type::type::STRING: {
            target_row_writer->WriteString(target_field_id, src_row.GetString(src_field_id));
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in write partial row", src_type->ToString()));
    }
    return Status::OK();
}

Result<BinaryRow> DataGenerator::ExtractPartialRow(const BinaryRow& binary_row,
                                                   const std::vector<DataField>& partition_fields) {
    BinaryRow partial_row(static_cast<int32_t>(partition_fields.size()));
    BinaryRowWriter writer(&partial_row, /*initial_size=*/0, memory_pool_.get());
    for (size_t field_idx = 0; field_idx < partition_fields.size(); field_idx++) {
        int32_t id = partition_fields[field_idx].Id();
        auto type = partition_fields[field_idx].Type();
        PAIMON_RETURN_NOT_OK(WriteBinaryRow(binary_row, id, type, field_idx, &writer));
    }
    writer.Complete();
    return partial_row;
}

Status DataGenerator::AppendValue(const BinaryRow& row, int32_t field_id,
                                  const std::shared_ptr<arrow::DataType>& type,
                                  arrow::StructBuilder* struct_builder) {
    arrow::Type::type type_id = type->id();
    switch (type_id) {
        case arrow::Type::type::BOOL: {
            auto builder =
                static_cast<arrow::BooleanBuilder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetBoolean(field_id)));
            break;
        }
        case arrow::Type::type::INT8: {
            auto builder =
                static_cast<arrow::Int8Builder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetByte(field_id)));
            break;
        }
        case arrow::Type::type::INT16: {
            auto builder =
                static_cast<arrow::Int16Builder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetShort(field_id)));
            break;
        }
        case arrow::Type::type::INT32: {
            auto builder =
                static_cast<arrow::Int32Builder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetInt(field_id)));
            break;
        }
        case arrow::Type::type::INT64: {
            auto builder =
                static_cast<arrow::Int64Builder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetLong(field_id)));
            break;
        }
        case arrow::Type::type::FLOAT: {
            auto builder =
                static_cast<arrow::FloatBuilder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetFloat(field_id)));
            break;
        }
        case arrow::Type::type::DOUBLE: {
            auto builder =
                static_cast<arrow::DoubleBuilder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetDouble(field_id)));
            break;
        }
        case arrow::Type::type::STRING: {
            auto builder =
                static_cast<arrow::StringBuilder*>(struct_builder->field_builder(field_id));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder->Append(row.GetString(field_id).ToString()));
            break;
        }
        default:
            return Status::Invalid(
                fmt::format("type {} not support in append value", type->ToString()));
    }
    return Status::OK();
}

Result<std::shared_ptr<arrow::StructBuilder>> DataGenerator::MakeStructBuilder(
    const std::vector<DataField>& fields) {
    auto data_type = DataField::ConvertDataFieldsToArrowStructType(fields);
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> field_builders;
    for (const auto& field : fields) {
        auto type_id = field.Type()->id();
        switch (type_id) {
            case arrow::Type::type::BOOL: {
                field_builders.push_back(std::make_shared<arrow::BooleanBuilder>());
                break;
            }
            case arrow::Type::type::INT8: {
                field_builders.push_back(std::make_shared<arrow::Int8Builder>());
                break;
            }
            case arrow::Type::type::INT16: {
                field_builders.push_back(std::make_shared<arrow::Int16Builder>());
                break;
            }
            case arrow::Type::type::INT32: {
                field_builders.push_back(std::make_shared<arrow::Int32Builder>());
                break;
            }
            case arrow::Type::type::INT64: {
                field_builders.push_back(std::make_shared<arrow::Int64Builder>());
                break;
            }
            case arrow::Type::type::FLOAT: {
                field_builders.push_back(std::make_shared<arrow::FloatBuilder>());
                break;
            }
            case arrow::Type::type::DOUBLE: {
                field_builders.push_back(std::make_shared<arrow::DoubleBuilder>());
                break;
            }
            case arrow::Type::type::STRING: {
                field_builders.push_back(std::make_shared<arrow::StringBuilder>());
                break;
            }
            default:
                return Status::Invalid(fmt::format("type {} not support in make struct builder",
                                                   field.Type()->ToString()));
        }
    }
    return std::make_shared<arrow::StructBuilder>(data_type, arrow::default_memory_pool(),
                                                  field_builders);
}

Result<std::vector<std::unique_ptr<RecordBatch>>> DataGenerator::SplitArrayByPartitionAndBucket(
    const std::vector<BinaryRow>& binary_rows) {
    auto fields = table_schema_->Fields();
    auto partition_keys = table_schema_->PartitionKeys();
    PAIMON_ASSIGN_OR_RAISE(auto partition_fields, table_schema_->GetFields(partition_keys));
    PAIMON_ASSIGN_OR_RAISE(auto bucket_fields,
                           table_schema_->GetFields(table_schema_->BucketKeys()));
    int32_t num_buckets = table_schema_->NumBuckets();
    // map: {partition_map, bucket_id} -> arrow::StructBuilder
    std::map<std::pair<std::map<std::string, std::string>, int32_t>,
             std::shared_ptr<arrow::StructBuilder>>
        struct_builder_holder;
    std::map<std::pair<std::map<std::string, std::string>, int32_t>,
             std::vector<RecordBatch::RowKind>>
        row_kinds_holder;
    auto schema = DataField::ConvertDataFieldsToArrowSchema(fields);
    PAIMON_ASSIGN_OR_RAISE(auto path_factory,
                           FileStorePathFactory::Create(
                               /*root=*/"/tmp", schema, partition_keys,
                               /*default_part_value=*/"__DEFAULT_PARTITION__",
                               /*identifier=*/"orc", /*data_file_prefix=*/"data-",
                               /*legacy_partition_name_enabled=*/true, std::vector<std::string>(),
                               /*index_file_in_data_file_dir=*/false, memory_pool_));

    for (const auto& binary_row : binary_rows) {
        PAIMON_ASSIGN_OR_RAISE(BinaryRow partition_row,
                               ExtractPartialRow(binary_row, partition_fields));

        std::vector<std::pair<std::string, std::string>> part_values;
        PAIMON_ASSIGN_OR_RAISE(part_values, path_factory->GeneratePartitionVector(partition_row));
        std::map<std::string, std::string> partition_map;
        for (const auto& part_value : part_values) {
            partition_map[part_value.first] = part_value.second;
        }
        PAIMON_ASSIGN_OR_RAISE(BinaryRow bucket_row, ExtractPartialRow(binary_row, bucket_fields));
        int32_t bucket_id = std::abs(bucket_row.HashCode() % num_buckets);
        auto struct_builder_iter = struct_builder_holder.find({partition_map, bucket_id});
        if (struct_builder_iter == struct_builder_holder.end()) {
            PAIMON_ASSIGN_OR_RAISE(auto struct_builder, MakeStructBuilder(fields));
            auto insert_result = struct_builder_holder.emplace(
                std::make_pair(partition_map, bucket_id), struct_builder);
            if (!insert_result.second) {
                return Status::Invalid("insert element to struct_builder_holder failed");
            }
            struct_builder_iter = insert_result.first;
        }
        auto struct_builder = struct_builder_iter->second;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Append());
        for (size_t i = 0; i < fields.size(); i++) {
            PAIMON_RETURN_NOT_OK(
                AppendValue(binary_row, i, fields[i].Type(), struct_builder.get()));
        }

        auto row_kinds_iter = row_kinds_holder.find({partition_map, bucket_id});
        if (row_kinds_iter == row_kinds_holder.end()) {
            auto insert_result = row_kinds_holder.emplace(std::make_pair(partition_map, bucket_id),
                                                          std::vector<RecordBatch::RowKind>());
            if (!insert_result.second) {
                return Status::Invalid("insert element to row_kinds_holder failed");
            }
            row_kinds_iter = insert_result.first;
        }
        auto& row_kinds = row_kinds_iter->second;
        PAIMON_ASSIGN_OR_RAISE(auto row_kind, binary_row.GetRowKind());
        row_kinds.push_back(static_cast<RecordBatch::RowKind>(row_kind->ToByteValue()));
    }

    std::vector<std::unique_ptr<RecordBatch>> batches;
    for (const auto& [identifier, struct_builder] : struct_builder_holder) {
        std::shared_ptr<arrow::Array> array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(struct_builder->Finish(&array));
        auto& row_kinds = row_kinds_holder[identifier];

        ArrowArray arrow_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &arrow_array));
        RecordBatchBuilder batch_builder(&arrow_array);
        PAIMON_ASSIGN_OR_RAISE(auto record_batch, batch_builder.SetPartition(identifier.first)
                                                      .SetBucket(identifier.second)
                                                      .SetRowKinds(row_kinds)
                                                      .Finish());
        batches.push_back(std::move(record_batch));
    }
    return batches;
}

}  // namespace paimon::test
