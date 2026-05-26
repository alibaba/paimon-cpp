/*
 * Copyright 2026-present Alibaba Inc.
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

#include "paimon/core/table/system/audit_log_system_table.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/c/abi.h"
#include "arrow/c/bridge.h"
#include "arrow/util/checked_cast.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/common/types/row_kind.h"
#include "paimon/common/utils/arrow/mem_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/core/core_options.h"
#include "paimon/core/schema/table_schema.h"
#include "paimon/core/table/source/key_value_table_read.h"
#include "paimon/defs.h"
#include "paimon/read_context.h"
#include "paimon/scan_context.h"
#include "paimon/status.h"
#include "paimon/table/source/table_read.h"
#include "paimon/table/source/table_scan.h"

namespace paimon {
namespace {

class AuditLogBatchConverter : public ChangelogBatchConverter {
 public:
    Result<std::shared_ptr<arrow::Array>> ConvertDataColumn(
        const std::shared_ptr<arrow::Array>& array, arrow::MemoryPool* /*pool*/) const override {
        return array;
    }
};

class ChangelogBatchReader : public BatchReader {
 public:
    ChangelogBatchReader(std::unique_ptr<BatchReader> reader,
                         std::shared_ptr<arrow::Schema> output_schema, bool include_sequence_number,
                         std::shared_ptr<const ChangelogBatchConverter> converter,
                         const std::shared_ptr<MemoryPool>& pool)
        : reader_(std::move(reader)),
          output_schema_(std::move(output_schema)),
          include_sequence_number_(include_sequence_number),
          converter_(std::move(converter)),
          arrow_pool_holder_(GetArrowPool(pool)),
          arrow_pool_(arrow_pool_holder_.get()) {}

    Result<ReadBatch> NextBatch() override {
        PAIMON_ASSIGN_OR_RAISE(ReadBatch batch, reader_->NextBatch());
        if (BatchReader::IsEofBatch(batch)) {
            return batch;
        }
        auto& [c_array, c_schema] = batch;
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> arrow_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        std::shared_ptr<arrow::StructArray> struct_array =
            std::dynamic_pointer_cast<arrow::StructArray>(arrow_array);
        if (!struct_array) {
            return Status::Invalid("audit_log system table expects struct batches");
        }

        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> rowkind_array,
                               BuildRowKindArray(struct_array));

        arrow::ArrayVector output_arrays = {rowkind_array};
        if (include_sequence_number_) {
            std::shared_ptr<arrow::Array> sequence_array =
                struct_array->GetFieldByName(SpecialFields::SequenceNumber().Name());
            if (!sequence_array) {
                return Status::Invalid("cannot find _SEQUENCE_NUMBER in audit_log batch");
            }
            PAIMON_ASSIGN_OR_RAISE(sequence_array, CopyToStablePool(sequence_array));
            output_arrays.push_back(sequence_array);
        }

        for (const auto& field : output_schema_->fields()) {
            if (field->name() == SpecialFields::RowKind().Name() ||
                field->name() == SpecialFields::SequenceNumber().Name()) {
                continue;
            }
            std::shared_ptr<arrow::Array> array = struct_array->GetFieldByName(field->name());
            if (!array) {
                return Status::Invalid("cannot find ", field->name(), " in changelog batch");
            }
            PAIMON_ASSIGN_OR_RAISE(array, converter_->ConvertDataColumn(array, arrow_pool_));
            PAIMON_ASSIGN_OR_RAISE(array, CopyToStablePool(array));
            output_arrays.push_back(array);
        }

        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> output_array,
            arrow::StructArray::Make(output_arrays, output_schema_->field_names()));
        auto output_c_array = std::make_unique<ArrowArray>();
        auto output_c_schema = std::make_unique<ArrowSchema>();
        PAIMON_RETURN_NOT_OK_FROM_ARROW(
            arrow::ExportArray(*output_array, output_c_array.get(), output_c_schema.get()));
        return std::make_pair(std::move(output_c_array), std::move(output_c_schema));
    }

    std::shared_ptr<Metrics> GetReaderMetrics() const override {
        return reader_->GetReaderMetrics();
    }

    void Close() override {
        reader_->Close();
    }

 private:
    Result<std::shared_ptr<arrow::Array>> CopyToStablePool(
        const std::shared_ptr<arrow::Array>& array) const {
        /// The imported data batch may release its C Arrow buffers after this wrapper returns.
        /// Keep returned system-table arrays independent of that input batch lifetime.
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> result,
                                          arrow::Concatenate({array}, arrow_pool_));
        return result;
    }

    Result<std::shared_ptr<arrow::Array>> BuildRowKindArray(
        const std::shared_ptr<arrow::StructArray>& struct_array) const {
        std::shared_ptr<arrow::Int8Array> value_kind_array =
            std::dynamic_pointer_cast<arrow::Int8Array>(
                struct_array->GetFieldByName(SpecialFields::ValueKind().Name()));
        if (!value_kind_array) {
            return Status::Invalid("cannot find _VALUE_KIND in audit_log batch");
        }
        arrow::StringBuilder builder(arrow_pool_);
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Reserve(value_kind_array->length()));
        for (int64_t i = 0; i < value_kind_array->length(); ++i) {
            if (value_kind_array->IsNull(i)) {
                PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.AppendNull());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(const RowKind* row_kind,
                                   RowKind::FromByteValue(value_kind_array->Value(i)));
            PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Append(row_kind->ShortString()));
        }
        std::shared_ptr<arrow::Array> result;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(builder.Finish(&result));
        return result;
    }

    std::unique_ptr<BatchReader> reader_;
    std::shared_ptr<arrow::Schema> output_schema_;
    bool include_sequence_number_;
    std::shared_ptr<const ChangelogBatchConverter> converter_;
    std::unique_ptr<arrow::MemoryPool> arrow_pool_holder_;
    arrow::MemoryPool* arrow_pool_;
};

class ChangelogTableRead : public TableRead {
 public:
    ChangelogTableRead(std::unique_ptr<TableRead> data_read,
                       std::shared_ptr<arrow::Schema> output_schema, bool include_sequence_number,
                       std::shared_ptr<const ChangelogBatchConverter> converter,
                       const std::shared_ptr<MemoryPool>& pool)
        : TableRead(pool),
          data_read_(std::move(data_read)),
          output_schema_(std::move(output_schema)),
          include_sequence_number_(include_sequence_number),
          converter_(std::move(converter)) {}

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::vector<std::shared_ptr<Split>>& splits) override {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<BatchReader> reader,
                               data_read_->CreateReader(splits));
        return std::make_unique<ChangelogBatchReader>(std::move(reader), output_schema_,
                                                      include_sequence_number_, converter_,
                                                      GetMemoryPool());
    }

    Result<std::unique_ptr<BatchReader>> CreateReader(
        const std::shared_ptr<Split>& split) override {
        std::vector<std::shared_ptr<Split>> splits = {split};
        return CreateReader(splits);
    }

 private:
    std::unique_ptr<TableRead> data_read_;
    std::shared_ptr<arrow::Schema> output_schema_;
    bool include_sequence_number_;
    std::shared_ptr<const ChangelogBatchConverter> converter_;
};

}  // namespace

AuditLogSystemTable::AuditLogSystemTable(std::shared_ptr<FileSystem> fs, std::string table_path,
                                         std::shared_ptr<TableSchema> table_schema,
                                         std::map<std::string, std::string> options)
    : fs_(std::move(fs)),
      table_path_(std::move(table_path)),
      table_schema_(std::move(table_schema)),
      options_(std::move(options)) {}

std::string AuditLogSystemTable::Name() const {
    return kName;
}

Result<std::shared_ptr<arrow::Schema>> AuditLogSystemTable::ArrowSchema() const {
    std::shared_ptr<arrow::Field> rowkind_field =
        DataField::ConvertDataFieldToArrowField(SpecialFields::RowKind());
    rowkind_field = rowkind_field->WithNullable(false);
    arrow::FieldVector fields = {rowkind_field};
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    if (core_options.TableReadSequenceNumberEnabled()) {
        fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    }
    for (const auto& field : table_schema_->Fields()) {
        fields.push_back(field.ArrowField());
    }
    return arrow::schema(fields);
}

Result<std::unique_ptr<TableScan>> AuditLogSystemTable::NewScan(
    const std::shared_ptr<ScanContext>& context) const {
    if (context->GetScanFilters() && context->GetScanFilters()->GetPredicate()) {
        return Status::NotImplemented("audit_log system table predicate pushdown is not supported");
    }
    std::shared_ptr<ScanFilter> scan_filter = context->GetScanFilters();
    ScanContextBuilder builder(table_path_);
    builder.SetOptions(options_)
        .WithStreamingMode(context->IsStreamingMode())
        .WithMemoryPool(context->GetMemoryPool())
        .WithExecutor(context->GetExecutor())
        .WithFileSystem(context->GetSpecificFileSystem());
    if (scan_filter) {
        if (scan_filter->GetBucketFilter()) {
            builder.SetBucketFilter(scan_filter->GetBucketFilter().value());
        }
        if (!scan_filter->GetPartitionFilters().empty()) {
            builder.SetPartitionFilter(scan_filter->GetPartitionFilters());
        }
    }
    if (context->GetLimit()) {
        builder.SetLimit(context->GetLimit().value());
    }
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ScanContext> data_context, builder.Finish());
    return TableScan::Create(std::move(data_context));
}

Result<std::unique_ptr<TableRead>> AuditLogSystemTable::NewRead(
    const std::shared_ptr<ReadContext>& context) const {
    return NewChangelogRead(context, std::make_shared<AuditLogBatchConverter>());
}

Result<std::unique_ptr<TableRead>> AuditLogSystemTable::NewChangelogRead(
    const std::shared_ptr<ReadContext>& context,
    std::shared_ptr<const ChangelogBatchConverter> converter) const {
    if (table_schema_->PrimaryKeys().empty()) {
        return Status::NotImplemented(Name(), " system table only supports primary key table");
    }
    if (context->GetPredicate()) {
        return Status::NotImplemented(Name(), " system table predicate pushdown is not supported");
    }

    ReadContextBuilder builder(table_path_);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> base_read_schema, BaseReadSchema());
    using StringMap = std::map<std::string, std::string>;
    PAIMON_ASSIGN_OR_RAISE(StringMap read_options, ReadOptions());
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(read_options));
    builder.SetOptions(read_options)
        .SetReadSchema(base_read_schema->field_names())
        .WithBranch(core_options.GetBranch())
        .WithMemoryPool(context->GetMemoryPool())
        .WithExecutor(context->GetExecutor())
        .WithFileSystem(context->GetSpecificFileSystem())
        .WithFileSystemSchemeToIdentifierMap(context->GetFileSystemSchemeToIdentifierMap())
        .EnablePrefetch(context->EnablePrefetch())
        .SetPrefetchBatchCount(context->GetPrefetchBatchCount())
        .SetPrefetchMaxParallelNum(context->GetPrefetchMaxParallelNum())
        .EnableMultiThreadRowToBatch(context->EnableMultiThreadRowToBatch())
        .SetRowToBatchThreadNumber(context->GetRowToBatchThreadNumber())
        .SetPrefetchCacheMode(context->GetPrefetchCacheMode())
        .WithCacheConfig(context->GetCacheConfig());

    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<ReadContext> data_context, builder.Finish());
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<TableRead> data_read,
                           TableRead::Create(std::move(data_context)));
    auto* key_value_read = dynamic_cast<KeyValueTableRead*>(data_read.get());
    if (!key_value_read) {
        return Status::Invalid("audit_log system table requires key-value table read");
    }
    key_value_read->ForceKeepDelete(true);
    bool include_sequence_number = core_options.TableReadSequenceNumberEnabled();
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Schema> output_schema, ArrowSchema());
    return std::make_unique<ChangelogTableRead>(std::move(data_read), std::move(output_schema),
                                                include_sequence_number, std::move(converter),
                                                context->GetMemoryPool());
}

Result<std::shared_ptr<arrow::Schema>> AuditLogSystemTable::BaseReadSchema() const {
    arrow::FieldVector fields = {
        DataField::ConvertDataFieldToArrowField(SpecialFields::ValueKind())};
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    bool include_sequence_number = core_options.TableReadSequenceNumberEnabled();
    if (include_sequence_number) {
        fields.push_back(DataField::ConvertDataFieldToArrowField(SpecialFields::SequenceNumber()));
    }
    for (const auto& field : table_schema_->Fields()) {
        fields.push_back(field.ArrowField());
    }
    return arrow::schema(fields);
}

Result<std::map<std::string, std::string>> AuditLogSystemTable::ReadOptions() const {
    auto read_options = options_;
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options, CoreOptions::FromMap(options_));
    if (core_options.TableReadSequenceNumberEnabled()) {
        read_options[Options::KEY_VALUE_SEQUENCE_NUMBER_ENABLED] = "true";
    }
    return read_options;
}

}  // namespace paimon
