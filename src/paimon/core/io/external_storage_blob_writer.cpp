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

#include "paimon/core/io/external_storage_blob_writer.h"

#include <utility>
#include <vector>

#include "arrow/array/array_nested.h"
#include "arrow/array/builder_binary.h"
#include "arrow/c/bridge.h"
#include "arrow/type.h"
#include "paimon/common/data/blob_descriptor.h"
#include "paimon/common/data/blob_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_path_factory.h"
#include "paimon/core/io/data_file_writer.h"
#include "paimon/format/blob/blob_writer_builder.h"
#include "paimon/format/file_format.h"
#include "paimon/format/file_format_factory.h"
#include "paimon/fs/file_system.h"
#include "paimon/memory/memory_pool.h"

namespace paimon {

ExternalStorageBlobWriter::ExternalStorageBlobWriter(
    const std::shared_ptr<arrow::Schema>& write_schema,
    const std::set<std::string>& external_storage_fields, const std::string& external_storage_path,
    int64_t schema_id, const std::shared_ptr<LongCounter>& seq_num_counter,
    const std::shared_ptr<DataFilePathFactory>& path_factory, const CoreOptions& options,
    const std::shared_ptr<MemoryPool>& memory_pool)
    : write_schema_(write_schema),
      external_storage_fields_(external_storage_fields),
      external_storage_path_(external_storage_path),
      schema_id_(schema_id),
      seq_num_counter_(seq_num_counter),
      path_factory_(path_factory),
      memory_pool_(memory_pool),
      options_(options) {}

Result<std::unique_ptr<ExternalStorageBlobWriter::BlobRollingWriter>>
ExternalStorageBlobWriter::CreateFieldRollingWriter(FieldWriter* field_writer) {
    auto field = write_schema_->GetFieldByName(field_writer->field_name);
    if (!field) {
        return Status::Invalid("External storage field '{}' not found in write schema",
                               field_writer->field_name);
    }

    auto single_field_schema = arrow::schema({field});
    ::ArrowSchema arrow_schema;
    ScopeGuard guard([&arrow_schema]() { ArrowSchemaRelease(&arrow_schema); });
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*single_field_schema, &arrow_schema));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileFormat> format,
                           FileFormatFactory::Get("blob", options_.ToMap()));
    PAIMON_ASSIGN_OR_RAISE(
        std::shared_ptr<WriterBuilder> writer_builder,
        format->CreateWriterBuilder(&arrow_schema, options_.GetWriteBatchSize()));
    writer_builder->WithMemoryPool(memory_pool_);

    // Inject WriteConsumer to capture BlobDescriptors during writes
    auto blob_writer_builder = std::dynamic_pointer_cast<blob::BlobWriterBuilder>(writer_builder);
    if (!blob_writer_builder) {
        return Status::Invalid(
            "writer_builder cannot be casted to BlobWriterBuilder in ExternalStorageBlobWriter");
    }
    blob_writer_builder->WithWriteConsumer(
        [field_writer](std::unique_ptr<BlobDescriptor> descriptor) -> bool {
            field_writer->captured_descriptors.push_back(std::move(descriptor));
            return true;  // Always flush for single row.
        });

    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportSchema(*single_field_schema, &arrow_schema));
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<FormatStatsExtractor> stats_extractor,
                           format->CreateStatsExtractor(&arrow_schema));

    std::vector<std::string> write_cols = {field_writer->field_name};
    auto single_blob_file_writer_creator = [this, writer_builder, stats_extractor, write_cols]()
        -> Result<std::unique_ptr<SingleFileWriter<::ArrowArray*, std::shared_ptr<DataFileMeta>>>> {
        auto writer = std::make_unique<DataFileWriter>(
            /*compression=*/"none", std::function<Status(ArrowArray*, ArrowArray*)>(), schema_id_,
            seq_num_counter_, FileSource::Append(), stats_extractor,
            path_factory_->IsExternalPath(), write_cols, memory_pool_);
        PAIMON_RETURN_NOT_OK(writer->Init(
            options_.GetFileSystem(),
            path_factory_->NewExternalStorageBlobPath(external_storage_path_), writer_builder));
        return writer;
    };

    return std::make_unique<BlobRollingWriter>(options_.GetBlobTargetFileSize(),
                                               single_blob_file_writer_creator);
}

Status ExternalStorageBlobWriter::InitializeFieldWritersIfNeeded() {
    if (initialized_) {
        return Status::OK();
    }
    for (int32_t i = 0; i < write_schema_->num_fields(); ++i) {
        const auto& field = write_schema_->field(i);
        if (external_storage_fields_.count(field->name()) > 0) {
            FieldWriter fw;
            fw.field_name = field->name();
            fw.field_index = i;
            field_writers_.push_back(std::move(fw));
        }
    }
    // Create rolling writers after push_back so FieldWriter addresses are stable
    // for the consumer lambda capture.
    for (auto& fw : field_writers_) {
        PAIMON_ASSIGN_OR_RAISE(fw.rolling_writer, CreateFieldRollingWriter(&fw));
    }
    initialized_ = true;
    return Status::OK();
}

Result<std::shared_ptr<arrow::Array>> ExternalStorageBlobWriter::TransformField(
    const std::shared_ptr<arrow::Array>& column, FieldWriter* field_writer) {
    int64_t num_rows = column->length();

    // Clear captured descriptors before processing this batch
    field_writer->captured_descriptors.clear();

    // Write each row via RollingFileWriter; the consumer captures the descriptor
    for (int64_t row = 0; row < num_rows; ++row) {
        std::shared_ptr<arrow::Array> slice = column->Slice(row, 1);
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(
            std::shared_ptr<arrow::StructArray> single_row_struct,
            arrow::StructArray::Make({slice}, {field_writer->field_name}));

        ::ArrowArray c_array;
        PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*single_row_struct, &c_array));
        PAIMON_RETURN_NOT_OK(field_writer->rolling_writer->Write(&c_array));
    }

    // Validate captured descriptor count
    if (static_cast<int64_t>(field_writer->captured_descriptors.size()) != num_rows) {
        return Status::Invalid(
            "Captured descriptor count {} does not match row count {} for field '{}'",
            field_writer->captured_descriptors.size(), num_rows, field_writer->field_name);
    }

    // Build descriptor column from captured descriptors
    arrow::LargeBinaryBuilder descriptor_builder;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(descriptor_builder.Reserve(num_rows));
    for (int64_t row = 0; row < num_rows; ++row) {
        const auto& descriptor = field_writer->captured_descriptors[row];
        if (!descriptor) {
            PAIMON_RETURN_NOT_OK_FROM_ARROW(descriptor_builder.AppendNull());
        } else {
            auto serialized = descriptor->Serialize(memory_pool_);
            PAIMON_RETURN_NOT_OK_FROM_ARROW(
                descriptor_builder.Append(serialized->data(), serialized->size()));
        }
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> descriptor_array,
                                      descriptor_builder.Finish());
    return descriptor_array;
}

Result<std::shared_ptr<arrow::StructArray>> ExternalStorageBlobWriter::TransformBatch(
    const std::shared_ptr<arrow::StructArray>& batch) {
    if (external_storage_fields_.empty()) {
        return batch;
    }

    PAIMON_RETURN_NOT_OK(InitializeFieldWritersIfNeeded());

    if (field_writers_.empty()) {
        return batch;
    }

    // Collect all arrays and field names from the original batch
    std::vector<std::shared_ptr<arrow::Array>> result_arrays;
    std::vector<std::string> result_names;
    result_arrays.reserve(batch->num_fields());
    result_names.reserve(batch->num_fields());

    for (int32_t col = 0; col < batch->num_fields(); ++col) {
        result_names.push_back(batch->type()->field(col)->name());
        result_arrays.push_back(batch->field(col));
    }

    // Transform each external storage field and replace in result
    for (FieldWriter& fw : field_writers_) {
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> descriptor_array,
                               TransformField(batch->field(fw.field_index), &fw));
        result_arrays[fw.field_index] = descriptor_array;
    }

    PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::StructArray> result,
                                      arrow::StructArray::Make(result_arrays, result_names));
    return result;
}

Status ExternalStorageBlobWriter::Close() {
    for (FieldWriter& fw : field_writers_) {
        if (fw.rolling_writer) {
            PAIMON_RETURN_NOT_OK(fw.rolling_writer->Close());
        }
    }
    return Status::OK();
}

void ExternalStorageBlobWriter::Abort() {
    for (FieldWriter& fw : field_writers_) {
        if (fw.rolling_writer) {
            fw.rolling_writer->Abort();
            fw.rolling_writer.reset();
        }
    }
    field_writers_.clear();
}

}  // namespace paimon
