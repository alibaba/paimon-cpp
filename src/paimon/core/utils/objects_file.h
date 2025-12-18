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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "arrow/c/helpers.h"
#include "paimon/common/data/columnar/columnar_row.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/arrow/status_utils.h"
#include "paimon/common/utils/path_util.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/meta_to_arrow_array_converter.h"
#include "paimon/core/utils/manifest_meta_reader.h"
#include "paimon/core/utils/object_serializer.h"
#include "paimon/core/utils/path_factory.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/reader_builder.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/record_batch.h"

namespace paimon {
/// A file which contains several `T`s, provides read and write.
class PredicateFilter;
template <typename T>
class ObjectsFile {
 public:
    ObjectsFile(const std::shared_ptr<FileSystem>& file_system,
                const std::shared_ptr<ReaderBuilder>& reader_builder,
                const std::shared_ptr<WriterBuilder>& writer_builder,
                std::unique_ptr<ObjectSerializer<T>>&& serializer, const std::string& compression,
                const std::shared_ptr<PathFactory>& path_factory,
                const std::shared_ptr<MemoryPool>& pool);

    virtual ~ObjectsFile() = default;

    Status Read(const std::string& file_name, const std::function<Result<bool>(const T&)>& filter,
                std::vector<T>* result) const;
    Status ReadIfFileExist(const std::string& file_name,
                           const std::function<Result<bool>(const T&)>& filter,
                           std::vector<T>* result) const;

    void DeleteQuietly(const std::string& file_name) {
        std::string path = path_factory_->ToPath(file_name);
        auto status = file_system_->Delete(path);
        // delete quietly will ignore any status error
        (void)status;
    }

    Result<std::pair<std::string, int64_t>> WriteWithoutRolling(const std::vector<T>& records);

 protected:
    std::shared_ptr<PathFactory> path_factory_;
    std::shared_ptr<MemoryPool> pool_;
    std::unique_ptr<ObjectSerializer<T>> serializer_;
    std::shared_ptr<WriterBuilder> writer_builder_;
    std::unique_ptr<MetaToArrowArrayConverter> to_array_converter_;

 private:
    std::shared_ptr<FileSystem> file_system_;
    std::shared_ptr<ReaderBuilder> reader_builder_;
    std::string compression_;
};

template <typename T>
ObjectsFile<T>::ObjectsFile(const std::shared_ptr<FileSystem>& file_system,
                            const std::shared_ptr<ReaderBuilder>& reader_builder,
                            const std::shared_ptr<WriterBuilder>& writer_builder,
                            std::unique_ptr<ObjectSerializer<T>>&& serializer,
                            const std::string& compression,
                            const std::shared_ptr<PathFactory>& path_factory,
                            const std::shared_ptr<MemoryPool>& pool)
    : path_factory_(path_factory),
      pool_(pool),
      serializer_(std::move(serializer)),
      writer_builder_(std::move(writer_builder)),
      file_system_(file_system),
      reader_builder_(std::move(reader_builder)),
      compression_(compression) {
    // TODO(xinyu.lxy): add cache
}

template <typename T>
Status ObjectsFile<T>::ReadIfFileExist(const std::string& file_name,
                                       const std::function<Result<bool>(const T&)>& filter,
                                       std::vector<T>* result) const {
    std::string file_path = path_factory_->ToPath(file_name);
    PAIMON_ASSIGN_OR_RAISE(bool path_exist, file_system_->Exists(file_path));
    if (path_exist) {
        return Read(file_name, filter, result);
    }
    return Status::OK();
}

template <typename T>
Status ObjectsFile<T>::Read(const std::string& file_name,
                            const std::function<Result<bool>(const T&)>& filter,
                            std::vector<T>* result) const {
    std::string file_path = path_factory_->ToPath(file_name);
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<InputStream> file_input_stream,
                           file_system_->Open(file_path));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileBatchReader> batch_reader,
                           reader_builder_->Build(file_input_stream));
    auto reader = std::make_unique<ManifestMetaReader>(std::move(batch_reader),
                                                       serializer_->GetDataType(), pool_);
    while (true) {
        PAIMON_ASSIGN_OR_RAISE(BatchReader::ReadBatch arrow_array, reader->NextBatch());
        auto& c_array = arrow_array.first;
        auto& c_schema = arrow_array.second;
        if (!c_array) {
            break;
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(std::shared_ptr<arrow::Array> typed_array,
                                          arrow::ImportArray(c_array.get(), c_schema.get()));
        auto* struct_array = dynamic_cast<arrow::StructArray*>(typed_array.get());
        if (!struct_array) {
            return Status::Invalid(fmt::format("file {}, cannot cast to struct array", file_name));
        }
        result->reserve(struct_array->length());
        for (int64_t i = 0; i < struct_array->length(); i++) {
            ColumnarRow row(struct_array->fields(), pool_, i);
            PAIMON_ASSIGN_OR_RAISE(T obj, serializer_->FromRow(row));
            if (filter) {
                PAIMON_ASSIGN_OR_RAISE(bool filter_res, filter(obj));
                if (filter_res) {
                    result->push_back(std::move(obj));
                }
            } else {
                result->push_back(std::move(obj));
            }
        }
    }
    reader->Close();
    return Status::OK();
}

template <typename T>
Result<std::pair<std::string, int64_t>> ObjectsFile<T>::WriteWithoutRolling(
    const std::vector<T>& records) {
    std::string file_path = path_factory_->NewPath();
    std::vector<BinaryRow> rows;
    rows.reserve(records.size());
    for (const auto& record : records) {
        PAIMON_ASSIGN_OR_RAISE(BinaryRow row, serializer_->ToRow(record));
        rows.push_back(std::move(row));
    }
    if (!to_array_converter_) {
        PAIMON_ASSIGN_OR_RAISE(to_array_converter_, MetaToArrowArrayConverter::Create(
                                                        serializer_->GetDataType(), pool_));
    }
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Array> array,
                           to_array_converter_->NextBatch(rows));
    ::ArrowArray c_array;
    PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*array, &c_array));
    ScopeGuard guard([&]() {
        ArrowArrayRelease(&c_array);
        DeleteQuietly(file_path);
    });
    PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<OutputStream> out,
                           file_system_->Create(file_path, /*overwrite=*/false));
    PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FormatWriter> format_writer,
                           writer_builder_->Build(out, compression_));
    PAIMON_RETURN_NOT_OK(format_writer->AddBatch(&c_array));
    PAIMON_RETURN_NOT_OK(format_writer->Flush());
    PAIMON_RETURN_NOT_OK(format_writer->Finish());
    PAIMON_RETURN_NOT_OK(out->Flush());
    PAIMON_ASSIGN_OR_RAISE(int64_t pos, out->GetPos());
    PAIMON_RETURN_NOT_OK(out->Close());
    guard.Release();
    return std::make_pair(PathUtil::GetName(file_path), pos);
}

}  // namespace paimon
