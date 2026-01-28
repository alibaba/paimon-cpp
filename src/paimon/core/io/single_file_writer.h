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

#include <cassert>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
#include "fmt/format.h"
#include "paimon/common/utils/arrow/arrow_utils.h"
#include "paimon/common/utils/scope_guard.h"
#include "paimon/core/io/data_file_meta.h"
#include "paimon/core/io/file_writer.h"
#include "paimon/format/format_writer.h"
#include "paimon/format/writer_builder.h"
#include "paimon/fs/file_system.h"
#include "paimon/logging.h"
#include "paimon/macros.h"
#include "paimon/record_batch.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace paimon {

class RecordBatch;
class Metrics;

/// A `FileWriter` to produce a single file.
///
/// <T> type of records to write.
/// <R> is the type of result to produce after writing a file.
template <typename T, typename R>
class SingleFileWriter : public FileWriter<T, R> {
 public:
    /// Abort executor to just have reference of path instead of whole writer.
    class AbortExecutor {
     public:
        AbortExecutor(const std::shared_ptr<FileSystem>& fs, const std::string& path)
            : fs_(fs), path_(path), logger_(Logger::GetLogger("AbortExecutor")) {}

        void Abort() {
            if (fs_) {
                auto status = fs_->Delete(path_);
                if (!status.ok()) {
                    PAIMON_LOG_WARN(logger_, "Exception occurs when deleting %s: %s", path_.c_str(),
                                    status.ToString().c_str());
                }
            }
        }

     private:
        std::shared_ptr<FileSystem> fs_;
        std::string path_;
        std::shared_ptr<Logger> logger_;
    };

    SingleFileWriter(const std::string& compression,
                     std::function<Status(T, ::ArrowArray*)> converter)
        : compression_(compression),
          converter_(converter),
          logger_(Logger::GetLogger("SingleFileWriter")) {}

    virtual Status Init(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                        const std::shared_ptr<WriterBuilder>& writer_builder);

    Status Write(T record) override;

    int64_t RecordCount() const override {
        return record_count_;
    }
    void Abort() override;
    Status Close() override;

    std::shared_ptr<Metrics> GetMetrics() const override {
        if (writer_) {
            return writer_->GetWriterMetrics();
        }
        return nullptr;
    }

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size);

    Result<AbortExecutor> GetAbortExecutor() const {
        if (closed_ == false) {
            return Status::Invalid("Writer should be closed!");
        }
        return AbortExecutor(fs_, path_);
    }

    std::string GetPath() const {
        return path_;
    }

 protected:
    int64_t output_bytes_ = -1;
    std::string compression_;
    std::function<Status(T, ArrowArray*)> converter_;
    std::shared_ptr<FileSystem> fs_;
    std::shared_ptr<OutputStream> out_;  // nullptr for DirectWriterBuilder
    bool closed_ = false;
    std::string path_;

 private:
    int64_t record_count_ = 0;
    std::unique_ptr<FormatWriter> writer_;

    std::unique_ptr<Logger> logger_;
};

template <typename T, typename R>
Status SingleFileWriter<T, R>::Init(const std::shared_ptr<FileSystem>& fs, const std::string& path,
                                    const std::shared_ptr<WriterBuilder>& writer_builder) {
    ScopeGuard guard([this]() -> void {
        this->Abort();
        PAIMON_LOG_WARN(logger_,
                        "Exception occurs when initializing single file writer %s. Cleaning up.",
                        path_.c_str());
    });
    path_ = path;
    fs_ = fs;

    if (auto specific_fs_writer_builder =
            std::dynamic_pointer_cast<SpecificFSWriterBuilder>(writer_builder)) {
        specific_fs_writer_builder->WithFileSystem(fs);
    }
    if (auto direct_writer_builder =
            std::dynamic_pointer_cast<DirectWriterBuilder>(writer_builder)) {
        PAIMON_ASSIGN_OR_RAISE(writer_, direct_writer_builder->BuildFromPath(path));
    } else {
        PAIMON_ASSIGN_OR_RAISE(out_, fs_->Create(path, /*overwrite=*/false));
        PAIMON_ASSIGN_OR_RAISE(writer_, writer_builder->Build(out_, compression_));
        assert(out_);
    }
    assert(writer_);
    record_count_ = 0;
    closed_ = false;
    guard.Release();
    return Status::OK();
}

template <typename T, typename R>
Status SingleFileWriter<T, R>::Write(T record) {
    if (PAIMON_UNLIKELY(closed_)) {
        return Status::Invalid("Writer has already closed!");
    }
    ScopeGuard guard([this]() -> void { this->Abort(); });
    int64_t record_count = 0;
    if (!converter_) {
        if constexpr (std::is_same_v<T, ::ArrowArray*>) {
            record_count = record->length;
            ScopeGuard inner_guard([&record]() { ArrowArrayRelease(record); });
            PAIMON_RETURN_NOT_OK(writer_->AddBatch(record));
            inner_guard.Release();
        } else {
            return Status::Invalid("converter is not set");
        }
    } else {
        ArrowArray array;
        ArrowArrayMarkReleased(&array);  // reset array
        ScopeGuard inner_guard([&array]() { ArrowArrayRelease(&array); });
        PAIMON_RETURN_NOT_OK(converter_(std::move(record), &array));
        record_count = array.length;
        PAIMON_RETURN_NOT_OK(writer_->AddBatch(&array));
        inner_guard.Release();
    }
    record_count_ += record_count;
    guard.Release();
    return Status::OK();
}

template <typename T, typename R>
Status SingleFileWriter<T, R>::Close() {
    if (closed_) {
        return Status::OK();
    }
    PAIMON_LOG_DEBUG(logger_, "Closing file %s", path_.c_str());
    ScopeGuard guard([this]() -> void {
        this->Abort();
        PAIMON_LOG_WARN(logger_, "Exception occurs when closing file %s. Cleaning up.",
                        path_.c_str());
    });
    PAIMON_RETURN_NOT_OK(writer_->Flush());
    PAIMON_RETURN_NOT_OK(writer_->Finish());
    if (out_) {
        PAIMON_RETURN_NOT_OK(out_->Flush());
        PAIMON_ASSIGN_OR_RAISE(output_bytes_, out_->GetPos());
        PAIMON_RETURN_NOT_OK(out_->Close());
    } else {
        PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<FileStatus> file_status, fs_->GetFileStatus(path_));
        output_bytes_ = file_status->GetLen();
    }
    closed_ = true;
    guard.Release();
    return Status::OK();
}

template <typename T, typename R>
Result<bool> SingleFileWriter<T, R>::ReachTargetSize(bool suggested_check, int64_t target_size) {
    return writer_->ReachTargetSize(suggested_check, target_size);
}

template <typename T, typename R>
void SingleFileWriter<T, R>::Abort() {
    if (out_) {
        auto status = out_->Close();
        if (!status.ok()) {
            PAIMON_LOG_WARN(logger_, "Exception occurs when closing %s: %s", path_.c_str(),
                            status.ToString().c_str());
        }
    }
    if (fs_) {
        auto status = fs_->Delete(path_);
        if (!status.ok()) {
            PAIMON_LOG_WARN(logger_, "Exception occurs when closing %s: %s", path_.c_str(),
                            status.ToString().c_str());
        }
    }
}

}  // namespace paimon
