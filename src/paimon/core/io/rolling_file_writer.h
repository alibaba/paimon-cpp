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

#include <memory>
#include <utility>
#include <vector>

#include "arrow/c/bridge.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/core/io/file_writer.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/key_value.h"
#include "paimon/metrics.h"
#include "paimon/record_batch.h"

namespace paimon {

// Writer to roll over to a new file if the current size exceed the target file size.
template <typename T, typename R>
class RollingFileWriter : public FileWriter<T, std::vector<R>> {
 public:
    RollingFileWriter(
        int64_t target_file_size,
        std::function<Result<std::unique_ptr<SingleFileWriter<T, R>>>()> create_file_writer)
        : target_file_size_(target_file_size),
          create_file_writer(create_file_writer),
          metrics_(std::make_shared<MetricsImpl>()),
          logger_(Logger::GetLogger("RollingFileWriter")) {}

    ~RollingFileWriter() = default;

    Status Write(T record) override;
    void Abort() override;
    Status Close() override;
    Result<std::vector<R>> GetResult() override;

    int64_t RecordCount() const override {
        return record_count_;
    }

    std::shared_ptr<Metrics> GetMetrics() const override {
        return metrics_;
    }

    int64_t TargetFileSize() const {
        return target_file_size_;
    }

 protected:
    static constexpr int32_t CHECK_ROLLING_RECORD_CNT = 1000;

    bool SuggestCheck();
    Result<bool> NeedRollingFile();
    Result<std::unique_ptr<SingleFileWriter<T, R>>> NewWriter();
    Status OpenCurrentWriter();

    int64_t target_file_size_ = 0;
    std::function<Result<std::unique_ptr<SingleFileWriter<T, R>>>()> create_file_writer;
    std::shared_ptr<Metrics> metrics_;

    int64_t record_count_ = 0;
    int64_t last_need_rolling_record_count_ = 0;
    bool closed_ = false;

    std::vector<typename SingleFileWriter<T, R>::AbortExecutor> closed_writers_;
    std::vector<R> results_;
    std::unique_ptr<SingleFileWriter<T, R>> current_writer_;

 private:
    Status CloseCurrentWriter();

    std::unique_ptr<Logger> logger_;
};

template <typename T, typename R>
bool RollingFileWriter<T, R>::SuggestCheck() {
    bool suggest_check = false;
    if (record_count_ - last_need_rolling_record_count_ >= CHECK_ROLLING_RECORD_CNT) {
        suggest_check = true;
        last_need_rolling_record_count_ = record_count_;
    }
    return suggest_check;
}

template <typename T, typename R>
Result<bool> RollingFileWriter<T, R>::NeedRollingFile() {
    return current_writer_->ReachTargetSize(SuggestCheck(), target_file_size_);
}

template <typename T, typename R>
Status RollingFileWriter<T, R>::Write(T record) {
    ScopeGuard guard([this]() -> void { this->Abort(); });
    // Open the current writer if write the first record or roll over happen before.
    if (PAIMON_UNLIKELY(current_writer_ == nullptr)) {
        PAIMON_RETURN_NOT_OK(OpenCurrentWriter());
    }
    int64_t record_count = 0;
    if constexpr (std::is_same_v<T, ::ArrowArray*>) {
        record_count = record->length;
    } else if constexpr (std::is_same_v<T, KeyValueBatch>) {
        record_count = record.batch->length;
    } else {
        record_count = 1;
    }
    PAIMON_RETURN_NOT_OK(current_writer_->Write(std::move(record)));
    record_count_ += record_count;
    PAIMON_ASSIGN_OR_RAISE(bool need_rolling_file, NeedRollingFile());
    if (need_rolling_file) {
        PAIMON_RETURN_NOT_OK(CloseCurrentWriter());
    }
    guard.Release();
    return Status::OK();
}

template <typename T, typename R>
Result<std::vector<R>> RollingFileWriter<T, R>::GetResult() {
    if (!closed_) {
        return Status::Invalid("Cannot access the results unless close all writers.");
    }
    return results_;
}

template <typename T, typename R>
Result<std::unique_ptr<SingleFileWriter<T, R>>> RollingFileWriter<T, R>::NewWriter() {
    return create_file_writer();
}

template <typename T, typename R>
Status RollingFileWriter<T, R>::OpenCurrentWriter() {
    PAIMON_ASSIGN_OR_RAISE(current_writer_, NewWriter());
    if (metrics_) {
        metrics_->Merge(current_writer_->GetMetrics());
    }
    return Status::OK();
}

template <typename T, typename R>
Status RollingFileWriter<T, R>::CloseCurrentWriter() {
    if (current_writer_ == nullptr) {
        return Status::OK();
    }
    std::shared_ptr<Metrics> current_metrics = current_writer_->GetMetrics();
    PAIMON_RETURN_NOT_OK(current_writer_->Close());
    PAIMON_ASSIGN_OR_RAISE(auto abort_executor, current_writer_->GetAbortExecutor());
    closed_writers_.push_back(abort_executor);
    PAIMON_ASSIGN_OR_RAISE(R result, current_writer_->GetResult());
    results_.push_back(result);
    current_writer_.reset();
    if (metrics_) {
        metrics_->Merge(current_metrics);
    }
    return Status::OK();
}

template <typename T, typename R>
Status RollingFileWriter<T, R>::Close() {
    if (closed_) {
        return Status::OK();
    }
    auto s = CloseCurrentWriter();
    if (!s.ok()) {
        if (current_writer_) {
            PAIMON_LOG_WARN(logger_, "Exception occurs when writing file %s. Cleaning up: %s",
                            current_writer_->GetPath().c_str(), s.ToString().c_str());
        }
        Abort();
    }
    closed_ = true;
    return s;
}

template <typename T, typename R>
void RollingFileWriter<T, R>::Abort() {
    if (current_writer_ != nullptr) {
        current_writer_->Abort();
    }
    for (auto& abort_executor : closed_writers_) {
        abort_executor.Abort();
    }
}

}  // namespace paimon
