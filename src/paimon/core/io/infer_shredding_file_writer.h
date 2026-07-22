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

#pragma once

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "paimon/common/data/shredding/shredding_write_plan_factory.h"
#include "paimon/core/io/single_file_writer.h"
#include "paimon/core/key_value.h"

namespace paimon {

/// A file writer that infers the shredding write plan from the first rows of the file. Incoming
/// batches are buffered until `ShreddingWritePlanFactory::InferBufferRowCount` rows have been
/// collected (or the writer is closed); the buffered batches are then sampled to create the
/// batch converter, the actual file writer is created with the resulting physical schema, and
/// the buffered batches are replayed into it. The file never rolls while buffering.
template <typename T, typename R>
class InferShreddingFileWriter : public SingleFileWriter<T, R> {
 public:
    using CreateInnerFn = std::function<Result<std::unique_ptr<SingleFileWriter<T, R>>>(
        const std::shared_ptr<ShreddingBatchConverter>&)>;

    InferShreddingFileWriter(const std::shared_ptr<arrow::Schema>& logical_schema,
                             const std::shared_ptr<ShreddingWritePlanFactory>& plan_factory,
                             const std::string& file_format_identifier, CreateInnerFn create_inner)
        : SingleFileWriter<T, R>(/*compression=*/"", std::function<Status(T, ::ArrowArray*)>()),
          logical_type_(arrow::struct_(logical_schema->fields())),
          plan_factory_(plan_factory),
          file_format_identifier_(file_format_identifier),
          create_inner_(std::move(create_inner)) {}

    Status Write(T record) override {
        if (plan_finalized_) {
            return inner_->Write(std::move(record));
        }
        PAIMON_RETURN_NOT_OK(Buffer(std::move(record)));
        if (buffered_rows_ >= plan_factory_->InferBufferRowCount()) {
            return FinalizePlanAndFlush();
        }
        return Status::OK();
    }

    Status Close() override {
        if (!plan_finalized_) {
            PAIMON_RETURN_NOT_OK(FinalizePlanAndFlush());
        }
        return inner_->Close();
    }

    Result<R> GetResult() override {
        if (!inner_) {
            return Status::Invalid("Cannot access the result unless the writer is closed.");
        }
        return inner_->GetResult();
    }

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) override {
        if (!plan_finalized_) {
            // Never roll the file while rows are being buffered for inference.
            return false;
        }
        return inner_->ReachTargetSize(suggested_check, target_size);
    }

    Result<typename SingleFileWriter<T, R>::AbortExecutor> GetAbortExecutor() const override {
        if (!inner_) {
            return Status::Invalid("Writer should be closed!");
        }
        return inner_->GetAbortExecutor();
    }

    std::string GetPath() const override {
        return inner_ ? inner_->GetPath() : "";
    }

    void Abort() override {
        if (inner_) {
            inner_->Abort();
        }
    }

    int64_t RecordCount() const override {
        return plan_finalized_ ? inner_->RecordCount() : buffered_rows_;
    }

    std::shared_ptr<Metrics> GetMetrics() const override {
        return inner_ ? inner_->GetMetrics() : nullptr;
    }

 private:
    struct BufferedBatch {
        std::shared_ptr<arrow::Array> batch;
        // Holds the non-batch part of a KeyValueBatch record; unused for plain batches.
        KeyValueBatch record_template;
    };

    Status Buffer(T record) {
        BufferedBatch buffered;
        ::ArrowArray* c_batch;
        if constexpr (std::is_same_v<T, ::ArrowArray*>) {
            c_batch = record;
        } else {
            static_assert(std::is_same_v<T, KeyValueBatch>,
                          "InferShreddingFileWriter supports ::ArrowArray* and KeyValueBatch");
            buffered.record_template = std::move(record);
            c_batch = buffered.record_template.batch.get();
        }
        PAIMON_ASSIGN_OR_RAISE_FROM_ARROW(buffered.batch,
                                          arrow::ImportArray(c_batch, logical_type_));
        buffered_rows_ += buffered.batch->length();
        buffered_batches_.push_back(std::move(buffered));
        return Status::OK();
    }

    Status FinalizePlanAndFlush() {
        std::vector<std::shared_ptr<arrow::Array>> samples;
        samples.reserve(buffered_batches_.size());
        for (const auto& buffered : buffered_batches_) {
            samples.push_back(buffered.batch);
        }
        PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<ShreddingBatchConverter> converter,
                               plan_factory_->CreateConverter(file_format_identifier_, samples));
        PAIMON_ASSIGN_OR_RAISE(inner_, create_inner_(converter));
        plan_finalized_ = true;
        for (auto& buffered : buffered_batches_) {
            if constexpr (std::is_same_v<T, ::ArrowArray*>) {
                ::ArrowArray c_batch;
                PAIMON_RETURN_NOT_OK_FROM_ARROW(arrow::ExportArray(*buffered.batch, &c_batch));
                PAIMON_RETURN_NOT_OK(inner_->Write(&c_batch));
            } else {
                KeyValueBatch record = std::move(buffered.record_template);
                record.batch = std::make_unique<::ArrowArray>();
                PAIMON_RETURN_NOT_OK_FROM_ARROW(
                    arrow::ExportArray(*buffered.batch, record.batch.get()));
                PAIMON_RETURN_NOT_OK(inner_->Write(std::move(record)));
            }
        }
        buffered_batches_.clear();
        return Status::OK();
    }

    std::shared_ptr<arrow::DataType> logical_type_;
    std::shared_ptr<ShreddingWritePlanFactory> plan_factory_;
    std::string file_format_identifier_;
    CreateInnerFn create_inner_;

    std::vector<BufferedBatch> buffered_batches_;
    int64_t buffered_rows_ = 0;
    bool plan_finalized_ = false;
    std::unique_ptr<SingleFileWriter<T, R>> inner_;
};

}  // namespace paimon
