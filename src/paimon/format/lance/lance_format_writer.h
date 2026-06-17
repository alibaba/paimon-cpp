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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "lance_lib/lance_api.h"
#include "paimon/common/metrics/metrics_impl.h"
#include "paimon/format/format_writer.h"
#include "paimon/result.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow
namespace paimon {
class Metrics;
}  // namespace paimon
struct ArrowArray;

namespace paimon::lance {
/// A `FormatWriter` implementation that writes data in Lance format.
class LanceFormatWriter : public FormatWriter {
 public:
    static Result<std::unique_ptr<LanceFormatWriter>> Create(
        const std::string& file_path, const std::shared_ptr<arrow::Schema>& schema);

    ~LanceFormatWriter() override;

    Status AddBatch(ArrowArray* batch) override;

    Status Flush() override {
        return Status::OK();
    }

    Status Finish() override;

    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;

    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        // TODO(xinyu.lxy): add metrics
        return metrics_;
    }

    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    LanceFormatWriter(LanceFileWriter* file_writer,
                      const std::shared_ptr<arrow::Schema>& arrow_schema,
                      std::string&& error_message)
        : error_message_(std::move(error_message)),
          arrow_schema_(arrow_schema),
          file_writer_(file_writer) {
        metrics_ = std::make_shared<MetricsImpl>();
    }

 private:
    mutable std::string error_message_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    LanceFileWriter* file_writer_ = nullptr;
    std::shared_ptr<Metrics> metrics_;
};

}  // namespace paimon::lance
