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

#include "paimon/format/format_writer.h"
#include "paimon/result.h"
#include "paimon/status.h"

struct ArrowArray;

namespace paimon {
class MemoryPool;
class Metrics;
class OutputStream;
}  // namespace paimon

namespace paimon::test {
class MockFormatWriter : public FormatWriter {
 public:
    MockFormatWriter(const std::shared_ptr<OutputStream>& out,
                     const std::shared_ptr<MemoryPool>& pool);

    Status AddBatch(ArrowArray* batch) override;
    Status Flush() override;
    Status Finish() override;
    Result<bool> ReachTargetSize(bool suggested_check, int64_t target_size) const override;
    std::shared_ptr<Metrics> GetWriterMetrics() const override {
        return nullptr;
    }
    Status AddMetadata(const std::map<std::string, std::string>& metadata) override;

 private:
    int64_t counter_ = 0;
    std::shared_ptr<OutputStream> out_;
    std::shared_ptr<MemoryPool> pool_;
};

}  // namespace paimon::test
