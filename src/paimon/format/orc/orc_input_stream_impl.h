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

#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <string>

#include "orc/OrcFile.hh"
#include "paimon/fs/file_system.h"
#include "paimon/result.h"

namespace orc {
struct ReaderMetrics;
}  // namespace orc

namespace paimon {
class InputStream;
}  // namespace paimon

namespace paimon::orc {
class OrcInputStreamImpl : public ::orc::InputStream {
 public:
    static Result<std::unique_ptr<OrcInputStreamImpl>> Create(
        const std::shared_ptr<paimon::InputStream>& input_stream, uint64_t natural_read_size);

    ~OrcInputStreamImpl() override;

 public:
    // in orc, metrics IOCount is accumulated in InputStream
    void SetMetrics(::orc::ReaderMetrics* metrics) {
        metrics_ = metrics;
    }
    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void* buf, uint64_t length, uint64_t offset) override;
    std::future<void> readAsync(void* buf, uint64_t length, uint64_t offset) override;
    const std::string& getName() const override;

 private:
    OrcInputStreamImpl(const std::shared_ptr<paimon::InputStream>& input_stream,
                       const std::string& name, uint64_t length, uint64_t natural_read_size);

 private:
    std::atomic<uint64_t> read_bytes_ = {0};
    std::atomic<uint64_t> pending_request_ = {0};
    std::shared_ptr<paimon::InputStream> input_stream_;
    const std::string uri_name_;
    const uint64_t length_;
    const uint64_t natural_read_size_;
    ::orc::ReaderMetrics* metrics_ = nullptr;
};
}  // namespace paimon::orc
