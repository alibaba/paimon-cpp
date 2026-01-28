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

#include "paimon/format/orc/orc_input_stream_impl.h"

#include <atomic>
#include <utility>

#include "fmt/format.h"
#include "orc/Exceptions.hh"
#include "orc/Reader.hh"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon::orc {
Result<std::unique_ptr<OrcInputStreamImpl>> OrcInputStreamImpl::Create(
    const std::shared_ptr<paimon::InputStream>& input_stream, uint64_t natural_read_size) {
    PAIMON_ASSIGN_OR_RAISE(std::string name, input_stream->GetUri());
    PAIMON_ASSIGN_OR_RAISE(uint64_t length, input_stream->Length());
    return std::unique_ptr<OrcInputStreamImpl>(
        new OrcInputStreamImpl(input_stream, name, length, natural_read_size));
}

OrcInputStreamImpl::OrcInputStreamImpl(const std::shared_ptr<paimon::InputStream>& input_stream,
                                       const std::string& name, uint64_t length,
                                       uint64_t natural_read_size)
    : input_stream_(input_stream),
      uri_name_(name),
      length_(length),
      natural_read_size_(natural_read_size) {}

OrcInputStreamImpl::~OrcInputStreamImpl() {
    if (input_stream_ != nullptr) {
        [[maybe_unused]] auto status = input_stream_->Close();
    }
}

uint64_t OrcInputStreamImpl::getLength() const {
    return length_;
}

uint64_t OrcInputStreamImpl::getNaturalReadSize() const {
    return natural_read_size_;
}

void OrcInputStreamImpl::read(void* buf, uint64_t length, uint64_t offset) {
    if (metrics_) {
        metrics_->IOCount.fetch_add(1);
    }

    Result<int32_t> read_bytes = input_stream_->Read(static_cast<char*>(buf), length, offset);
    if (!read_bytes.ok()) {
        throw ::orc::ParseError("read failed, status: " + read_bytes.status().ToString());
    }
    if (static_cast<uint64_t>(read_bytes.value()) != length) {
        throw ::orc::ParseError(
            fmt::format("read failed, expected length: {}, actual read length: {}", length,
                        read_bytes.value()));
    }
}

std::future<void> OrcInputStreamImpl::readAsync(void* buf, uint64_t length, uint64_t offset) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto callback = [this, promise, length, offset](const Status& status) mutable {
        try {
            if (status.ok()) {
                read_bytes_.fetch_add(length, std::memory_order_relaxed);
                promise->set_value();
            } else {
                promise->set_exception(std::make_exception_ptr(::orc::ParseError(
                    "Async read failed at offset " + std::to_string(offset) + ", length " +
                    std::to_string(length) + ": " + status.ToString())));
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
        --pending_request_;
    };

    ++pending_request_;
    input_stream_->ReadAsync(static_cast<char*>(buf), length, offset, std::move(callback));

    return future;
}

const std::string& OrcInputStreamImpl::getName() const {
    return uri_name_;
}

}  // namespace paimon::orc
