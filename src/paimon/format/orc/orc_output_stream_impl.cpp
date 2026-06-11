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

#include "paimon/format/orc/orc_output_stream_impl.h"

#include <cassert>
#include <stdexcept>
#include <utility>

#include "fmt/format.h"
#include "paimon/common/utils/math.h"
#include "paimon/fs/file_system.h"
#include "paimon/status.h"

namespace paimon::orc {
Result<std::unique_ptr<OrcOutputStreamImpl>> OrcOutputStreamImpl::Create(
    const std::shared_ptr<paimon::OutputStream>& output_stream) {
    PAIMON_ASSIGN_OR_RAISE(std::string name, output_stream->GetUri());
    return std::unique_ptr<OrcOutputStreamImpl>(new OrcOutputStreamImpl(output_stream, name));
}

OrcOutputStreamImpl::OrcOutputStreamImpl(const std::shared_ptr<paimon::OutputStream>& output_stream,
                                         const std::string& name)
    : output_stream_(output_stream), file_name_(name) {
    assert(output_stream_);
}

uint64_t OrcOutputStreamImpl::getLength() const {
    Result<int64_t> pos = output_stream_->GetPos();
    if (!pos.ok()) {
        throw std::runtime_error(fmt::format("get length failed, file name {}, error msg {}",
                                             file_name_, pos.status().ToString()));
    }
    Status status = ValidateValueInRange<uint64_t>(pos.value(), "file position");
    if (!status.ok()) {
        throw std::runtime_error(fmt::format("get length failed, file name {}, error msg {}",
                                             file_name_, status.ToString()));
    }
    return static_cast<uint64_t>(pos.value());
}

void OrcOutputStreamImpl::write(const void* buf, size_t length) {
    Status status = ValidateValueInRange<int64_t>(length, "write length");
    if (!status.ok()) {
        throw std::runtime_error("write failed, status: " + status.ToString());
    }
    Result<int64_t> write_len =
        output_stream_->Write(static_cast<const char*>(buf), static_cast<int64_t>(length));
    if (!write_len.ok()) {
        throw std::runtime_error("write failed, status: " + write_len.status().ToString());
    }
    if (write_len.value() != static_cast<int64_t>(length)) {
        throw std::runtime_error(
            fmt::format("write failed, expected length: {}, actual write length: {}", length,
                        write_len.value()));
    }
}

void OrcOutputStreamImpl::close() {
    // output stream close is called by paimon single file writer, no need to close here
}

}  // namespace paimon::orc
