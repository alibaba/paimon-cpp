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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "avro/Stream.hh"
#include "paimon/fs/file_system.h"

namespace paimon {
class MemoryPool;
class OutputStream;
}  // namespace paimon

namespace paimon::avro {

class AvroOutputStreamImpl : public ::avro::OutputStream {
 public:
    AvroOutputStreamImpl(const std::shared_ptr<paimon::OutputStream>& out, size_t buffer_size,
                         const std::shared_ptr<MemoryPool>& pool);
    ~AvroOutputStreamImpl() override;

    // Invariant: byte_count_ == bytes_written + buffer_size_ - available_;
    bool next(uint8_t** data, size_t* len) final;
    void backup(size_t len) final;
    void flush() final;
    uint64_t byteCount() const final {
        return byte_count_;
    }

    void FlushBuffer();

 private:
    std::shared_ptr<MemoryPool> pool_;
    const size_t buffer_size_;
    uint8_t* const buffer_;
    std::shared_ptr<paimon::OutputStream> out_;
    uint8_t* next_;
    size_t available_;
    size_t byte_count_;
};

}  // namespace paimon::avro
