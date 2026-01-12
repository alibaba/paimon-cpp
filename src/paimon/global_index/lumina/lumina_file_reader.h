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

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>

#include "fmt/format.h"
#include "lumina/io/FileReader.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/lumina/lumina_utils.h"
namespace paimon::lumina {
class LuminaFileReader : public ::lumina::io::FileReader {
 public:
    explicit LuminaFileReader(const std::shared_ptr<InputStream>& in) : in_(in) {}
    ~LuminaFileReader() override = default;

    ::lumina::core::Result<uint64_t> GetLength() const noexcept override {
        Result<uint64_t> length_result = in_->Length();
        if (!length_result.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(
                PaimonToLuminaStatus(length_result.status()));
        }
        return ::lumina::core::Result<uint64_t>::Ok(length_result.value());
    }

    ::lumina::core::Result<uint64_t> GetPosition() const noexcept override {
        Result<int64_t> pos_result = in_->GetPos();
        if (!pos_result.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(PaimonToLuminaStatus(pos_result.status()));
        }
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(pos_result.value()));
    }

    ::lumina::core::Status Seek(uint64_t position) noexcept override {
        return PaimonToLuminaStatus(in_->Seek(position, SeekOrigin::FS_SEEK_SET));
    }

    ::lumina::core::Status Read(char* data, uint64_t size) override {
        uint64_t total_read_size = 0;
        while (total_read_size < size) {
            uint64_t current_read_size = std::min(size - total_read_size, max_read_size_);
            Result<int32_t> read_result =
                in_->Read(data + total_read_size, static_cast<uint32_t>(current_read_size));
            if (!read_result.ok()) {
                return PaimonToLuminaStatus(read_result.status());
            }
            if (static_cast<uint64_t>(read_result.value()) != current_read_size) {
                return ::lumina::core::Status(
                    ::lumina::core::ErrorCode::IoError,
                    fmt::format("expect read len {} mismatch actual read len {}", current_read_size,
                                read_result.value()));
            }
            total_read_size += current_read_size;
        }
        return ::lumina::core::Status::Ok();
    }

    void ReadAsync(char* data, uint64_t size, uint64_t offset,
                   std::function<void(::lumina::core::Status)> call_back) noexcept override {
        if (size == 0) {
            call_back(::lumina::core::Status::Ok());
            return;
        }

        struct ReadContext {
            char* current_data;
            uint64_t remaining;
            uint64_t current_offset;
            std::function<void(::lumina::core::Status)> final_call_back;
            std::shared_ptr<InputStream> in;
        };

        auto ctx = std::make_shared<ReadContext>(
            ReadContext{data, size, offset, std::move(call_back), in_});

        // recursive lambda to read next chunk
        std::function<void()> read_next;
        read_next = [ctx, max_read_size = max_read_size_, &read_next]() {
            if (ctx->remaining == 0) {
                // all done
                ctx->final_call_back(::lumina::core::Status::Ok());
                return;
            }

            // determine this chunk's size
            uint64_t chunk_size = std::min(ctx->remaining, max_read_size);
            auto safe_size = static_cast<int32_t>(chunk_size);

            // issue async read for this chunk
            ctx->in->ReadAsync(ctx->current_data, safe_size, ctx->current_offset,
                               [ctx, safe_size, read_next](const Status& status) {
                                   if (!status.ok()) {
                                       // propagate error immediately
                                       ctx->final_call_back(PaimonToLuminaStatus(status));
                                       return;
                                   }
                                   // advance pointers and counters
                                   ctx->current_data += safe_size;
                                   ctx->current_offset += safe_size;
                                   ctx->remaining -= safe_size;

                                   // continue with next chunk
                                   read_next();
                               });
        };

        // start the first read
        read_next();
    }

    ::lumina::core::Status Close() override {
        return PaimonToLuminaStatus(in_->Close());
    }

 private:
    static constexpr uint64_t kMaxReadSize = std::numeric_limits<int32_t>::max();

 private:
    uint64_t max_read_size_ = kMaxReadSize;
    std::shared_ptr<InputStream> in_;
};
}  // namespace paimon::lumina
