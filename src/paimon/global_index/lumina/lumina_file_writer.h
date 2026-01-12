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

#include "fmt/format.h"
#include "lumina/io/FileWriter.h"
#include "paimon/fs/file_system.h"
#include "paimon/global_index/lumina/lumina_utils.h"
namespace paimon::lumina {
class LuminaFileWriter : public ::lumina::io::FileWriter {
 public:
    explicit LuminaFileWriter(const std::shared_ptr<OutputStream>& out) : out_(out) {}
    ~LuminaFileWriter() override = default;

    ::lumina::core::Result<uint64_t> GetLength() const noexcept override {
        Result<int64_t> pos_result = out_->GetPos();
        if (!pos_result.ok()) {
            return ::lumina::core::Result<uint64_t>::Err(PaimonToLuminaStatus(pos_result.status()));
        }
        return ::lumina::core::Result<uint64_t>::Ok(static_cast<uint64_t>(pos_result.value()));
    }

    ::lumina::core::Status Write(const char* data, uint64_t size) override {
        uint64_t total_write_size = 0;
        while (total_write_size < size) {
            uint64_t current_write_size = std::min(size - total_write_size, max_write_size_);
            Result<int32_t> write_result =
                out_->Write(data + total_write_size, static_cast<uint32_t>(current_write_size));
            if (!write_result.ok()) {
                return PaimonToLuminaStatus(write_result.status());
            }
            if (static_cast<uint64_t>(write_result.value()) != current_write_size) {
                return ::lumina::core::Status(
                    ::lumina::core::ErrorCode::IoError,
                    fmt::format("expect write len {} mismatch actual write len {}",
                                current_write_size, write_result.value()));
            }
            total_write_size += current_write_size;
        }
        return ::lumina::core::Status::Ok();
    }

    ::lumina::core::Status Close() override {
        auto status = out_->Flush();
        if (!status.ok()) {
            return PaimonToLuminaStatus(status);
        }
        return PaimonToLuminaStatus(out_->Close());
    }

 private:
    static constexpr uint64_t kMaxWriteSize = std::numeric_limits<int32_t>::max();

 private:
    uint64_t max_write_size_ = kMaxWriteSize;
    std::shared_ptr<OutputStream> out_;
};
}  // namespace paimon::lumina
