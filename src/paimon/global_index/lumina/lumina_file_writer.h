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

#include "fmt/format.h"
#include "lumina/io/FileWriter.h"
#include "paimon/common/utils/math.h"
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

    ::lumina::core::Status Write(const char* data, uint64_t size) noexcept override {
        Status status = ValidateValueInRange<int64_t>(size, "write size");
        if (!status.ok()) {
            return PaimonToLuminaStatus(status);
        }
        Result<int64_t> write_result = out_->Write(data, static_cast<int64_t>(size));
        if (!write_result.ok()) {
            return PaimonToLuminaStatus(write_result.status());
        }
        if (write_result.value() != static_cast<int64_t>(size)) {
            return ::lumina::core::Status(
                ::lumina::core::ErrorCode::IoError,
                fmt::format("expect write len {} mismatch actual write len {}", size,
                            write_result.value()));
        }
        return ::lumina::core::Status::Ok();
    }

    ::lumina::core::Status Close() noexcept override {
        auto status = out_->Flush();
        if (!status.ok()) {
            return PaimonToLuminaStatus(status);
        }
        return PaimonToLuminaStatus(out_->Close());
    }

 private:
    std::shared_ptr<OutputStream> out_;
};
}  // namespace paimon::lumina
